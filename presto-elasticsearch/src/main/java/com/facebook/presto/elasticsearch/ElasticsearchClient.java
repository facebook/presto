/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Closer;
import io.airlift.json.ObjectMapperProvider;
import io.airlift.log.Logger;
import io.airlift.units.Duration;
import org.apache.http.HttpHost;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.elasticsearch.client.indices.GetMappingsRequest;
import org.elasticsearch.cluster.metadata.MappingMetaData;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.security.GeneralSecurityException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.elasticsearch.ElasticsearchErrorCode.ELASTICSEARCH_CORRUPTED_MAPPING_METADATA;
import static com.facebook.presto.elasticsearch.RetryDriver.retry;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RowType.Field;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Verify.verify;
import static com.google.common.cache.CacheLoader.asyncReloading;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.Map.Entry;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.TimeUnit.MINUTES;

public class ElasticsearchClient
{
    private static final Logger LOG = Logger.get(ElasticsearchClient.class);

    private final ExecutorService executor = newFixedThreadPool(1, daemonThreadsNamed("elasticsearch-metadata-%s"));
    private final ObjectMapper objecMapper = new ObjectMapperProvider().get();
    private final ElasticsearchTableDescriptionProvider tableDescriptions;
    private final Map<String, RestHighLevelClient> clients = new HashMap<>();
    private final LoadingCache<ElasticsearchTableDescription, List<ColumnMetadata>> columnMetadataCache;
    private final Duration requestTimeout;
    private final int maxAttempts;
    private final Duration maxRetryTime;

    @Inject
    public ElasticsearchClient(ElasticsearchTableDescriptionProvider descriptions, ElasticsearchConnectorConfig config)
            throws IOException, GeneralSecurityException
    {
        tableDescriptions = requireNonNull(descriptions, "description is null");
        ElasticsearchConnectorConfig configuration = requireNonNull(config, "config is null");
        requestTimeout = configuration.getRequestTimeout();
        maxAttempts = configuration.getMaxRequestRetries();
        maxRetryTime = configuration.getMaxRetryTime();

        for (ElasticsearchTableDescription tableDescription : tableDescriptions.getAllTableDescriptions()) {
            if (!clients.containsKey(tableDescription.getClusterName())) {
                RestHighLevelClient client = createClient(config, tableDescription.getHost(), tableDescription.getPort());
                clients.put(tableDescription.getClusterName(), client);
            }
        }
        this.columnMetadataCache = CacheBuilder.newBuilder()
                .expireAfterWrite(30, MINUTES)
                .refreshAfterWrite(15, MINUTES)
                .maximumSize(500)
                .build(asyncReloading(CacheLoader.from(this::loadColumns), executor));
    }

    @PreDestroy
    public void tearDown()
    {
        // Closer closes the resources in reverse order.
        // Therefore, we first clear the clients map, then close each client.
        try (Closer closer = Closer.create()) {
            closer.register(clients::clear);
            for (Entry<String, RestHighLevelClient> entry : clients.entrySet()) {
                closer.register(entry.getValue());
            }
            closer.register(executor::shutdown);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public List<String> listSchemas()
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    public List<SchemaTableName> listTables(Optional<String> schemaName)
    {
        return tableDescriptions.getAllSchemaTableNames()
                .stream()
                .filter(schemaTableName -> !schemaName.isPresent() || schemaTableName.getSchemaName().equals(schemaName.get()))
                .collect(toImmutableList());
    }

    private List<ColumnMetadata> loadColumns(ElasticsearchTableDescription table)
    {
        if (table.getColumns().isPresent()) {
            return buildMetadata(table.getColumns().get());
        }
        return buildMetadata(buildColumns(table));
    }

    public List<ColumnMetadata> getColumnMetadata(ElasticsearchTableDescription tableDescription)
    {
        return columnMetadataCache.getUnchecked(tableDescription);
    }

    public ElasticsearchTableDescription getTable(String schemaName, String tableName)
    {
        requireNonNull(schemaName, "schemaName is null");
        requireNonNull(tableName, "tableName is null");
        ElasticsearchTableDescription table = tableDescriptions.get(new SchemaTableName(schemaName, tableName));
        if (table == null) {
            return null;
        }
        if (table.getColumns().isPresent()) {
            return table;
        }
        return new ElasticsearchTableDescription(
                table.getTableName(),
                table.getSchemaName(),
                table.getHost(),
                table.getPort(),
                table.getClusterName(),
                table.getIndex(),
                table.getIndexExactMatch(),
                Optional.of(buildColumns(table)));
    }

    public List<String> getIndices(ElasticsearchTableDescription tableDescription)
    {
        if (tableDescription.getIndexExactMatch()) {
            return ImmutableList.of(tableDescription.getIndex());
        }
        RestHighLevelClient client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client is null");
        GetIndexRequest request = new GetIndexRequest();
        String[] indices = getIndices(client);
        return Arrays.stream(indices)
                .filter(index -> index.startsWith(tableDescription.getIndex()))
                .collect(toImmutableList());
    }

    public ClusterHealthResponse getHealthResponse(String index, ElasticsearchTableDescription tableDescription)
    {
        RestHighLevelClient client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client is null");
        ClusterHealthRequest request = new ClusterHealthRequest(index);

        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("getSearchShardsResponse", () -> client
                            .cluster()
                            .health(request, RequestOptions.DEFAULT));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private String[] getIndices(RestHighLevelClient client)
    {
        ClusterHealthRequest request = new ClusterHealthRequest();

        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("getIndices", () -> client
                            .cluster()
                            .health(request, RequestOptions.DEFAULT)
                            .getIndices()
                            .keySet()
                            .toArray(new String[0]));
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<ColumnMetadata> buildMetadata(List<ElasticsearchColumn> columns)
    {
        List<ColumnMetadata> result = new ArrayList<>();
        for (ElasticsearchColumn column : columns) {
            Map<String, Object> properties = new HashMap<>();
            properties.put("jsonPath", column.getJsonPath());
            properties.put("jsonType", column.getJsonType());
            properties.put("isList", column.isList());
            properties.put("ordinalPosition", column.getOrdinalPosition());
            result.add(new ColumnMetadata(column.getName(), column.getType(), "", "", false, properties));
        }
        return result;
    }

    private List<ElasticsearchColumn> buildColumns(ElasticsearchTableDescription tableDescription)
    {
        List<ElasticsearchColumn> columns = new ArrayList<>();
        RestHighLevelClient client = clients.get(tableDescription.getClusterName());
        verify(client != null, "client is null");
        for (String index : getIndices(tableDescription)) {
            GetMappingsRequest mappingsRequest = new GetMappingsRequest();

            if (!isNullOrEmpty(index)) {
                mappingsRequest.indices(index);
            }
            Map<String, MappingMetaData> mappings = getMappings(client, mappingsRequest);

            Iterator<String> indexIterator = mappings.keySet().iterator();
            while (indexIterator.hasNext()) {
                // TODO use io.airlift.json.JsonCodec
                MappingMetaData mappingMetaData = mappings.get(indexIterator.next());
                JsonNode rootNode;
                try {
                    rootNode = objecMapper.readTree(mappingMetaData.source().uncompressed());
                }
                catch (IOException e) {
                    throw new PrestoException(ELASTICSEARCH_CORRUPTED_MAPPING_METADATA, e);
                }
                JsonNode mappingNode = rootNode.get("_doc");
                JsonNode propertiesNode = mappingNode.get("properties");

                List<String> lists = new ArrayList<>();
                JsonNode metaNode = mappingNode.get("_meta");
                if (metaNode != null) {
                    JsonNode arrayNode = metaNode.get("lists");
                    if (arrayNode != null && arrayNode.isArray()) {
                        ArrayNode arrays = (ArrayNode) arrayNode;
                        for (int i = 0; i < arrays.size(); i++) {
                            lists.add(arrays.get(i).textValue());
                        }
                    }
                }
                populateColumns(propertiesNode, lists, columns);
            }
        }
        return columns;
    }

    private Map<String, MappingMetaData> getMappings(RestHighLevelClient client, GetMappingsRequest request)
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(maxRetryTime)
                    .run("getMappings", () -> client
                            .indices()
                            .getMapping(request, RequestOptions.DEFAULT)
                            .mappings());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private List<String> getColumnMetadata(Optional<String> parent, JsonNode propertiesNode)
    {
        ImmutableList.Builder<String> metadata = ImmutableList.builder();
        Iterator<Entry<String, JsonNode>> iterator = propertiesNode.fields();
        while (iterator.hasNext()) {
            Entry<String, JsonNode> entry = iterator.next();
            String key = entry.getKey();
            JsonNode value = entry.getValue();
            String childKey;
            if (parent.isPresent()) {
                if (parent.get().isEmpty()) {
                    childKey = key;
                }
                else {
                    childKey = parent.get().concat(".").concat(key);
                }
            }
            else {
                childKey = key;
            }

            if (value.isObject()) {
                metadata.addAll(getColumnMetadata(Optional.of(childKey), value));
                continue;
            }

            if (!value.isArray()) {
                metadata.add(childKey.concat(":").concat(value.textValue()));
            }
        }
        return metadata.build();
    }

    private void populateColumns(JsonNode propertiesNode, List<String> arrays, List<ElasticsearchColumn> columns)
    {
        FieldNestingComparator comparator = new FieldNestingComparator();
        TreeMap<String, Type> fieldsMap = new TreeMap<>(comparator);
        for (String columnMetadata : getColumnMetadata(Optional.empty(), propertiesNode)) {
            int delimiterIndex = columnMetadata.lastIndexOf(":");
            if (delimiterIndex == -1 || delimiterIndex == columnMetadata.length() - 1) {
                LOG.debug("Invalid column path format: %s", columnMetadata);
                continue;
            }
            String fieldName = columnMetadata.substring(0, delimiterIndex);
            String typeName = columnMetadata.substring(delimiterIndex + 1);

            if (!fieldName.endsWith(".type")) {
                LOG.debug("Ignoring column with no type info: %s", columnMetadata);
                continue;
            }
            String propertyName = fieldName.substring(0, fieldName.lastIndexOf('.'));
            String nestedName = propertyName.replaceAll("properties\\.", "");
            if (nestedName.contains(".")) {
                fieldsMap.put(nestedName, getPrestoType(typeName));
            }
            else {
                boolean newColumnFound = columns.stream()
                        .noneMatch(column -> column.getName().equalsIgnoreCase(nestedName));
                if (newColumnFound) {
                    columns.add(new ElasticsearchColumn(nestedName, getPrestoType(typeName), nestedName, typeName, arrays.contains(nestedName), -1));
                }
            }
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private void processNestedFields(TreeMap<String, Type> fieldsMap, List<ElasticsearchColumn> columns, List<String> arrays)
    {
        if (fieldsMap.size() == 0) {
            return;
        }
        Entry<String, Type> first = fieldsMap.firstEntry();
        String field = first.getKey();
        Type type = first.getValue();
        if (field.contains(".")) {
            String prefix = field.substring(0, field.lastIndexOf('.'));
            ImmutableList.Builder<Field> fieldsBuilder = ImmutableList.builder();
            int size = field.split("\\.").length;
            Iterator<String> iterator = fieldsMap.navigableKeySet().iterator();
            while (iterator.hasNext()) {
                String name = iterator.next();
                if (name.split("\\.").length == size && name.startsWith(prefix)) {
                    Optional<String> columnName = Optional.of(name.substring(name.lastIndexOf('.') + 1));
                    Type columnType = fieldsMap.get(name);
                    Field column = new Field(columnName, columnType);
                    fieldsBuilder.add(column);
                    iterator.remove();
                }
            }
            fieldsMap.put(prefix, RowType.from(fieldsBuilder.build()));
        }
        else {
            boolean newColumnFound = columns.stream()
                    .noneMatch(column -> column.getName().equalsIgnoreCase(field));
            if (newColumnFound) {
                columns.add(new ElasticsearchColumn(field, type, field, type.getDisplayName(), arrays.contains(field), -1));
            }
            fieldsMap.remove(field);
        }
        processNestedFields(fieldsMap, columns, arrays);
    }

    private static Type getPrestoType(String elasticsearchType)
    {
        switch (elasticsearchType) {
            case "double":
            case "float":
                return DOUBLE;
            case "integer":
                return INTEGER;
            case "long":
                return BIGINT;
            case "string":
            case "text":
            case "keyword":
                return VARCHAR;
            case "boolean":
                return BOOLEAN;
            case "binary":
                return VARBINARY;
            default:
                throw new IllegalArgumentException("Unsupported type: " + elasticsearchType);
        }
    }

    private static class FieldNestingComparator
            implements Comparator<String>
    {
        FieldNestingComparator() {}

        @Override
        public int compare(String left, String right)
        {
            // comparator based on levels of nesting
            int leftLength = left.split("\\.").length;
            int rightLength = right.split("\\.").length;
            if (leftLength == rightLength) {
                return left.compareTo(right);
            }
            return rightLength - leftLength;
        }
    }

    static RestHighLevelClient createClient(ElasticsearchConnectorConfig config, String host, Integer port)
            throws IOException, GeneralSecurityException
    {
        HttpHost httpHost;
        RestClientBuilder builder;
        RestHighLevelClient client;
        Optional<String> keyPasswordOpt = Optional.of(config.getPemkeyPassword());
        boolean trustSelfSigned = false; // FIXME: should be an option?

        switch (config.getCertificateFormat()) {
            case PEM:
                SSLContext sslContextFromPem = SSLContexts
                        .custom()
                        .loadKeyMaterial(
                                PemReader.loadKeyStore(config.getPemcertFilepath(), config.getPemkeyFilepath(), keyPasswordOpt), keyPasswordOpt.orElse("").toCharArray())
                        .loadTrustMaterial(PemReader.loadTrustStore(config.getPemtrustedcasFilepath()), trustSelfSigned ? new TrustSelfSignedStrategy() : null)
                        .build();
                httpHost = new HttpHost(host, port, "https");
                builder = RestClient.builder(httpHost)
                        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContextFromPem));
                client = new RestHighLevelClient(builder);

                break;
            case JKS:
                SSLContext sslContextFromJks = SSLContexts
                        .custom()
                        .loadKeyMaterial(config.getKeystoreFilepath(), config.getKeystorePassword().toCharArray(), "".toCharArray()) // FIXME: Assume no pass?
                        .loadTrustMaterial(config.getTruststoreFilepath(), config.getTruststorePassword().toCharArray(), trustSelfSigned ? new TrustSelfSignedStrategy() : null)
                        .build();

                httpHost = new HttpHost(host, port, "https");
                builder = RestClient.builder(httpHost)
                        .setHttpClientConfigCallback(httpClientBuilder -> httpClientBuilder.setSSLContext(sslContextFromJks));
                client = new RestHighLevelClient(builder);

                break;
            default:
                httpHost = new HttpHost(host, port, "http");
                client = new RestHighLevelClient(RestClient.builder(httpHost));
                break;
        }
        return client;
    }
}

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
package com.facebook.presto.connector.informationSchema;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.InternalTable;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorInfo;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.PartitionResult;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.QualifiedTablePrefix;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.split.SplitManager;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.transform;
import static java.lang.String.format;

public class InformationSchemaDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final Metadata metadata;
    private final SplitManager splitManager;

    @Inject
    public InformationSchemaDataStreamProvider(Metadata metadata, SplitManager splitManager)
    {
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.splitManager = checkNotNull(splitManager, "splitManager is null");
    }

    @Override
    public Operator createNewDataStream(OperatorContext operatorContext, ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        List<BlockIterable> channels = createChannels(split, columns);
        return new AlignmentOperator(operatorContext, channels);
    }

    private List<BlockIterable> createChannels(ConnectorSplit split, List<ConnectorColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof InformationSchemaSplit, "Split must be of type %s, not %s", InformationSchemaSplit.class.getName(), split.getClass().getName());

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        InformationSchemaTableHandle handle = ((InformationSchemaSplit) split).getTableHandle();
        Map<String, Object> filters = ((InformationSchemaSplit) split).getFilters();

        InternalTable table = getInformationSchemaTable(handle.getSession(), handle.getCatalogName(), handle.getSchemaTableName(), filters);

        ImmutableList.Builder<BlockIterable> list = ImmutableList.builder();
        for (ConnectorColumnHandle column : columns) {
            checkArgument(column instanceof InformationSchemaColumnHandle, "column must be of type %s, not %s", InformationSchemaColumnHandle.class.getName(), column.getClass().getName());
            InformationSchemaColumnHandle internalColumn = (InformationSchemaColumnHandle) column;

            list.add(table.getColumn(internalColumn.getColumnName()));
        }
        return list.build();
    }

    public InternalTable getInformationSchemaTable(ConnectorSession session, String catalog, SchemaTableName table, Map<String, Object> filters)
    {
        if (table.equals(InformationSchemaMetadata.TABLE_COLUMNS)) {
            return buildColumns(session, catalog, filters);
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_TABLES)) {
            return buildTables(session, catalog, filters);
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_SCHEMATA)) {
            return buildSchemata(session, catalog);
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS)) {
            return buildFunctions();
        }
        else if (table.equals(InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS)) {
            return buildPartitions(session, catalog, filters);
        }

        throw new IllegalArgumentException(format("table does not exist: %s", table));
    }

    private InternalTable buildColumns(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_COLUMNS));
        for (Entry<QualifiedTableName, List<ColumnMetadata>> entry : getColumnsList(session, catalogName, filters).entrySet()) {
            QualifiedTableName tableName = entry.getKey();
            for (ColumnMetadata column : entry.getValue()) {
                table.add(
                        tableName.getCatalogName(),
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        column.getName(),
                        column.getOrdinalPosition() + 1,
                        null,
                        "YES",
                        column.getType().getName(),
                        column.isPartitionKey() ? "YES" : "NO");
            }
        }
        return table.build();
    }

    private Map<QualifiedTableName, List<ColumnMetadata>> getColumnsList(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        return metadata.listTableColumns(session, extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildTables(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_TABLES));
        for (QualifiedTableName name : getTablesList(session, catalogName, filters)) {
            table.add(
                    name.getCatalogName(),
                    name.getSchemaName(),
                    name.getTableName(),
                    "BASE TABLE");
        }
        return table.build();
    }

    private List<QualifiedTableName> getTablesList(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        return metadata.listTables(session, extractQualifiedTablePrefix(catalogName, filters));
    }

    private InternalTable buildFunctions()
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS));
        for (FunctionInfo function : metadata.listFunctions()) {
            if (function.isApproximate()) {
                continue;
            }

            Iterable<String> arguments = transform(function.getArgumentTypes(), new Function<Type, String>()
            {
                @Override
                public String apply(Type type)
                {
                    return type.getName();
                }
            });

            String functionType;
            if (function.isAggregate()) {
                functionType = "aggregate";
            }
            else if (function.isWindow()) {
                functionType = "window";
            }
            else if (function.isDeterministic()) {
                functionType = "scalar";
            }
            else {
                functionType = "scalar (non-deterministic)";
            }

            table.add(
                    function.getName().toString(),
                    Joiner.on(", ").join(arguments),
                    function.getReturnType().getName(),
                    functionType,
                    nullToEmpty(function.getDescription()));
        }
        return table.build();
    }

    private InternalTable buildSchemata(ConnectorSession session, String catalogName)
    {
        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_SCHEMATA));
        for (String schema : metadata.listSchemaNames(session, catalogName)) {
            table.add(catalogName, schema);
        }
        return table.build();
    }

    private InternalTable buildPartitions(ConnectorSession session, String catalogName, Map<String, Object> filters)
    {
        QualifiedTableName tableName = extractQualifiedTableName(catalogName, filters);

        InternalTable.Builder table = InternalTable.builder(InformationSchemaMetadata.informationSchemaTableColumns(InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS));
        int partitionNumber = 1;

        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, tableName);
        checkArgument(tableHandle.isPresent(), "Table %s does not exist", tableName);
        Map<ColumnHandle, String> columnHandles = ImmutableBiMap.copyOf(metadata.getColumnHandles(tableHandle.get())).inverse();
        PartitionResult partitionResult = splitManager.getPartitions(tableHandle.get(), Optional.<TupleDomain<ColumnHandle>>absent());

        for (Partition partition : partitionResult.getPartitions()) {
            for (Entry<ColumnHandle, Comparable<?>> entry : partition.getTupleDomain().extractFixedValues().entrySet()) {
                ColumnHandle columnHandle = entry.getKey();
                String columnName = columnHandles.get(columnHandle);
                String value = null;
                if (entry.getValue() != null) {
                    ColumnMetadata columnMetadata  = metadata.getColumnMetadata(tableHandle.get(), columnHandle);
                    try {
                        OperatorInfo operator = metadata.getExactOperator(OperatorInfo.OperatorType.CAST, VarcharType.VARCHAR, ImmutableList.of(columnMetadata.getType()));
                        value = ((Slice) operator.getMethodHandle().invokeWithArguments(entry.getValue())).toStringUtf8();
                    }
                    catch (OperatorNotFoundException e) {
                        value = "<UNREPRESENTABLE VALUE>";
                    }
                    catch (Throwable throwable) {
                        throw Throwables.propagate(throwable);
                    }
                }
                table.add(
                        catalogName,
                        tableName.getSchemaName(),
                        tableName.getTableName(),
                        partitionNumber,
                        columnName,
                        value);
            }
            partitionNumber++;
        }
        return table.build();
    }

    private static QualifiedTableName extractQualifiedTableName(String catalogName, Map<String, Object> filters)
    {
        Optional<String> schemaName = getFilterColumn(filters, "table_schema");
        checkArgument(schemaName.isPresent(), "filter is required for column: %s.%s", InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS, "table_schema");
        Optional<String> tableName = getFilterColumn(filters, "table_name");
        checkArgument(tableName.isPresent(), "filter is required for column: %s.%s", InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS, "table_name");
        return new QualifiedTableName(catalogName, schemaName.get(), tableName.get());
    }

    private static QualifiedTablePrefix extractQualifiedTablePrefix(String catalogName, Map<String, Object> filters)
    {
        Optional<String> schemaName = getFilterColumn(filters, "table_schema");
        Optional<String> tableName = getFilterColumn(filters, "table_name");
        if (!schemaName.isPresent()) {
            return new QualifiedTablePrefix(catalogName, Optional.<String>absent(), Optional.<String>absent());
        }
        return new QualifiedTablePrefix(catalogName, schemaName, tableName);
    }

    private static Optional<String> getFilterColumn(Map<String, Object> filters, String columnName)
    {
        for (Map.Entry<String, Object> entry : filters.entrySet()) {
            if (entry.getKey().equals(columnName)) {
                if (entry.getValue() instanceof String) {
                    return Optional.of((String) entry.getValue());
                }
                if (entry.getValue() instanceof Slice) {
                    return Optional.of(((Slice) entry.getValue()).toStringUtf8());
                }
                break;
            }
        }
        return Optional.absent();
    }
}

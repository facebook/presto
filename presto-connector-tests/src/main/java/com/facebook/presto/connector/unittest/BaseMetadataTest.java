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
package com.facebook.presto.connector.unittest;

import com.facebook.presto.connector.meta.RequiredFeatures;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorOutputTableHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorMetadata;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.facebook.presto.spi.transaction.IsolationLevel;
import com.facebook.presto.testing.TestingConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

import static com.facebook.presto.connector.meta.ConnectorFeature.CREATE_TABLE;
import static com.facebook.presto.connector.meta.ConnectorFeature.DROP_TABLE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static org.junit.jupiter.api.Assertions.assertEquals;

public interface BaseMetadataTest
        extends SPITest
{
    Map<String, Object> getTableProperties();

    default List<String> systemSchemas()
    {
        return ImmutableList.of();
    }

    default List<SchemaTableName> systemTables()
    {
        return ImmutableList.of();
    }

    /*
     * Connectors that add columns containing connector metadata to a table, like Hive,
     * should return a List containing the metadata for all of the expectedColumns and the connector-specific columns added in the appropriate positions.
     */
    default List<ColumnMetadata> extendWithConnectorSpecificColumns(List<ColumnMetadata> expectedColumns)
    {
        return expectedColumns;
    }

    /*
     * Returns a SchemaTableName to be used when the name of the schema is NOT
     * important.
     *
     * Tests that require tables in one schema should use this method. An
     * example would be a test that renames a table within a single schema.
     *
     * Tests written using this can be run against connectors that support the
     * creation of schemas and those that do not, but come with a usable schema
     * out of the box. PostgreSQL, for example has a "public" schema.
     *
     * A class implementing this interface for e.g. PostgreSQL should return a
     * SchemaTableName with the schemaName set to "public".
     *
     * Tests invoking this method do NOT generally need to be annotated with
     * @RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA}).
     *
     * A class overriding this method should also override withSchemas, as
     * described below.
     */
    default SchemaTableName schemaTableName(String tableName)
    {
        return new SchemaTableName("default_schema", tableName);
    }

    /*
     * Returns a SchemaTableName to be used when the name of the schema is
     * important.
     *
     * Tests that require tables in multiple schemas should use this method. An
     * example would be a test that renames a table from one schema to another.
     *
     * Tests invoking this method must be annotated with
     * @RequiredFeatures({CREATE_SCHEMA, DROP_SCHEMA})
     */
    default SchemaTableName schemaTableName(String schemaName, String tableName)
    {
        return new SchemaTableName(schemaName, tableName);
    }

    /*
     * Create and drop the schemas specified by schemaNames, running callable
     * in between creating and dropping the schemas.
     *
     * A class implementing this interface for a connector that does NOT
     * support creating and/or dropping schemas, but DOES have a usable schema
     * (e.g. PostgreSQL with the "public" schema) should override this method
     * and invoke callable without trying to create or destroy any of the
     * schemas specified.
     *
     * Overriding this method in such a way allows tests that explicitly
     * require creating and dropping tables to be written such that they are
     * independent of whether or not the connector supports creating and
     * dropping schemas.
     */
    default void withSchemas(ConnectorSession session, List<String> schemaNames, Callable<Void> callable)
            throws Exception
    {
        try (Closer cleanup = closerOf(schemaNames.stream()
                .distinct()
                .map(schemaName -> new Schema(this, session, schemaName))
                .collect(toImmutableList()))) {
            callable.call();
        }
    }

    @Test
    default void testEmptyMetadata()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());

        withMetadata(
                ImmutableList.of(
                        metadata -> assertEquals(metadata.listSchemaNames(session), systemSchemas()),
                        metadata -> assertEquals(metadata.listTables(session, null), systemTables())));
    }

    /*
     * Arguably, this belongs in MetadataTableTest. Unfortunately many
     * connectors don't support CREATE_TABLE, but do support CREATE_TABLE_AS.
     * As a result, MetadataTableTest is written in terms of CREATE_TABLE_AS,
     * and is annotated with @RequiredFeatures({..., CREATE_TABLE_AS, ...) at
     * the class level.
     *
     * Moving this to MetadataTableTest would require either:
     * 1. Annotating every other methos than this as requiring CREATE_TABLE_AS.
     * 2. Adding a way to unrequire previously required dependencies.
     *
     * 1 is cumbersome, 2 adds confusing semantics and surface area for weird
     * bugs. Putting the test here seems like a good-enough solution.
     */
    @Test
    @RequiredFeatures({CREATE_TABLE, DROP_TABLE})
    default void testCreateDropTable()
            throws Exception
    {
        ConnectorSession session = new TestingConnectorSession(ImmutableList.of());
        String tableName = "table";
        SchemaTableName schemaTableName = schemaTableName(tableName);

        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                schemaTableName,
                ImmutableList.of(
                        new ColumnMetadata("bigint_column", BIGINT),
                        new ColumnMetadata("double_column", DOUBLE)),
                getTableProperties());

        withSchemas(session, ImmutableList.of(schemaTableName.getSchemaName()),
                ImmutableList.of(
                        metadata -> metadata.createTable(session, tableMetadata),
                        metadata -> assertEquals(getOnlyElement(metadata.listTables(session, schemaTableName.getSchemaName())), schemaTableName),
                        metadata -> metadata.dropTable(session, metadata.getTableHandle(session, schemaTableName))));
    }

    default List<String> schemaNamesOf(SchemaTableName... schemaTableNames)
    {
        return stream(schemaTableNames)
                .map(SchemaTableName::getSchemaName)
                .collect(toImmutableList());
    }

    default List<String> schemaNamesOf(List<ConnectorTableMetadata> tables)
    {
        return tables.stream()
                .map(ConnectorTableMetadata::getTable)
                .map(SchemaTableName::getSchemaName)
                .distinct()
                .collect(toImmutableList());
    }

    default SchemaTablePrefix prefixOfSchemaName(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName());
    }

    default SchemaTablePrefix prefixOf(SchemaTableName schemaTableName)
    {
        return new SchemaTablePrefix(schemaTableName.getSchemaName(), schemaTableName.getTableName());
    }

    default void withMetadata(List<Consumer<ConnectorMetadata>> consumers)
    {
        consumers.forEach(this::withMetadata);
    }

    default void withMetadata(Consumer<ConnectorMetadata> consumer)
    {
        Connector connector = getConnector();
        ConnectorTransactionHandle transaction = connector.beginTransaction(IsolationLevel.READ_UNCOMMITTED, true);
        ConnectorMetadata metadata = connector.getMetadata(transaction);
        consumer.accept(metadata);
        connector.commit(transaction);
    }

    class Schema
            implements Closeable
    {
        private final BaseMetadataTest test;
        private final ConnectorSession session;
        private final String name;

        public Schema(BaseMetadataTest test, ConnectorSession session, String name)
        {
            this.test = requireNonNull(test, "test is null");
            this.session = requireNonNull(session, "session is null");
            this.name = requireNonNull(name, "name is null");

            test.withMetadata(metadata -> metadata.createSchema(session, name, ImmutableMap.of()));
        }

        @Override
        public void close()
        {
            test.withMetadata(metadata -> metadata.dropSchema(session, name));
        }
    }

    /*
     * Don't override this! Override the version that takes a Callable; withTables is written in terms of that version, and will not work properly if you override this version.
     */
    default void withSchemas(ConnectorSession session, List<String> schemaNames, List<Consumer<ConnectorMetadata>> consumers)
            throws Exception
    {
        withSchemas(session, schemaNames,
                () -> {
                    consumers.forEach(this::withMetadata);
                    return null;
                });
    }

    class Table
            implements Closeable
    {
        private final BaseMetadataTest test;
        private final ConnectorSession session;
        private final ConnectorTableMetadata tableMetadata;

        public Table(BaseMetadataTest test, ConnectorSession session, ConnectorTableMetadata tableMetadata)
        {
            this.test = requireNonNull(test, "test is null");
            this.session = requireNonNull(session, "session is null");
            this.tableMetadata = requireNonNull(tableMetadata, "tableMetadata is null");

            test.withMetadata(metadata -> {
                ConnectorOutputTableHandle handle = metadata.beginCreateTable(session, tableMetadata, Optional.empty());
                metadata.finishCreateTable(session, handle, ImmutableList.of());
            });
        }

        @Override
        public void close()
        {
            test.withMetadata(metadata -> {
                ConnectorTableHandle handle = metadata.getTableHandle(session, tableMetadata.getTable());
                metadata.dropTable(session, handle);
            });
        }
    }

    default void withTables(ConnectorSession session, List<ConnectorTableMetadata> tables, List<Consumer<ConnectorMetadata>> consumers)
            throws Exception
    {
        withSchemas(session, schemaNamesOf(tables), () -> {
            try (Closer cleanup = closerOf(tables.stream()
                    .map(table -> new Table(this, session, table))
                    .collect(toImmutableList()))) {
                consumers.forEach(this::withMetadata);
            }
            return null;
        });
    }

    default Closer closerOf(List<Closeable> closables)
    {
        Closer result = Closer.create();
        closables.forEach(result::register);
        return result;
    }
}

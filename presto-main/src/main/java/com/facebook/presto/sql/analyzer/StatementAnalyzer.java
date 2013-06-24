package com.facebook.presto.sql.analyzer;

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.MetadataUtil;
import com.facebook.presto.metadata.NativeTableHandle;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.sql.tree.CreateMaterializedView;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.RefreshMaterializedView;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_COLUMNS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_FUNCTIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_INTERNAL_PARTITIONS;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_SCHEMATA;
import static com.facebook.presto.connector.informationSchema.InformationSchemaMetadata.TABLE_TABLES;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.DUPLICATE_RELATION;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_MATERIALIZED_VIEW_REFRESH_INTERVAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_ORDINAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_SCHEMA_NAME;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TABLE_ALREADY_EXISTS;
import static com.facebook.presto.sql.tree.QueryUtil.aliasedName;
import static com.facebook.presto.sql.tree.QueryUtil.ascending;
import static com.facebook.presto.sql.tree.QueryUtil.caseWhen;
import static com.facebook.presto.sql.tree.QueryUtil.equal;
import static com.facebook.presto.sql.tree.QueryUtil.functionCall;
import static com.facebook.presto.sql.tree.QueryUtil.logicalAnd;
import static com.facebook.presto.sql.tree.QueryUtil.nameReference;
import static com.facebook.presto.sql.tree.QueryUtil.selectAll;
import static com.facebook.presto.sql.tree.QueryUtil.selectList;
import static com.facebook.presto.sql.tree.QueryUtil.table;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;


class StatementAnalyzer
        extends DefaultTraversalVisitor<TupleDescriptor, AnalysisContext>
{
    private final Analysis analysis;
    private final Metadata metadata;
    private final Session session;
    private final Optional<QueryExplainer> queryExplainer;

    public StatementAnalyzer(Analysis analysis, Metadata metadata, Session session, Optional<QueryExplainer> queryExplainer)
    {
        this.analysis = checkNotNull(analysis, "analysis is null");
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.session = checkNotNull(session, "session is null");
        this.queryExplainer = checkNotNull(queryExplainer, "queryExplainer is null");
    }

    @Override
    protected TupleDescriptor visitShowTables(ShowTables showTables, AnalysisContext context)
    {
        String catalogName = session.getCatalog();
        String schemaName = session.getSchema();

        QualifiedName schema = showTables.getSchema();
        if (schema != null) {
            List<String> parts = schema.getParts();
            if (parts.size() > 2) {
                throw new SemanticException(INVALID_SCHEMA_NAME, showTables, "too many parts in schema name: %s", schema);
            }
            if (parts.size() == 2) {
                catalogName = parts.get(0);
            }
            schemaName = schema.getSuffix();
        }

        // TODO: throw SemanticException if schema does not exist

        Expression predicate = equal(nameReference("table_schema"), new StringLiteral(schemaName));

        String likePattern = showTables.getLikePattern();
        if (likePattern != null) {
            Expression likePredicate = new LikePredicate(nameReference("table_name"), new StringLiteral(likePattern), null);
            predicate = logicalAnd(predicate, likePredicate);
        }

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(aliasedName("table_name", "Table")),
                        table(QualifiedName.of(catalogName, TABLE_TABLES.getSchemaName(), TABLE_TABLES.getTableName())),
                        Optional.of(predicate),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("table_name")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowSchemas(ShowSchemas node, AnalysisContext context)
    {
        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(aliasedName("schema_name", "Schema")),
                        table(QualifiedName.of(session.getCatalog(), TABLE_SCHEMATA.getSchemaName(), TABLE_SCHEMATA.getTableName())),
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("schema_name")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowColumns(ShowColumns showColumns, AnalysisContext context)
    {
        QualifiedTableName tableName = MetadataUtil.createQualifiedTableName(session, showColumns.getTable());

        if (!metadata.getTableHandle(tableName).isPresent()) {
            throw new SemanticException(MISSING_TABLE, showColumns, "Table '%s' does not exist", tableName);
        }

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(
                                aliasedName("column_name", "Column"),
                                aliasedName("data_type", "Type"),
                                aliasedName("is_nullable", "Null"),
                                aliasedName("is_partition_key", "Partition Key")),
                        table(QualifiedName.of(tableName.getCatalogName(), TABLE_COLUMNS.getSchemaName(), TABLE_COLUMNS.getTableName())),
                        Optional.of(logicalAnd(
                                equal(nameReference("table_schema"), new StringLiteral(tableName.getSchemaName())),
                                equal(nameReference("table_name"), new StringLiteral(tableName.getTableName())))),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("ordinal_position")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowPartitions(ShowPartitions showPartitions, AnalysisContext context)
    {
        QualifiedTableName table = MetadataUtil.createQualifiedTableName(session, showPartitions.getTable());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(table);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, showPartitions, "Table '%s' does not exist", table);
        }

            /*
                Generate a dynamic pivot to output one column per partition key.
                For example, a table with two partition keys (ds, cluster_name)
                would generate the following query:

                SELECT
                  max(CASE WHEN partition_key = 'ds' THEN partition_value END) ds
                , max(CASE WHEN partition_key = 'cluster_name' THEN partition_value END) cluster_name
                FROM ...
                GROUP BY partition_number
                ORDER BY partition_number
            */

        ImmutableList.Builder<SelectItem> selectList = ImmutableList.builder();
        for (ColumnMetadata column : metadata.getTableMetadata(tableHandle.get()).getColumns()) {
            if (!column.isPartitionKey()) {
                continue;
            }
            Expression key = equal(nameReference("partition_key"), new StringLiteral(column.getName()));
            Expression function = functionCall("max", caseWhen(key, nameReference("partition_value")));
            selectList.add(new SingleColumn(function, column.getName()));
        }

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectAll(selectList.build()),
                        table(QualifiedName.of(table.getCatalogName(), TABLE_INTERNAL_PARTITIONS.getSchemaName(), TABLE_INTERNAL_PARTITIONS.getTableName())),
                        Optional.of(logicalAnd(
                                equal(nameReference("table_schema"), new StringLiteral(table.getSchemaName())),
                                equal(nameReference("table_name"), new StringLiteral(table.getTableName())))),
                        ImmutableList.of(nameReference("partition_number")),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("partition_number")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitShowFunctions(ShowFunctions node, AnalysisContext context)
    {
        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(
                                aliasedName("function_name", "Function"),
                                aliasedName("return_type", "Return Type"),
                                aliasedName("argument_types", "Argument Types"),
                                aliasedName("function_type", "Function Type")),
                        table(QualifiedName.of(TABLE_INTERNAL_FUNCTIONS.getSchemaName(), TABLE_INTERNAL_FUNCTIONS.getTableName())),
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.of(ascending("function_name")),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }


    @Override
    protected TupleDescriptor visitCreateMaterializedView(CreateMaterializedView node, AnalysisContext context)
    {
        // Turn this into a query that has a new table writer node on top.
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        analysis.setDestination(targetTable);

        Optional<TableHandle> targetTableHandle = metadata.getTableHandle(targetTable);
        if (targetTableHandle.isPresent()) {
            throw new SemanticException(TABLE_ALREADY_EXISTS, node, "Destination table '%s' already exists", targetTable);
        }

        if (node.getRefresh().isPresent()) {
            int refreshInterval = Integer.parseInt(node.getRefresh().get());
            if (refreshInterval <= 0) {
                throw new SemanticException(INVALID_MATERIALIZED_VIEW_REFRESH_INTERVAL, node, "Refresh interval must be > 0 (was %s)", refreshInterval);
            }

            analysis.setRefreshInterval(Optional.of(refreshInterval));
        }
        else {
            analysis.setRefreshInterval(Optional.<Integer>absent());
        }

        // Analyze the query that creates the table...
        process(node.getTableDefinition(), context);

        return new TupleDescriptor(Field.newUnqualified("imported_rows", Type.LONG));
    }

    @Override
    protected TupleDescriptor visitRefreshMaterializedView(RefreshMaterializedView node, AnalysisContext context)
    {
        QualifiedTableName targetTable = MetadataUtil.createQualifiedTableName(session, node.getName());
        Optional<TableHandle> tableHandle = metadata.getTableHandle(targetTable);
        if (!tableHandle.isPresent()) {
            throw new SemanticException(MISSING_TABLE, node, "Destination table '%s' does not exist", targetTable);
        }

        checkState(tableHandle.get() instanceof NativeTableHandle, "Cannot import into non-native table %s", targetTable);
        analysis.setDestination(targetTable);
        analysis.setDoRefresh(true);

        return new TupleDescriptor(Field.newUnqualified("imported_rows", Type.LONG));
    }

    @Override
    protected TupleDescriptor visitExplain(Explain node, AnalysisContext context)
    {
        checkState(queryExplainer.isPresent(), "query explainer not available");
        String queryPlan = queryExplainer.get().getPlan(node.getQuery());

        Query query = new Query(
                Optional.<With>absent(),
                new QuerySpecification(
                        selectList(
                                new SingleColumn(new StringLiteral(queryPlan), "Query Plan")),
                        null,
                        Optional.<Expression>absent(),
                        ImmutableList.<Expression>of(),
                        Optional.<Expression>absent(),
                        ImmutableList.<SortItem>of(),
                        Optional.<String>absent()
                ),
                ImmutableList.<SortItem>of(),
                Optional.<String>absent());

        return process(query, context);
    }

    @Override
    protected TupleDescriptor visitQuery(Query node, AnalysisContext parentContext)
    {
        AnalysisContext context = new AnalysisContext(parentContext);

        analyzeWith(node, context);

        TupleAnalyzer analyzer = new TupleAnalyzer(analysis, session, metadata);
        TupleDescriptor descriptor = analyzer.process(node.getQueryBody(), context);
        analyzeOrderBy(node, descriptor);

        // Input fields == Output fields
        analysis.setOutputDescriptor(node, descriptor);
        analysis.setOutputExpressions(node, descriptorToFields(descriptor));
        analysis.setQuery(node);

        return descriptor;
    }

    private List<FieldOrExpression> descriptorToFields(TupleDescriptor tupleDescriptor)
    {
        ImmutableList.Builder<FieldOrExpression> builder = ImmutableList.builder();
        for (int fieldIndex = 0; fieldIndex < tupleDescriptor.getFields().size(); fieldIndex++) {
            builder.add(new FieldOrExpression(fieldIndex));
        }
        return builder.build();
    }

    private void analyzeWith(Query node, AnalysisContext context)
    {
        // analyze WITH clause
        if (!node.getWith().isPresent()) {
            return;
        }

        With with = node.getWith().get();
        if (with.isRecursive()) {
            throw new SemanticException(NOT_SUPPORTED, with, "Recursive WITH queries are not supported");
        }

        for (WithQuery withQuery : with.getQueries()) {
            if (withQuery.getColumnNames() != null && !withQuery.getColumnNames().isEmpty()) {
                throw new SemanticException(NOT_SUPPORTED, withQuery, "Column alias not supported in WITH queries");
            }

            Query query = withQuery.getQuery();
            process(query, context);

            String name = withQuery.getName();
            if (context.isNamedQueryDeclared(name)) {
                throw new SemanticException(DUPLICATE_RELATION, withQuery, "WITH query name '%s' specified more than once", name);
            }

            context.addNamedQuery(name, query);
        }
    }

    private void analyzeOrderBy(Query node, TupleDescriptor tupleDescriptor)
    {
        List<SortItem> items = node.getOrderBy();

        ImmutableList.Builder<FieldOrExpression> orderByFieldsBuilder = ImmutableList.builder();

        if (!items.isEmpty()) {
            for (SortItem item : items) {
                Expression expression = item.getSortKey();

                FieldOrExpression orderByField;
                if (expression instanceof LongLiteral) {
                    // this is an ordinal in the output tuple

                    long ordinal = ((LongLiteral) expression).getValue();
                    if (ordinal < 1 || ordinal > tupleDescriptor.getFields().size()) {
                        throw new SemanticException(INVALID_ORDINAL, expression, "ORDER BY position %s is not in select list", ordinal);
                    }

                    orderByField = new FieldOrExpression((int) (ordinal - 1));
                }
                else {
                    // otherwise, just use the expression as is
                    orderByField = new FieldOrExpression(expression);
                    Analyzer.analyzeExpression(metadata, tupleDescriptor, analysis, orderByField.getExpression());
                }

                orderByFieldsBuilder.add(orderByField);
            }
        }

        analysis.setOrderByExpressions(node, orderByFieldsBuilder.build());
    }
}

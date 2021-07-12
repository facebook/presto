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
package com.facebook.presto.sql.analyzer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.LogicalRowExpressions;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AllowAllAccessControl;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupBy;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.OrderBy;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QueryBody;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.expressions.LogicalRowExpressions.and;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedObjectName;
import static com.facebook.presto.spi.relation.DomainTranslator.BASIC_COLUMN_EXTRACTOR;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MISSING_TABLE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static com.facebook.presto.spi.relation.DomainTranslator.ExtractionResult;
import static com.facebook.presto.sql.relational.Expressions.call;
import static java.util.Objects.requireNonNull;

public class MaterializedViewQueryOptimizer
        extends AstVisitor<Node, Void>
{
    private final Metadata metadata;
    private final Session session;
    private final SqlParser sqlParser;
    private final RowExpressionDomainTranslator domainTranslator;
    private final Table materializedView;
    private final Query materializedViewQuery;

    private MaterializedViewInfo materializedViewInfo;
    private final LogicalRowExpressions logicalRowExpressions;

    private static final Logger logger = Logger.get(MaterializedViewQueryOptimizer.class);

    public MaterializedViewQueryOptimizer(Metadata metadata, Session session, SqlParser sqlParser, RowExpressionDomainTranslator domainTranslator, Table materializedView, Query materializedViewQuery)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.session = requireNonNull(session, "session is null");
        this.sqlParser = requireNonNull(sqlParser, "sql parser is null");
        this.domainTranslator = requireNonNull(domainTranslator, "row expression domain translator is null");
        this.materializedView = requireNonNull(materializedView, "materialized view is null");
        this.materializedViewQuery = requireNonNull(materializedViewQuery, "materialized view query is null");
        this.logicalRowExpressions = new LogicalRowExpressions(new RowExpressionDeterminismEvaluator(metadata.getFunctionAndTypeManager()), new FunctionResolution(metadata.getFunctionAndTypeManager()), metadata.getFunctionAndTypeManager());
    }

    public Node rewrite(Node node)
    {
        try {
            MaterializedViewInformationExtractor materializedViewInformationExtractor = new MaterializedViewInformationExtractor();
            materializedViewInformationExtractor.process(materializedViewQuery);
            this.materializedViewInfo = materializedViewInformationExtractor.getMaterializedViewInfo();
            return process(node);
        }
        catch (Exception ex) {
            logger.error(ex.getMessage());
            return node;
        }
    }

    @Override
    protected Node visitNode(Node node, Void context)
    {
        return node;
    }

    @Override
    protected Node visitQuery(Query node, Void context)
    {
        QueryBody rewrittenQueryBody = (QueryBody) process(node.getQueryBody(), context);
        return new Query(
                node.getWith(),
                rewrittenQueryBody,
                node.getOrderBy(),
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitQuerySpecification(QuerySpecification node, Void context)
    {
        if (!node.getFrom().isPresent()) {
            throw new IllegalStateException("Query with no From clause is not rewritable by materialized view");
        }
        // TODO: Handle filter containment problem https://github.com/prestodb/presto/issues/16405
        if (materializedViewInfo.getWhereClause().isPresent() && !node.getWhere().isPresent()) {
            throw new IllegalStateException("Query with no where clause is not rewritable by materialized view with where clause");
        }
        if (materializedViewInfo.getGroupBy().isPresent() && !node.getGroupBy().isPresent()) {
            throw new IllegalStateException("Query with no groupBy clause is not rewritable by materialized view with groupBy clause");
        }
        // TODO: Add HAVING validation to the validator https://github.com/prestodb/presto/issues/16406
        if (node.getHaving().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Having clause is not supported in query optimizer");
        }

        Optional<Relation> rewrittenFrom = node.getFrom().map(from -> (Relation) process(from, context));
        Select rewrittenSelect = (Select) process(node.getSelect(), context);

        // Filter containment problem
        if (materializedViewInfo.getWhereClause().isPresent() && node.getWhere().isPresent()) {
            if (!(node.getFrom().get() instanceof Table)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Relation other than Table is not supported in query optimizer");
            }
            // Creating scope for conversion
            Table baseTable = (Table) node.getFrom().get();
            QualifiedObjectName baseTableName = createQualifiedObjectName(session, baseTable, baseTable.getName());

            Optional<TableHandle> tableHandle = metadata.getTableHandle(session, baseTableName);

            // Table not presented
            if (!tableHandle.isPresent()) {
                throw new SemanticException(MISSING_TABLE, node, "Table does not exist");
            }

            List<Field> fields = new ArrayList<>();

            for (ColumnHandle columnHandle : metadata.getColumnHandles(session, tableHandle.get()).values()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableHandle.get(), columnHandle);
                fields.add(Field.newUnqualified(columnMetadata.getName(), columnMetadata.getType()));
            }

            Scope scope = Scope.builder()
                    .withRelationType(RelationId.anonymous(), new RelationType(fields))
                    .build();
            RowExpression materializedViewWhereCondition = convertToRowExpression(materializedViewInfo.getWhereClause().get(), scope);
            RowExpression baseQueryWhereCondition = convertToRowExpression(node.getWhere().get(), scope);
            RowExpression rewriteLogicExpression = and(baseQueryWhereCondition, not(materializedViewWhereCondition));
            RowExpression disjunctiveNormalForm = logicalRowExpressions.convertToDisjunctiveNormalForm(rewriteLogicExpression);
            ExtractionResult result = fromPredicate(disjunctiveNormalForm);

            if (!result.getTupleDomain().equals(TupleDomain.none())) {
                throw new IllegalStateException("View filter condition does not contains base query's filter condition");
            }
        }

        Optional<Expression> rewrittenWhere = node.getWhere().map(where -> (Expression) process(where, context));
        Optional<GroupBy> rewrittenGroupBy = node.getGroupBy().map(groupBy -> (GroupBy) process(groupBy, context));
        Optional<Expression> rewrittenHaving = node.getHaving().map(having -> (Expression) process(having, context));
        Optional<OrderBy> rewrittenOrderBy = node.getOrderBy().map(orderBy -> (OrderBy) process(orderBy, context));

        return new QuerySpecification(
                rewrittenSelect,
                rewrittenFrom,
                rewrittenWhere,
                rewrittenGroupBy,
                rewrittenHaving,
                rewrittenOrderBy,
                node.getOffset(),
                node.getLimit());
    }

    @Override
    protected Node visitSelect(Select node, Void context)
    {
        if (materializedViewInfo.isDistinct() && !node.isDistinct()) {
            throw new IllegalStateException("Materialized view has distinct and base query does not");
        }
        ImmutableList.Builder<SelectItem> rewrittenSelectItems = ImmutableList.builder();

        for (SelectItem selectItem : node.getSelectItems()) {
            SelectItem rewrittenSelectItem = (SelectItem) process(selectItem, context);
            rewrittenSelectItems.add(rewrittenSelectItem);
        }

        return new Select(node.isDistinct(), rewrittenSelectItems.build());
    }

    @Override
    protected Node visitSingleColumn(SingleColumn node, Void context)
    {
        return new SingleColumn((Expression) process(node.getExpression(), context), node.getAlias());
    }

    @Override
    protected Node visitAllColumns(AllColumns node, Void context)
    {
        throw new SemanticException(NOT_SUPPORTED, node, "All columns rewrite is not supported in query optimizer");
    }

    @Override
    protected Node visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
    {
        return new ArithmeticBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitIdentifier(Identifier node, Void context)
    {
        if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            throw new IllegalStateException("Materialized view definition does not contain mapping for the column: " + node.getValue());
        }
        return new Identifier(materializedViewInfo.getBaseToViewColumnMap().get(node).getValue(), node.isDelimited());
    }

    @Override
    protected Node visitFunctionCall(FunctionCall node, Void context)
    {
        ImmutableList.Builder<Expression> rewrittenArguments = ImmutableList.builder();

        if (materializedViewInfo.getBaseToViewColumnMap().containsKey(node)) {
            rewrittenArguments.add(materializedViewInfo.getBaseToViewColumnMap().get(node));
        }
        else {
            for (Expression argument : node.getArguments()) {
                rewrittenArguments.add((Expression) process(argument, context));
            }
        }

        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewrittenArguments.build());
    }

    @Override
    protected Node visitRelation(Relation node, Void context)
    {
        if (materializedViewInfo.getBaseTable().isPresent() && node.equals(materializedViewInfo.getBaseTable().get())) {
            return materializedView;
        }
        throw new IllegalStateException("Mismatching table or non-supporting relation format in base query");
    }

    @Override
    protected Node visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
    {
        return new LogicalBinaryExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitComparisonExpression(ComparisonExpression node, Void context)
    {
        return new ComparisonExpression(
                node.getOperator(),
                (Expression) process(node.getLeft(), context),
                (Expression) process(node.getRight(), context));
    }

    @Override
    protected Node visitGroupBy(GroupBy node, Void context)
    {
        ImmutableList.Builder<GroupingElement> rewrittenGroupBy = ImmutableList.builder();
        for (GroupingElement element : node.getGroupingElements()) {
            if (materializedViewInfo.getGroupBy().isPresent() && !materializedViewInfo.getGroupBy().get().contains(element)) {
                throw new IllegalStateException(format("Grouping element %s is not present in materialized view groupBy field", element));
            }
            rewrittenGroupBy.add((GroupingElement) process(element, context));
        }
        return new GroupBy(node.isDistinct(), rewrittenGroupBy.build());
    }

    @Override
    protected Node visitOrderBy(OrderBy node, Void context)
    {
        ImmutableList.Builder<SortItem> rewrittenOrderBy = ImmutableList.builder();
        for (SortItem sortItem : node.getSortItems()) {
            if (!materializedViewInfo.getBaseToViewColumnMap().containsKey(sortItem.getSortKey())) {
                throw new IllegalStateException(format("Sort key is not present in materialized view select fields", sortItem));
            }
            rewrittenOrderBy.add((SortItem) process(sortItem, context));
        }
        return new OrderBy(rewrittenOrderBy.build());
    }

    @Override
    protected Node visitSortItem(SortItem node, Void context)
    {
        return new SortItem((Expression) process(node.getSortKey(), context), node.getOrdering(), node.getNullOrdering());
    }

    @Override
    protected Node visitSimpleGroupBy(SimpleGroupBy node, Void context)
    {
        ImmutableList.Builder<Expression> rewrittenSimpleGroupBy = ImmutableList.builder();
        for (Expression column : node.getExpressions()) {
            rewrittenSimpleGroupBy.add((Expression) process(column, context));
        }
        return new SimpleGroupBy(rewrittenSimpleGroupBy.build());
    }

    private ExtractionResult fromPredicate(RowExpression rewriteLogicExpression)
    {
        return domainTranslator.fromPredicate(session.toConnectorSession(), rewriteLogicExpression, BASIC_COLUMN_EXTRACTOR);
    }

    // Translate expression to RowExpression
    private RowExpression convertToRowExpression(Expression expression, Scope scope)
    {
        //metadata.getColumnHandles()
        //metadata.getColumnMetadata()
        Map<NodeRef<Expression>, Type> expressionTypes = analyzeExpression(expression, scope).getExpressionTypes();
        return SqlToRowExpressionTranslator.translate(expression, expressionTypes, ImmutableMap.of(), metadata.getFunctionAndTypeManager(), session);
    }

    private ExpressionAnalysis analyzeExpression(Expression expression, Scope scope)
    {
        return ExpressionAnalyzer.analyzeExpression(
                session,
                metadata,
                new AllowAllAccessControl(),
                sqlParser,
                scope,
                new Analysis(null, new ArrayList<>(), false),
                expression,
                WarningCollector.NOOP);
    }

    private RowExpression not(RowExpression expression)
    {
        return call("not", new FunctionResolution(metadata.getFunctionAndTypeManager()).notFunction(), expression.getType(), expression);
    }
}

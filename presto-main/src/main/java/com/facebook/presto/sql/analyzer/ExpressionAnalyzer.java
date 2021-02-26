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

import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalParseResult;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.TypeWithName;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorNotFoundException;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.DenyAllAccessControl;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.StandardWarningCode;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.ArrayConstructor;
import com.facebook.presto.sql.tree.AtTimeZone;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BinaryLiteral;
import com.facebook.presto.sql.tree.BindExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CharLiteral;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.CurrentTime;
import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.DecimalLiteral;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Extract;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.IfExpression;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.IntervalLiteral;
import com.facebook.presto.sql.tree.IsNotNullPredicate;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QuantifiedComparisonExpression;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.StackableAstVisitor;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SubqueryExpression;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.sql.tree.TimeLiteral;
import com.facebook.presto.sql.tree.TimestampLiteral;
import com.facebook.presto.sql.tree.TryExpression;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.sql.tree.WindowFrame;
import com.facebook.presto.transaction.TransactionId;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import io.airlift.slice.SliceUtf8;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.isCharacterType;
import static com.facebook.presto.common.type.TypeUtils.isNonDecimalNumericType;
import static com.facebook.presto.metadata.CastType.CAST;
import static com.facebook.presto.metadata.FunctionAndTypeManager.qualifyObjectName;
import static com.facebook.presto.sql.NodeUtils.getSortItemsFromOrderBy;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoAggregateWindowOrGroupingFunctions;
import static com.facebook.presto.sql.analyzer.Analyzer.verifyNoExternalFunctions;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.tryResolveEnumLiteralType;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXPRESSION_NOT_CONSTANT;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_LITERAL;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PARAMETER_USAGE;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.MULTIPLE_FIELDS_FROM_SUBQUERY;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.STANDALONE_LAMBDA;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.TYPE_MISMATCH;
import static com.facebook.presto.sql.analyzer.SemanticExceptions.missingAttributeException;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.tree.Extract.Field.TIMEZONE_HOUR;
import static com.facebook.presto.sql.tree.Extract.Field.TIMEZONE_MINUTE;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.TypeUtils.BIGINT_TYPE;
import static com.facebook.presto.type.TypeUtils.BOOLEAN_TYPE;
import static com.facebook.presto.type.TypeUtils.DATE_TYPE;
import static com.facebook.presto.type.TypeUtils.DOUBLE_TYPE;
import static com.facebook.presto.type.TypeUtils.INTEGER_TYPE;
import static com.facebook.presto.type.TypeUtils.INTERVAL_DAY_TIME_TYPE;
import static com.facebook.presto.type.TypeUtils.INTERVAL_YEAR_MONTH_TYPE;
import static com.facebook.presto.type.TypeUtils.JSON_TYPE;
import static com.facebook.presto.type.TypeUtils.TIMESTAMP_TYPE;
import static com.facebook.presto.type.TypeUtils.TIMESTAMP_WITH_TIME_ZONE_TYPE;
import static com.facebook.presto.type.TypeUtils.TIME_TYPE;
import static com.facebook.presto.type.TypeUtils.TIME_WITH_TIME_ZONE_TYPE;
import static com.facebook.presto.type.TypeUtils.UNKNOWN_TYPE;
import static com.facebook.presto.type.TypeUtils.VARBINARY_TYPE;
import static com.facebook.presto.type.TypeUtils.VARCHAR_TYPE;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampLiteral;
import static com.facebook.presto.util.DateTimeUtils.timeHasTimeZone;
import static com.facebook.presto.util.DateTimeUtils.timestampHasTimeZone;
import static com.facebook.presto.util.LegacyRowFieldOrdinalAccessUtil.parseAnonymousRowFieldOrdinalAccess;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

public class ExpressionAnalyzer
{
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT = 63;
    private static final int MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER = 31;

    private final FunctionAndTypeManager functionAndTypeManager;
    private final Function<Node, StatementAnalyzer> statementAnalyzerFactory;
    private final SemanticTypeProvider symbolTypes;
    private final boolean isDescribe;

    private final Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = new LinkedHashMap<>();
    private final Set<NodeRef<SubqueryExpression>> scalarSubqueries = new LinkedHashSet<>();
    private final Set<NodeRef<ExistsPredicate>> existsSubqueries = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, TypeWithName> expressionCoercions = new LinkedHashMap<>();
    private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
    private final Set<NodeRef<InPredicate>> subqueryInPredicates = new LinkedHashSet<>();
    private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
    private final Map<NodeRef<Expression>, TypeWithName> expressionTypes = new LinkedHashMap<>();
    private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons = new LinkedHashSet<>();
    // For lambda argument references, maps each QualifiedNameReference to the referenced LambdaArgumentDeclaration
    private final Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = new LinkedHashMap<>();
    private final Set<NodeRef<FunctionCall>> windowFunctions = new LinkedHashSet<>();
    private final Multimap<QualifiedObjectName, String> tableColumnReferences = HashMultimap.create();

    private final Optional<TransactionId> transactionId;
    private final Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions;
    private final SqlFunctionProperties sqlFunctionProperties;
    private final List<Expression> parameters;
    private final WarningCollector warningCollector;

    private ExpressionAnalyzer(
            FunctionAndTypeManager functionAndTypeManager,
            Function<Node, StatementAnalyzer> statementAnalyzerFactory,
            Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions,
            Optional<TransactionId> transactionId,
            SqlFunctionProperties sqlFunctionProperties,
            SemanticTypeProvider symbolTypes,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionManager is null");
        this.statementAnalyzerFactory = requireNonNull(statementAnalyzerFactory, "statementAnalyzerFactory is null");
        this.transactionId = requireNonNull(transactionId, "transactionId is null");
        this.sessionFunctions = requireNonNull(sessionFunctions, "sessionFunctions is null");
        this.sqlFunctionProperties = requireNonNull(sqlFunctionProperties, "sqlFunctionProperties is null");
        this.symbolTypes = requireNonNull(symbolTypes, "symbolTypes is null");
        this.parameters = requireNonNull(parameters, "parameters is null");
        this.isDescribe = isDescribe;
        this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
    }

    public Map<NodeRef<FunctionCall>, FunctionHandle> getResolvedFunctions()
    {
        return unmodifiableMap(resolvedFunctions);
    }

    public Map<NodeRef<Expression>, TypeWithName> getExpressionTypes()
    {
        return unmodifiableMap(expressionTypes);
    }

    public TypeWithName setExpressionType(Expression expression, TypeWithName type)
    {
        requireNonNull(expression, "expression cannot be null");
        requireNonNull(type, "type cannot be null");

        expressionTypes.put(NodeRef.of(expression), type);

        return type;
    }

    private TypeWithName getExpressionType(Expression expression)
    {
        requireNonNull(expression, "expression cannot be null");

        TypeWithName type = expressionTypes.get(NodeRef.of(expression));
        checkState(type != null, "Expression not yet analyzed: %s", expression);
        return type;
    }

    public Map<NodeRef<Expression>, TypeWithName> getExpressionCoercions()
    {
        return unmodifiableMap(expressionCoercions);
    }

    public Set<NodeRef<Expression>> getTypeOnlyCoercions()
    {
        return unmodifiableSet(typeOnlyCoercions);
    }

    public Set<NodeRef<InPredicate>> getSubqueryInPredicates()
    {
        return unmodifiableSet(subqueryInPredicates);
    }

    public Map<NodeRef<Expression>, FieldId> getColumnReferences()
    {
        return unmodifiableMap(columnReferences);
    }

    public Map<NodeRef<Identifier>, LambdaArgumentDeclaration> getLambdaArgumentReferences()
    {
        return unmodifiableMap(lambdaArgumentReferences);
    }

    public TypeWithName analyze(Expression expression, Scope scope)
    {
        Visitor visitor = new Visitor(scope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(Context.notInLambda(scope)));
    }

    private TypeWithName analyze(Expression expression, Scope baseScope, Context context)
    {
        Visitor visitor = new Visitor(baseScope, warningCollector);
        return visitor.process(expression, new StackableAstVisitor.StackableAstVisitorContext<>(context));
    }

    public Set<NodeRef<SubqueryExpression>> getScalarSubqueries()
    {
        return unmodifiableSet(scalarSubqueries);
    }

    public Set<NodeRef<ExistsPredicate>> getExistsSubqueries()
    {
        return unmodifiableSet(existsSubqueries);
    }

    public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons()
    {
        return unmodifiableSet(quantifiedComparisons);
    }

    public Set<NodeRef<FunctionCall>> getWindowFunctions()
    {
        return unmodifiableSet(windowFunctions);
    }

    public Multimap<QualifiedObjectName, String> getTableColumnReferences()
    {
        return tableColumnReferences;
    }

    private class Visitor
            extends StackableAstVisitor<TypeWithName, Context>
    {
        // Used to resolve FieldReferences (e.g. during local execution planning)
        private final Scope baseScope;
        private final WarningCollector warningCollector;

        public Visitor(Scope baseScope, WarningCollector warningCollector)
        {
            this.baseScope = requireNonNull(baseScope, "baseScope is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        public TypeWithName process(Node node, @Nullable StackableAstVisitorContext<Context> context)
        {
            if (node instanceof Expression) {
                // don't double process a node
                TypeWithName type = expressionTypes.get(NodeRef.of(((Expression) node)));
                if (type != null) {
                    return type;
                }
            }
            return super.process(node, context);
        }

        @Override
        protected TypeWithName visitRow(Row node, StackableAstVisitorContext<Context> context)
        {
            List<Type> types = node.getItems().stream()
                    .map((child) -> process(child, context).getType())
                    .collect(toImmutableList());

            TypeWithName type = new TypeWithName(RowType.anonymous(types));
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitCurrentTime(CurrentTime node, StackableAstVisitorContext<Context> context)
        {
            if (node.getPrecision() != null) {
                throw new SemanticException(NOT_SUPPORTED, node, "non-default precision not yet supported");
            }

            TypeWithName type;
            switch (node.getFunction()) {
                case DATE:
                    type = DATE_TYPE;
                    break;
                case TIME:
                    type = TIME_WITH_TIME_ZONE_TYPE;
                    break;
                case LOCALTIME:
                    type = TIME_TYPE;
                    break;
                case TIMESTAMP:
                    type = TIMESTAMP_WITH_TIME_ZONE_TYPE;
                    break;
                case LOCALTIMESTAMP:
                    type = TIMESTAMP_TYPE;
                    break;
                default:
                    throw new SemanticException(NOT_SUPPORTED, node, "%s not yet supported", node.getFunction().getName());
            }

            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitSymbolReference(SymbolReference node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                Optional<ResolvedField> resolvedField = context.getContext().getScope().tryResolveField(node, QualifiedName.of(node.getName()));
                if (resolvedField.isPresent() && context.getContext().getFieldToLambdaArgumentDeclaration().containsKey(FieldId.from(resolvedField.get()))) {
                    return setExpressionType(node, resolvedField.get().getType());
                }
            }
            TypeWithName type = symbolTypes.get(node);
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitIdentifier(Identifier node, StackableAstVisitorContext<Context> context)
        {
            ResolvedField resolvedField = context.getContext().getScope().resolveField(node, QualifiedName.of(node.getValue()));
            return handleResolvedField(node, resolvedField, context);
        }

        private TypeWithName handleResolvedField(Expression node, ResolvedField resolvedField, StackableAstVisitorContext<Context> context)
        {
            return handleResolvedField(node, FieldId.from(resolvedField), resolvedField.getField(), context);
        }

        private TypeWithName handleResolvedField(Expression node, FieldId fieldId, Field field, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                LambdaArgumentDeclaration lambdaArgumentDeclaration = context.getContext().getFieldToLambdaArgumentDeclaration().get(fieldId);
                if (lambdaArgumentDeclaration != null) {
                    // Lambda argument reference is not a column reference
                    lambdaArgumentReferences.put(NodeRef.of((Identifier) node), lambdaArgumentDeclaration);
                    return setExpressionType(node, field.getType());
                }
            }

            if (field.getOriginTable().isPresent() && field.getOriginColumnName().isPresent()) {
                tableColumnReferences.put(field.getOriginTable().get(), field.getOriginColumnName().get());
            }

            FieldId previous = columnReferences.put(NodeRef.of(node), fieldId);
            checkState(previous == null, "%s already known to refer to %s", node, previous);
            return setExpressionType(node, field.getType());
        }

        @Override
        protected TypeWithName visitDereferenceExpression(DereferenceExpression node, StackableAstVisitorContext<Context> context)
        {
            QualifiedName qualifiedName = DereferenceExpression.getQualifiedName(node);

            // Handle qualified name
            if (qualifiedName != null) {
                // first, try to match it to a column name
                Scope scope = context.getContext().getScope();
                Optional<ResolvedField> resolvedField = scope.tryResolveField(node, qualifiedName);
                if (resolvedField.isPresent()) {
                    return handleResolvedField(node, resolvedField.get(), context);
                }
                // otherwise, try to match it to an enum literal (eg Mood.HAPPY)
                if (!scope.isColumnReference(qualifiedName)) {
                    Optional<TypeWithName> enumType = tryResolveEnumLiteralType(qualifiedName, functionAndTypeManager);
                    if (enumType.isPresent()) {
                        setExpressionType(node.getBase(), enumType.get());
                        return setExpressionType(node, enumType.get());
                    }
                    throw missingAttributeException(node, qualifiedName);
                }
            }

            TypeWithName baseType = process(node.getBase(), context);
            if (!(baseType.getType() instanceof RowType)) {
                throw new SemanticException(TYPE_MISMATCH, node.getBase(), "Expression %s is not of type ROW", node.getBase());
            }

            RowType rowType = (RowType) baseType.getType();
            String fieldName = node.getField().getValue();

            Type rowFieldType = null;
            for (RowType.Field rowField : rowType.getFields()) {
                if (fieldName.equalsIgnoreCase(rowField.getName().orElse(null))) {
                    rowFieldType = rowField.getType();
                    break;
                }
            }

            if (sqlFunctionProperties.isLegacyRowFieldOrdinalAccessEnabled() && rowFieldType == null) {
                OptionalInt rowIndex = parseAnonymousRowFieldOrdinalAccess(fieldName, rowType.getFields());
                if (rowIndex.isPresent()) {
                    rowFieldType = rowType.getFields().get(rowIndex.getAsInt()).getType();
                }
            }

            if (rowFieldType == null) {
                throw missingAttributeException(node);
            }

            // TODO Field of RowType should use TypeWithName
            return setExpressionType(node, new TypeWithName(rowFieldType));
        }

        @Override
        protected TypeWithName visitNotExpression(NotExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getValue(), BOOLEAN_TYPE, "Value of logical NOT expression");

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitLogicalBinaryExpression(LogicalBinaryExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getLeft(), BOOLEAN_TYPE, "Left side of logical expression");
            coerceType(context, node.getRight(), BOOLEAN_TYPE, "Right side of logical expression");

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitComparisonExpression(ComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            OperatorType operatorType = OperatorType.valueOf(node.getOperator().name());
            return getOperator(context, node, operatorType, node.getLeft(), node.getRight());
        }

        @Override
        protected TypeWithName visitIsNullPredicate(IsNullPredicate node, StackableAstVisitorContext<Context> context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitIsNotNullPredicate(IsNotNullPredicate node, StackableAstVisitorContext<Context> context)
        {
            process(node.getValue(), context);

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitNullIfExpression(NullIfExpression node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName firstType = process(node.getFirst(), context);
            TypeWithName secondType = process(node.getSecond(), context);

            if (!functionAndTypeManager.getCommonSuperType(firstType, secondType).isPresent()) {
                throw new SemanticException(TYPE_MISMATCH, node, "Types are not comparable with NULLIF: %s vs %s", firstType, secondType);
            }

            return setExpressionType(node, firstType);
        }

        @Override
        protected TypeWithName visitIfExpression(IfExpression node, StackableAstVisitorContext<Context> context)
        {
            coerceType(context, node.getCondition(), BOOLEAN_TYPE, "IF condition");

            TypeWithName type;
            if (node.getFalseValue().isPresent()) {
                type = coerceToSingleType(context, node, "Result types for IF must be the same: %s vs %s", node.getTrueValue(), node.getFalseValue().get());
            }
            else {
                type = process(node.getTrueValue(), context);
            }

            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitSearchedCaseExpression(SearchedCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceType(context, whenClause.getOperand(), BOOLEAN_TYPE, "CASE WHEN clause");
            }

            TypeWithName type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                TypeWithName whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        @Override
        protected TypeWithName visitSimpleCaseExpression(SimpleCaseExpression node, StackableAstVisitorContext<Context> context)
        {
            for (WhenClause whenClause : node.getWhenClauses()) {
                coerceToSingleType(context, whenClause, "CASE operand type does not match WHEN clause operand type: %s vs %s", node.getOperand(), whenClause.getOperand());
            }

            TypeWithName type = coerceToSingleType(context,
                    "All CASE results must be the same type: %s",
                    getCaseResultExpressions(node.getWhenClauses(), node.getDefaultValue()));
            setExpressionType(node, type);

            for (WhenClause whenClause : node.getWhenClauses()) {
                TypeWithName whenClauseType = process(whenClause.getResult(), context);
                setExpressionType(whenClause, whenClauseType);
            }

            return type;
        }

        private List<Expression> getCaseResultExpressions(List<WhenClause> whenClauses, Optional<Expression> defaultValue)
        {
            List<Expression> resultExpressions = new ArrayList<>();
            for (WhenClause whenClause : whenClauses) {
                resultExpressions.add(whenClause.getResult());
            }
            defaultValue.ifPresent(resultExpressions::add);
            return resultExpressions;
        }

        @Override
        protected TypeWithName visitCoalesceExpression(CoalesceExpression node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = coerceToSingleType(context, "All COALESCE operands must be the same type: %s", node.getOperands());

            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitArithmeticUnary(ArithmeticUnaryExpression node, StackableAstVisitorContext<Context> context)
        {
            switch (node.getSign()) {
                case PLUS:
                    TypeWithName type = process(node.getValue(), context);

                    if (!isNonDecimalNumericType(type.getType())) {
                        // TODO: figure out a type-agnostic way of dealing with this. Maybe add a special unary operator
                        // that types can chose to implement, or piggyback on the existence of the negation operator
                        throw new SemanticException(TYPE_MISMATCH, node, "Unary '+' operator cannot by applied to %s type", type);
                    }
                    return setExpressionType(node, type);
                case MINUS:
                    return getOperator(context, node, OperatorType.NEGATION, node.getValue());
            }

            throw new UnsupportedOperationException("Unsupported unary operator: " + node.getSign());
        }

        @Override
        protected TypeWithName visitArithmeticBinary(ArithmeticBinaryExpression node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, OperatorType.valueOf(node.getOperator().name()), node.getLeft(), node.getRight());
        }

        @Override
        protected TypeWithName visitLikePredicate(LikePredicate node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName valueType = process(node.getValue(), context);
            if (!isCharacterType(valueType.getType())) {
                coerceType(context, node.getValue(), VARCHAR_TYPE, "Left side of LIKE expression");
            }

            TypeWithName patternType = getVarcharType(node.getPattern(), context);
            coerceType(context, node.getPattern(), patternType, "Pattern for LIKE expression");
            if (node.getEscape().isPresent()) {
                Expression escape = node.getEscape().get();
                TypeWithName escapeType = getVarcharType(escape, context);
                coerceType(context, escape, escapeType, "Escape for LIKE expression");
            }

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        private TypeWithName getVarcharType(Expression value, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = process(value, context);
            if (!(type.getType() instanceof VarcharType)) {
                return VARCHAR_TYPE;
            }
            return type;
        }

        @Override
        protected TypeWithName visitSubscriptExpression(SubscriptExpression node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName baseType = process(node.getBase(), context);
            // Subscript on Row hasn't got a dedicated operator. Its Type is resolved by hand.
            if (baseType.getType() instanceof RowType) {
                if (!(node.getIndex() instanceof LongLiteral)) {
                    throw new SemanticException(
                            INVALID_PARAMETER_USAGE,
                            node.getIndex(),
                            "Subscript expression on ROW requires a constant index");
                }
                TypeWithName indexType = process(node.getIndex(), context);
                if (!indexType.equals(INTEGER_TYPE)) {
                    throw new SemanticException(
                            TYPE_MISMATCH,
                            node.getIndex(),
                            "Subscript expression on ROW requires integer index, found %s", indexType);
                }
                int indexValue = toIntExact(((LongLiteral) node.getIndex()).getValue());
                if (indexValue <= 0) {
                    throw new SemanticException(
                            INVALID_PARAMETER_USAGE,
                            node.getIndex(),
                            "Invalid subscript index: %s. ROW indices start at 1", indexValue);
                }
                List<Type> rowTypes = baseType.getType().getTypeParameters();
                if (indexValue > rowTypes.size()) {
                    throw new SemanticException(
                            INVALID_PARAMETER_USAGE,
                            node.getIndex(),
                            "Subscript index out of bounds: %s, max value is %s", indexValue, rowTypes.size());
                }
                return setExpressionType(node, new TypeWithName(rowTypes.get(indexValue - 1)));
            }
            return getOperator(context, node, SUBSCRIPT, node.getBase(), node.getIndex());
        }

        @Override
        protected TypeWithName visitArrayConstructor(ArrayConstructor node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = coerceToSingleType(context, "All ARRAY elements must be the same type: %s", node.getValues());
            TypeWithName arrayType = functionAndTypeManager.getParameterizedSemanticType(ARRAY.getName(), ImmutableList.of(TypeSignatureParameter.of(type.getTypeSignature())));
            return setExpressionType(node, arrayType);
        }

        @Override
        protected TypeWithName visitStringLiteral(StringLiteral node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = new TypeWithName(VarcharType.createVarcharType(SliceUtf8.countCodePoints(node.getSlice())));
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitCharLiteral(CharLiteral node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = new TypeWithName(CharType.createCharType(node.getValue().length()));
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitBinaryLiteral(BinaryLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARBINARY_TYPE);
        }

        @Override
        protected TypeWithName visitLongLiteral(LongLiteral node, StackableAstVisitorContext<Context> context)
        {
            if (node.getValue() >= Integer.MIN_VALUE && node.getValue() <= Integer.MAX_VALUE) {
                return setExpressionType(node, INTEGER_TYPE);
            }

            return setExpressionType(node, BIGINT_TYPE);
        }

        @Override
        protected TypeWithName visitDoubleLiteral(DoubleLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, DOUBLE_TYPE);
        }

        @Override
        protected TypeWithName visitDecimalLiteral(DecimalLiteral node, StackableAstVisitorContext<Context> context)
        {
            DecimalParseResult parseResult = Decimals.parse(node.getValue());
            return setExpressionType(node, new TypeWithName(parseResult.getType()));
        }

        @Override
        protected TypeWithName visitBooleanLiteral(BooleanLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitGenericLiteral(GenericLiteral node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type;
            try {
                type = functionAndTypeManager.getSemanticType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (!type.equals(JSON_TYPE)) {
                try {
                    functionAndTypeManager.lookupCast(CAST, VARCHAR_TYPE.getTypeSignature(), type.getTypeSignature());
                }
                catch (IllegalArgumentException e) {
                    throw new SemanticException(TYPE_MISMATCH, node, "No literal form for type %s", type);
                }
            }

            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitEnumLiteral(EnumLiteral node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type;
            try {
                type = functionAndTypeManager.getSemanticType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitTimeLiteral(TimeLiteral node, StackableAstVisitorContext<Context> context)
        {
            boolean hasTimeZone;
            try {
                hasTimeZone = timeHasTimeZone(node.getValue());
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(INVALID_LITERAL, node, "'%s' is not a valid time literal", node.getValue());
            }
            TypeWithName type = hasTimeZone ? TIME_WITH_TIME_ZONE_TYPE : TIME_TYPE;
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitTimestampLiteral(TimestampLiteral node, StackableAstVisitorContext<Context> context)
        {
            try {
                if (sqlFunctionProperties.isLegacyTimestamp()) {
                    parseTimestampLiteral(sqlFunctionProperties.getTimeZoneKey(), node.getValue());
                }
                else {
                    parseTimestampLiteral(node.getValue());
                }
            }
            catch (Exception e) {
                throw new SemanticException(INVALID_LITERAL, node, "'%s' is not a valid timestamp literal", node.getValue());
            }

            TypeWithName type;
            if (timestampHasTimeZone(node.getValue())) {
                type = TIMESTAMP_WITH_TIME_ZONE_TYPE;
            }
            else {
                type = TIMESTAMP_TYPE;
            }
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitIntervalLiteral(IntervalLiteral node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type;
            if (node.isYearToMonth()) {
                type = INTERVAL_YEAR_MONTH_TYPE;
            }
            else {
                type = INTERVAL_DAY_TIME_TYPE;
            }
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitNullLiteral(NullLiteral node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, UNKNOWN_TYPE);
        }

        @Override
        protected TypeWithName visitFunctionCall(FunctionCall node, StackableAstVisitorContext<Context> context)
        {
            if (node.getWindow().isPresent()) {
                for (Expression expression : node.getWindow().get().getPartitionBy()) {
                    process(expression, context);
                    TypeWithName type = getExpressionType(expression);
                    if (!type.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "%s is not comparable, and therefore cannot be used in window function PARTITION BY", type);
                    }
                }

                for (SortItem sortItem : getSortItemsFromOrderBy(node.getWindow().get().getOrderBy())) {
                    process(sortItem.getSortKey(), context);
                    TypeWithName type = getExpressionType(sortItem.getSortKey());
                    if (!type.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "%s is not orderable, and therefore cannot be used in window function ORDER BY", type);
                    }
                }

                if (node.getWindow().get().getFrame().isPresent()) {
                    WindowFrame frame = node.getWindow().get().getFrame().get();

                    if (frame.getStart().getValue().isPresent()) {
                        TypeWithName type = process(frame.getStart().getValue().get(), context);
                        if (!type.equals(INTEGER_TYPE) && !type.equals(BIGINT_TYPE)) {
                            throw new SemanticException(TYPE_MISMATCH, node, "Window frame start value type must be INTEGER or BIGINT(actual %s)", type);
                        }
                    }

                    if (frame.getEnd().isPresent() && frame.getEnd().get().getValue().isPresent()) {
                        TypeWithName type = process(frame.getEnd().get().getValue().get(), context);
                        if (!type.equals(INTEGER_TYPE) && !type.equals(BIGINT_TYPE)) {
                            throw new SemanticException(TYPE_MISMATCH, node, "Window frame end value type must be INTEGER or BIGINT (actual %s)", type);
                        }
                    }
                }

                windowFunctions.add(NodeRef.of(node));
            }

            if (node.getFilter().isPresent()) {
                Expression expression = node.getFilter().get();
                process(expression, context);
            }

            ImmutableList.Builder<TypeSignatureProvider> argumentTypesBuilder = ImmutableList.builder();
            for (Expression expression : node.getArguments()) {
                if (expression instanceof LambdaExpression || expression instanceof BindExpression) {
                    argumentTypesBuilder.add(new TypeSignatureProvider(
                            types -> {
                                ExpressionAnalyzer innerExpressionAnalyzer = new ExpressionAnalyzer(
                                        functionAndTypeManager,
                                        statementAnalyzerFactory,
                                        sessionFunctions,
                                        transactionId,
                                        sqlFunctionProperties,
                                        symbolTypes,
                                        parameters,
                                        warningCollector,
                                        isDescribe);
                                if (context.getContext().isInLambda()) {
                                    for (LambdaArgumentDeclaration argument : context.getContext().getFieldToLambdaArgumentDeclaration().values()) {
                                        innerExpressionAnalyzer.setExpressionType(argument, getExpressionType(argument));
                                    }
                                }
                                TypeWithName type = innerExpressionAnalyzer.analyze(expression, baseScope, context.getContext().expectingLambda(types));
                                if (expression instanceof LambdaExpression) {
                                    verifyNoAggregateWindowOrGroupingFunctions(innerExpressionAnalyzer.getResolvedFunctions(), functionAndTypeManager, ((LambdaExpression) expression).getBody(), "Lambda expression");
                                    verifyNoExternalFunctions(innerExpressionAnalyzer.getResolvedFunctions(), functionAndTypeManager, ((LambdaExpression) expression).getBody(), "Lambda expression");
                                }
                                return type.getTypeSignature();
                            }));
                }
                else {
                    argumentTypesBuilder.add(new TypeSignatureProvider(process(expression, context).getTypeSignature()));
                }
            }

            ImmutableList<TypeSignatureProvider> argumentTypes = argumentTypesBuilder.build();
            FunctionHandle function = resolveFunction(sessionFunctions, transactionId, node, argumentTypes, functionAndTypeManager);
            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(function);

            if (node.getOrderBy().isPresent()) {
                for (SortItem sortItem : node.getOrderBy().get().getSortItems()) {
                    TypeWithName sortKeyType = process(sortItem.getSortKey(), context);
                    if (!sortKeyType.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "ORDER BY can only be applied to orderable types (actual: %s)", sortKeyType.getDisplayName());
                    }
                }
            }

            for (int i = 0; i < node.getArguments().size(); i++) {
                Expression expression = node.getArguments().get(i);
                TypeWithName expectedType = functionAndTypeManager.getSemanticType(functionMetadata.getArgumentTypes().get(i));
                requireNonNull(expectedType, format("Type %s not found", functionMetadata.getArgumentTypes().get(i)));
                if (node.isDistinct() && !expectedType.isComparable()) {
                    throw new SemanticException(TYPE_MISMATCH, node, "DISTINCT can only be applied to comparable types (actual: %s)", expectedType);
                }
                if (argumentTypes.get(i).hasDependency()) {
                    FunctionType expectedFunctionType = (FunctionType) expectedType.getType();
                    process(expression, new StackableAstVisitorContext<>(context.getContext().expectingLambda(expectedFunctionType.getArgumentTypes())));
                }
                else {
                    TypeWithName actualType = functionAndTypeManager.getSemanticType(argumentTypes.get(i).getTypeSignature());
                    coerceType(expression, actualType, expectedType, format("Function %s argument %d", function, i));
                }
            }
            resolvedFunctions.put(NodeRef.of(node), function);

            TypeWithName type = functionAndTypeManager.getSemanticType(functionMetadata.getReturnType());
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitAtTimeZone(AtTimeZone node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName valueType = process(node.getValue(), context);
            process(node.getTimeZone(), context);

            if (!isTimeType(valueType)) {
                throw new SemanticException(TYPE_MISMATCH, node.getValue(), "Type of value must be a time or timestamp with or without time zone (actual %s)", valueType);
            }
            TypeWithName resultType = valueType;
            if (valueType.equals(TIME_TYPE)) {
                resultType = TIME_WITH_TIME_ZONE_TYPE;
            }
            else if (valueType.equals(TIMESTAMP_TYPE)) {
                resultType = TIMESTAMP_WITH_TIME_ZONE_TYPE;
            }

            return setExpressionType(node, resultType);
        }

        @Override
        protected TypeWithName visitCurrentUser(CurrentUser node, StackableAstVisitorContext<Context> context)
        {
            return setExpressionType(node, VARCHAR_TYPE);
        }

        @Override
        protected TypeWithName visitParameter(Parameter node, StackableAstVisitorContext<Context> context)
        {
            if (isDescribe) {
                return setExpressionType(node, UNKNOWN_TYPE);
            }
            if (parameters.size() == 0) {
                throw new SemanticException(INVALID_PARAMETER_USAGE, node, "query takes no parameters");
            }
            if (node.getPosition() >= parameters.size()) {
                throw new SemanticException(INVALID_PARAMETER_USAGE, node, "invalid parameter index %s, max value is %s", node.getPosition(), parameters.size() - 1);
            }

            TypeWithName resultType = process(parameters.get(node.getPosition()), context);
            return setExpressionType(node, resultType);
        }

        @Override
        protected TypeWithName visitExtract(Extract node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = process(node.getExpression(), context);
            if (!isDateTimeType(type)) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract must be DATE, TIME, TIMESTAMP, or INTERVAL (actual %s)", type);
            }
            Extract.Field field = node.getField();
            if ((field == TIMEZONE_HOUR || field == TIMEZONE_MINUTE) && !(type.equals(TIME_WITH_TIME_ZONE_TYPE) || type.equals(TIMESTAMP_WITH_TIME_ZONE_TYPE))) {
                throw new SemanticException(TYPE_MISMATCH, node.getExpression(), "Type of argument to extract time zone field must have a time zone (actual %s)", type);
            }

            return setExpressionType(node, BIGINT_TYPE);
        }

        private boolean isTimeType(TypeWithName type)
        {
            return type.equals(TIME_TYPE) ||
                    type.equals(TIME_WITH_TIME_ZONE_TYPE) ||
                    type.equals(TIMESTAMP_TYPE) ||
                    type.equals(TIMESTAMP_WITH_TIME_ZONE_TYPE);
        }

        private boolean isDateTimeType(TypeWithName type)
        {
            return isTimeType(type) ||
                    type.equals(DATE_TYPE) ||
                    type.equals(INTERVAL_DAY_TIME_TYPE) ||
                    type.equals(INTERVAL_YEAR_MONTH_TYPE);
        }

        @Override
        protected TypeWithName visitBetweenPredicate(BetweenPredicate node, StackableAstVisitorContext<Context> context)
        {
            return getOperator(context, node, OperatorType.BETWEEN, node.getValue(), node.getMin(), node.getMax());
        }

        @Override
        public TypeWithName visitTryExpression(TryExpression node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = process(node.getInnerExpression(), context);
            return setExpressionType(node, type);
        }

        @Override
        public TypeWithName visitCast(Cast node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type;
            try {
                type = functionAndTypeManager.getSemanticType(parseTypeSignature(node.getType()));
            }
            catch (IllegalArgumentException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "Unknown type: " + node.getType());
            }

            if (type.equals(UNKNOWN_TYPE)) {
                throw new SemanticException(TYPE_MISMATCH, node, "UNKNOWN is not a valid type");
            }

            TypeWithName value = process(node.getExpression(), context);
            if (!value.equals(UNKNOWN_TYPE) && !node.isTypeOnly()) {
                try {
                    functionAndTypeManager.lookupCast(CAST, value.getTypeSignature(), type.getTypeSignature());
                }
                catch (OperatorNotFoundException e) {
                    throw new SemanticException(TYPE_MISMATCH, node, "Cannot cast %s to %s", value, type);
                }
            }

            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitInPredicate(InPredicate node, StackableAstVisitorContext<Context> context)
        {
            Expression value = node.getValue();
            process(value, context);

            Expression valueList = node.getValueList();
            process(valueList, context);

            if (valueList instanceof InListExpression) {
                InListExpression inListExpression = (InListExpression) valueList;

                coerceToSingleType(context,
                        "IN value and list items must be the same type: %s",
                        ImmutableList.<Expression>builder().add(value).addAll(inListExpression.getValues()).build());
            }
            else if (valueList instanceof SubqueryExpression) {
                coerceToSingleType(context, node, "value and result of subquery must be of the same type for IN expression: %s vs %s", value, valueList);
            }

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitInListExpression(InListExpression node, StackableAstVisitorContext<Context> context)
        {
            TypeWithName type = coerceToSingleType(context, "All IN list values must be the same type: %s", node.getValues());

            setExpressionType(node, type);
            return type; // TODO: this really should a be relation type
        }

        @Override
        protected TypeWithName visitSubqueryExpression(SubqueryExpression node, StackableAstVisitorContext<Context> context)
        {
            if (context.getContext().isInLambda()) {
                throw new SemanticException(NOT_SUPPORTED, node, "Lambda expression cannot contain subqueries");
            }
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node);
            Scope subqueryScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .build();
            Scope queryScope = analyzer.analyze(node.getQuery(), subqueryScope);

            // Subquery should only produce one column
            if (queryScope.getRelationType().getVisibleFieldCount() != 1) {
                throw new SemanticException(MULTIPLE_FIELDS_FROM_SUBQUERY,
                        node,
                        "Multiple columns returned by subquery are not yet supported. Found %s",
                        queryScope.getRelationType().getVisibleFieldCount());
            }

            Node previousNode = context.getPreviousNode().orElse(null);
            if (previousNode instanceof InPredicate && ((InPredicate) previousNode).getValue() != node) {
                subqueryInPredicates.add(NodeRef.of((InPredicate) previousNode));
            }
            else if (previousNode instanceof QuantifiedComparisonExpression) {
                quantifiedComparisons.add(NodeRef.of((QuantifiedComparisonExpression) previousNode));
            }
            else {
                scalarSubqueries.add(NodeRef.of(node));
            }

            TypeWithName type = getOnlyElement(queryScope.getRelationType().getVisibleFields()).getType();
            return setExpressionType(node, type);
        }

        @Override
        protected TypeWithName visitExists(ExistsPredicate node, StackableAstVisitorContext<Context> context)
        {
            StatementAnalyzer analyzer = statementAnalyzerFactory.apply(node);
            Scope subqueryScope = Scope.builder().withParent(context.getContext().getScope()).build();
            analyzer.analyze(node.getSubquery(), subqueryScope);

            existsSubqueries.add(NodeRef.of(node));

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        protected TypeWithName visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, StackableAstVisitorContext<Context> context)
        {
            Expression value = node.getValue();
            process(value, context);

            Expression subquery = node.getSubquery();
            process(subquery, context);

            TypeWithName comparisonType = coerceToSingleType(context, node, "Value expression and result of subquery must be of the same type for quantified comparison: %s vs %s", value, subquery);

            switch (node.getOperator()) {
                case LESS_THAN:
                case LESS_THAN_OR_EQUAL:
                case GREATER_THAN:
                case GREATER_THAN_OR_EQUAL:
                    if (!comparisonType.isOrderable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type [%s] must be orderable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                case EQUAL:
                case NOT_EQUAL:
                    if (!comparisonType.isComparable()) {
                        throw new SemanticException(TYPE_MISMATCH, node, "Type [%s] must be comparable in order to be used in quantified comparison", comparisonType);
                    }
                    break;
                default:
                    throw new IllegalStateException(format("Unexpected comparison type: %s", node.getOperator()));
            }

            return setExpressionType(node, BOOLEAN_TYPE);
        }

        @Override
        public TypeWithName visitFieldReference(FieldReference node, StackableAstVisitorContext<Context> context)
        {
            Field field = baseScope.getRelationType().getFieldByIndex(node.getFieldIndex());
            return handleResolvedField(node, new FieldId(baseScope.getRelationId(), node.getFieldIndex()), field, context);
        }

        @Override
        protected TypeWithName visitLambdaExpression(LambdaExpression node, StackableAstVisitorContext<Context> context)
        {
            if (!context.getContext().isExpectingLambda()) {
                throw new SemanticException(STANDALONE_LAMBDA, node, "Lambda expression should always be used inside a function");
            }

            List<TypeWithName> types = context.getContext().getFunctionInputTypes();
            List<LambdaArgumentDeclaration> lambdaArguments = node.getArguments();

            if (types.size() != lambdaArguments.size()) {
                throw new SemanticException(INVALID_PARAMETER_USAGE, node,
                        format("Expected a lambda that takes %s argument(s) but got %s", types.size(), lambdaArguments.size()));
            }

            ImmutableList.Builder<Field> fields = ImmutableList.builder();
            for (int i = 0; i < lambdaArguments.size(); i++) {
                LambdaArgumentDeclaration lambdaArgument = lambdaArguments.get(i);
                TypeWithName type = types.get(i);
                fields.add(com.facebook.presto.sql.analyzer.Field.newUnqualified(lambdaArgument.getName().getValue(), type));
                setExpressionType(lambdaArgument, type);
            }

            Scope lambdaScope = Scope.builder()
                    .withParent(context.getContext().getScope())
                    .withRelationType(RelationId.of(node), new RelationType(fields.build()))
                    .build();

            ImmutableMap.Builder<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration = ImmutableMap.builder();
            if (context.getContext().isInLambda()) {
                fieldToLambdaArgumentDeclaration.putAll(context.getContext().getFieldToLambdaArgumentDeclaration());
            }
            for (LambdaArgumentDeclaration lambdaArgument : lambdaArguments) {
                ResolvedField resolvedField = lambdaScope.resolveField(lambdaArgument, QualifiedName.of(lambdaArgument.getName().getValue()));
                fieldToLambdaArgumentDeclaration.put(FieldId.from(resolvedField), lambdaArgument);
            }

            TypeWithName returnType = process(node.getBody(), new StackableAstVisitorContext<>(Context.inLambda(lambdaScope, fieldToLambdaArgumentDeclaration.build())));
            FunctionType functionType = new FunctionType(types, new TypeWithName(returnType));
            return setExpressionType(node, new TypeWithName(functionType));
        }

        @Override
        protected TypeWithName visitBindExpression(BindExpression node, StackableAstVisitorContext<Context> context)
        {
            verify(context.getContext().isExpectingLambda(), "bind expression found when lambda is not expected");

            StackableAstVisitorContext<Context> innerContext = new StackableAstVisitorContext<>(context.getContext().notExpectingLambda());
            ImmutableList.Builder<TypeWithName> functionInputTypesBuilder = ImmutableList.builder();
            for (Expression value : node.getValues()) {
                functionInputTypesBuilder.add(process(value, innerContext));
            }
            functionInputTypesBuilder.addAll(context.getContext().getFunctionInputTypes());
            List<TypeWithName> functionInputTypes = functionInputTypesBuilder.build();

            TypeWithName type = process(node.getFunction(), new StackableAstVisitorContext<>(context.getContext().expectingLambda(functionInputTypes)));
            checkState(type.getType() instanceof FunctionType, "Expected physical type of be FunctionType, got %s", type.getType().getClass());
            FunctionType functionType = (FunctionType) type.getType();
            List<TypeWithName> argumentTypes = functionType.getArgumentTypes();
            int numCapturedValues = node.getValues().size();
            verify(argumentTypes.size() == functionInputTypes.size());
            for (int i = 0; i < numCapturedValues; i++) {
                verify(functionInputTypes.get(i).equals(argumentTypes.get(i)));
            }

            FunctionType result = new FunctionType(argumentTypes.subList(numCapturedValues, argumentTypes.size()), functionType.getReturnType());
            return setExpressionType(node, new TypeWithName(result));
        }

        @Override
        protected TypeWithName visitExpression(Expression node, StackableAstVisitorContext<Context> context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected TypeWithName visitNode(Node node, StackableAstVisitorContext<Context> context)
        {
            throw new SemanticException(NOT_SUPPORTED, node, "not yet implemented: " + node.getClass().getName());
        }

        @Override
        public TypeWithName visitGroupingOperation(GroupingOperation node, StackableAstVisitorContext<Context> context)
        {
            if (node.getGroupingColumns().size() > MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT) {
                throw new SemanticException(INVALID_PROCEDURE_ARGUMENTS, node, String.format("GROUPING supports up to %d column arguments", MAX_NUMBER_GROUPING_ARGUMENTS_BIGINT));
            }

            for (Expression columnArgument : node.getGroupingColumns()) {
                process(columnArgument, context);
            }

            if (node.getGroupingColumns().size() <= MAX_NUMBER_GROUPING_ARGUMENTS_INTEGER) {
                return setExpressionType(node, INTEGER_TYPE);
            }
            else {
                return setExpressionType(node, BIGINT_TYPE);
            }
        }

        private TypeWithName getOperator(StackableAstVisitorContext<Context> context, Expression node, OperatorType operatorType, Expression... arguments)
        {
            ImmutableList.Builder<Type> argumentTypes = ImmutableList.builder();
            for (Expression expression : arguments) {
                argumentTypes.add(process(expression, context));
            }

            FunctionMetadata operatorMetadata;
            try {
                operatorMetadata = functionAndTypeManager.getFunctionMetadata(functionAndTypeManager.resolveOperator(operatorType, fromTypes(argumentTypes.build())));
            }
            catch (OperatorNotFoundException e) {
                throw new SemanticException(TYPE_MISMATCH, node, "%s", e.getMessage());
            }
            catch (PrestoException e) {
                if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
                    throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
                }
                throw e;
            }

            for (int i = 0; i < arguments.length; i++) {
                Expression expression = arguments[i];
                TypeWithName type = functionAndTypeManager.getSemanticType(operatorMetadata.getArgumentTypes().get(i));
                coerceType(context, expression, type, format("Operator %s argument %d", operatorMetadata, i));
            }

            TypeWithName type = functionAndTypeManager.getSemanticType(operatorMetadata.getReturnType());
            return setExpressionType(node, type);
        }

        private void coerceType(Expression expression, TypeWithName actualType, TypeWithName expectedType, String message)
        {
            if (!actualType.equals(expectedType)) {
                if (!functionAndTypeManager.canCoerce(actualType, expectedType)) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message + " must evaluate to a %s (actual: %s)", expectedType, actualType);
                }
                addOrReplaceExpressionCoercion(expression, actualType, expectedType);
            }
        }

        private void coerceType(StackableAstVisitorContext<Context> context, Expression expression, TypeWithName expectedType, String message)
        {
            TypeWithName actualType = process(expression, context);
            coerceType(expression, actualType, expectedType, message);
        }

        private TypeWithName coerceToSingleType(StackableAstVisitorContext<Context> context, Node node, String message, Expression first, Expression second)
        {
            TypeWithName unknwon = UNKNOWN_TYPE;
            TypeWithName firstType = unknwon;
            if (first != null) {
                firstType = process(first, context);
            }
            TypeWithName secondType = unknwon;
            if (second != null) {
                secondType = process(second, context);
            }

            // coerce types if possible
            Optional<TypeWithName> superTypeOptional = functionAndTypeManager.getCommonSuperType(firstType, secondType);
            if (superTypeOptional.isPresent()
                    && functionAndTypeManager.canCoerce(firstType, superTypeOptional.get())
                    && functionAndTypeManager.canCoerce(secondType, superTypeOptional.get())) {
                TypeWithName superType = superTypeOptional.get();
                if (!firstType.equals(superType)) {
                    addOrReplaceExpressionCoercion(first, firstType, superType);
                }
                if (!secondType.equals(superType)) {
                    addOrReplaceExpressionCoercion(second, secondType, superType);
                }
                return superType;
            }

            throw new SemanticException(TYPE_MISMATCH, node, message, firstType, secondType);
        }

        private TypeWithName coerceToSingleType(StackableAstVisitorContext<Context> context, String message, List<Expression> expressions)
        {
            // determine super type
            TypeWithName superType = UNKNOWN_TYPE;
            for (Expression expression : expressions) {
                Optional<TypeWithName> newSuperType = functionAndTypeManager.getCommonSuperType(superType, process(expression, context));
                if (!newSuperType.isPresent()) {
                    throw new SemanticException(TYPE_MISMATCH, expression, message, superType.getDisplayName());
                }
                superType = newSuperType.get();
            }

            // verify all expressions can be coerced to the superType
            for (Expression expression : expressions) {
                TypeWithName type = process(expression, context);
                if (!type.equals(superType)) {
                    if (!functionAndTypeManager.canCoerce(type, superType)) {
                        throw new SemanticException(TYPE_MISMATCH, expression, message, superType.getDisplayName());
                    }
                    addOrReplaceExpressionCoercion(expression, type, superType);
                }
            }

            return superType;
        }

        private void addOrReplaceExpressionCoercion(Expression expression, TypeWithName type, TypeWithName superType)
        {
            if (sqlFunctionProperties.isLegacyTypeCoercionWarningEnabled()) {
                if ((type.getTypeSignature().getBase().equals(StandardTypes.DATE) || type.getTypeSignature().getBase().equals(StandardTypes.TIMESTAMP)) && superType.getTypeSignature().getBase().equals(StandardTypes.VARCHAR)) {
                    warningCollector.add(new PrestoWarning(StandardWarningCode.SEMANTIC_WARNING, format("This query relies on legacy semantic behavior that coerces date/timestamp to varchar. Expression: %s", expression)));
                }
            }
            NodeRef<Expression> ref = NodeRef.of(expression);
            expressionCoercions.put(ref, superType);
            if (functionAndTypeManager.isTypeOnlyCoercion(type, superType)) {
                typeOnlyCoercions.add(ref);
            }
            else if (typeOnlyCoercions.contains(ref)) {
                typeOnlyCoercions.remove(ref);
            }
        }
    }

    private static class Context
    {
        private final Scope scope;

        // functionInputTypes and nameToLambdaDeclarationMap can be null or non-null independently. All 4 combinations are possible.

        // The list of types when expecting a lambda (i.e. processing lambda parameters of a function); null otherwise.
        // Empty list represents expecting a lambda with no arguments.
        private final List<TypeWithName> functionInputTypes;
        // The mapping from names to corresponding lambda argument declarations when inside a lambda; null otherwise.
        // Empty map means that the all lambda expressions surrounding the current node has no arguments.
        private final Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration;

        private Context(
                Scope scope,
                List<TypeWithName> functionInputTypes,
                Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            this.scope = requireNonNull(scope, "scope is null");
            this.functionInputTypes = functionInputTypes;
            this.fieldToLambdaArgumentDeclaration = fieldToLambdaArgumentDeclaration;
        }

        public static Context notInLambda(Scope scope)
        {
            return new Context(scope, null, null);
        }

        public static Context inLambda(Scope scope, Map<FieldId, LambdaArgumentDeclaration> fieldToLambdaArgumentDeclaration)
        {
            return new Context(scope, null, requireNonNull(fieldToLambdaArgumentDeclaration, "fieldToLambdaArgumentDeclaration is null"));
        }

        public Context expectingLambda(List<TypeWithName> functionInputTypes)
        {
            return new Context(scope, requireNonNull(functionInputTypes, "functionInputTypes is null"), this.fieldToLambdaArgumentDeclaration);
        }

        public Context notExpectingLambda()
        {
            return new Context(scope, null, this.fieldToLambdaArgumentDeclaration);
        }

        Scope getScope()
        {
            return scope;
        }

        public boolean isInLambda()
        {
            return fieldToLambdaArgumentDeclaration != null;
        }

        public boolean isExpectingLambda()
        {
            return functionInputTypes != null;
        }

        public Map<FieldId, LambdaArgumentDeclaration> getFieldToLambdaArgumentDeclaration()
        {
            checkState(isInLambda());
            return fieldToLambdaArgumentDeclaration;
        }

        public List<TypeWithName> getFunctionInputTypes()
        {
            checkState(isExpectingLambda());
            return functionInputTypes;
        }
    }

    public static FunctionHandle resolveFunction(
            Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions,
            Optional<TransactionId> transactionId,
            FunctionCall node,
            List<TypeSignatureProvider> argumentTypes,
            FunctionAndTypeManager functionAndTypeManager)
    {
        try {
            return functionAndTypeManager.resolveFunction(sessionFunctions, transactionId, qualifyObjectName(node.getName()), argumentTypes);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new SemanticException(SemanticErrorCode.FUNCTION_NOT_FOUND, node, e.getMessage());
            }
            if (e.getErrorCode().getCode() == StandardErrorCode.AMBIGUOUS_FUNCTION_CALL.toErrorCode().getCode()) {
                throw new SemanticException(SemanticErrorCode.AMBIGUOUS_FUNCTION_CALL, node, e.getMessage());
            }
            throw e;
        }
    }

    public static Map<NodeRef<Expression>, TypeWithName> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            SemanticTypeProvider types,
            Expression expression,
            List<Expression> parameters,
            WarningCollector warningCollector)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, expression, parameters, warningCollector, false);
    }

    public static Map<NodeRef<Expression>, TypeWithName> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            SemanticTypeProvider types,
            Expression expression,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return getExpressionTypes(session, metadata, sqlParser, types, ImmutableList.of(expression), parameters, warningCollector, isDescribe);
    }

    public static Map<NodeRef<Expression>, TypeWithName> getExpressionTypes(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            SemanticTypeProvider types,
            Iterable<Expression> expressions,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return analyzeExpressions(session, metadata, sqlParser, types, expressions, parameters, warningCollector, isDescribe).getExpressionTypes();
    }

    public static ExpressionAnalysis analyzeExpressions(
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            SemanticTypeProvider types,
            Iterable<Expression> expressions,
            List<Expression> parameters,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        // expressions at this point can not have sub queries so deny all access checks
        // in the future, we will need a full access controller here to verify access to functions
        Analysis analysis = new Analysis(null, parameters, isDescribe);
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, new DenyAllAccessControl(), types, warningCollector);
        for (Expression expression : expressions) {
            analyzer.analyze(expression, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType()).build());
        }

        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeExpression(
            Session session,
            Metadata metadata,
            AccessControl accessControl,
            SqlParser sqlParser,
            Scope scope,
            Analysis analysis,
            Expression expression,
            WarningCollector warningCollector)
    {
        ExpressionAnalyzer analyzer = create(analysis, session, metadata, sqlParser, accessControl, SemanticTypeProvider.empty(), warningCollector);
        analyzer.analyze(expression, scope);

        Map<NodeRef<Expression>, TypeWithName> expressionTypes = analyzer.getExpressionTypes();
        Map<NodeRef<Expression>, TypeWithName> expressionCoercions = analyzer.getExpressionCoercions();
        Set<NodeRef<Expression>> typeOnlyCoercions = analyzer.getTypeOnlyCoercions();
        Map<NodeRef<FunctionCall>, FunctionHandle> resolvedFunctions = analyzer.getResolvedFunctions();

        analysis.addTypes(expressionTypes);
        analysis.addCoercions(expressionCoercions, typeOnlyCoercions);
        analysis.addFunctionHandles(resolvedFunctions);
        analysis.addColumnReferences(analyzer.getColumnReferences());
        analysis.addLambdaArgumentReferences(analyzer.getLambdaArgumentReferences());
        analysis.addTableColumnReferences(accessControl, session.getIdentity(), analyzer.getTableColumnReferences());

        return new ExpressionAnalysis(
                expressionTypes,
                expressionCoercions,
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
    }

    public static ExpressionAnalysis analyzeSqlFunctionExpression(
            Metadata metadata,
            SqlFunctionProperties sqlFunctionProperties,
            Expression expression,
            Map<String, TypeWithName> argumentTypes)
    {
        ExpressionAnalyzer analyzer = ExpressionAnalyzer.createWithoutSubqueries(
                metadata.getFunctionAndTypeManager(),
                Optional.empty(), // SQL function expression cannot contain session functions
                Optional.empty(),
                sqlFunctionProperties,
                SemanticTypeProvider.copyOf(argumentTypes),
                emptyList(),
                node -> new SemanticException(NOT_SUPPORTED, node, "SQL function does not support subquery"),
                WarningCollector.NOOP,
                false);

        analyzer.analyze(
                expression,
                Scope.builder()
                        .withRelationType(
                                RelationId.anonymous(),
                                new RelationType(argumentTypes.entrySet().stream()
                                        .map(entry -> Field.newUnqualified(entry.getKey(), entry.getValue()))
                                        .collect(toImmutableList()))).build());
        return new ExpressionAnalysis(
                analyzer.getExpressionTypes(),
                analyzer.getExpressionCoercions(),
                analyzer.getSubqueryInPredicates(),
                analyzer.getScalarSubqueries(),
                analyzer.getExistsSubqueries(),
                analyzer.getColumnReferences(),
                analyzer.getTypeOnlyCoercions(),
                analyzer.getQuantifiedComparisons(),
                analyzer.getLambdaArgumentReferences(),
                analyzer.getWindowFunctions());
    }

    private static ExpressionAnalyzer create(
            Analysis analysis,
            Session session,
            Metadata metadata,
            SqlParser sqlParser,
            AccessControl accessControl,
            SemanticTypeProvider types,
            WarningCollector warningCollector)
    {
        return new ExpressionAnalyzer(
                metadata.getFunctionAndTypeManager(),
                node -> new StatementAnalyzer(analysis, metadata, sqlParser, accessControl, session, warningCollector),
                Optional.of(session.getSessionFunctions()),
                session.getTransactionId(),
                session.getSqlFunctionProperties(),
                types,
                analysis.getParameters(),
                warningCollector,
                analysis.isDescribe());
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session, List<Expression> parameters, WarningCollector warningCollector)
    {
        return createWithoutSubqueries(
                metadata.getFunctionAndTypeManager(),
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                warningCollector,
                false);
    }

    public static ExpressionAnalyzer createConstantAnalyzer(Metadata metadata, Session session, List<Expression> parameters, WarningCollector warningCollector, boolean isDescribe)
    {
        return createWithoutSubqueries(
                metadata.getFunctionAndTypeManager(),
                session,
                parameters,
                EXPRESSION_NOT_CONSTANT,
                "Constant expression cannot contain a subquery",
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            List<Expression> parameters,
            SemanticErrorCode errorCode,
            String message,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                functionAndTypeManager,
                session,
                SemanticTypeProvider.empty(),
                parameters,
                node -> new SemanticException(errorCode, node, message),
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionAndTypeManager functionAndTypeManager,
            Session session,
            SemanticTypeProvider symbolTypes,
            List<Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return createWithoutSubqueries(
                functionAndTypeManager,
                Optional.of(session.getSessionFunctions()),
                session.getTransactionId(),
                session.getSqlFunctionProperties(),
                symbolTypes,
                parameters,
                statementAnalyzerRejection,
                warningCollector,
                isDescribe);
    }

    public static ExpressionAnalyzer createWithoutSubqueries(
            FunctionAndTypeManager functionAndTypeManager,
            Optional<Map<SqlFunctionId, SqlInvokedFunction>> sessionFunctions,
            Optional<TransactionId> transactionId,
            SqlFunctionProperties sqlFunctionProperties,
            SemanticTypeProvider symbolTypes,
            List<Expression> parameters,
            Function<? super Node, ? extends RuntimeException> statementAnalyzerRejection,
            WarningCollector warningCollector,
            boolean isDescribe)
    {
        return new ExpressionAnalyzer(
                functionAndTypeManager,
                node -> {
                    throw statementAnalyzerRejection.apply(node);
                },
                sessionFunctions,
                transactionId,
                sqlFunctionProperties,
                symbolTypes,
                parameters,
                warningCollector,
                isDescribe);
    }
}

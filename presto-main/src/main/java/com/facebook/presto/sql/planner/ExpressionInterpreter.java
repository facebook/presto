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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.OperatorInfo;
import com.facebook.presto.metadata.OperatorInfo.OperatorType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.Session;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BetweenPredicate;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.InListExpression;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.IsNullPredicate;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NegativeExpression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NotExpression;
import com.facebook.presto.sql.tree.NullIfExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SearchedCaseExpression;
import com.facebook.presto.sql.tree.SimpleCaseExpression;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.WhenClause;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joni.Regex;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpression;
import static com.facebook.presto.sql.planner.LiteralInterpreter.toExpressions;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.any;

public class ExpressionInterpreter
{
    private final Expression expression;
    private final Metadata metadata;
    private final Session session;
    private final boolean optimize;
    private final IdentityHashMap<Expression, Type> expressionTypes;

    private final Visitor visitor;

    // identity-based cache for LIKE expressions with constant pattern and escape char
    private final IdentityHashMap<LikePredicate, Regex> likePatternCache = new IdentityHashMap<>();
    private final IdentityHashMap<InListExpression, Set<Object>> inListCache = new IdentityHashMap<>();

    public static ExpressionInterpreter expressionInterpreter(Expression expression, Metadata metadata, Session session, IdentityHashMap<Expression, Type> expressionTypes)
    {
        checkNotNull(expression, "expression is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, expressionTypes, false);
    }

    public static ExpressionInterpreter expressionOptimizer(Expression expression, Metadata metadata, Session session, IdentityHashMap<Expression, Type> expressionTypes)
    {
        checkNotNull(expression, "expression is null");
        checkNotNull(metadata, "metadata is null");
        checkNotNull(session, "session is null");

        return new ExpressionInterpreter(expression, metadata, session, expressionTypes, true);
    }

    private ExpressionInterpreter(Expression expression, Metadata metadata, Session session, IdentityHashMap<Expression, Type> expressionTypes, boolean optimize)
    {
        this.expression = expression;
        this.metadata = metadata;
        this.session = session;
        this.expressionTypes = expressionTypes;
        this.optimize = optimize;

        this.visitor = new Visitor();
    }

    public Object evaluate(RecordCursor inputs)
    {
        checkState(!optimize, "evaluate(RecordCursor) not allowed for optimizer");
        return visitor.process(expression, inputs);
    }

    public Object evaluate(BlockCursor[] inputs)
    {
        checkState(!optimize, "evaluate(BlockCursor[]) not allowed for optimizer");
        return visitor.process(expression, inputs);
    }

    public Object optimize(SymbolResolver inputs)
    {
        checkState(optimize, "evaluate(SymbolResolver) not allowed for interpreter");
        return visitor.process(expression, inputs);
    }

    @SuppressWarnings("FloatingPointEquality")
    private class Visitor
            extends AstVisitor<Object, Object>
    {
        @Override
        public Object visitInputReference(InputReference node, Object context)
        {
            Input input = node.getInput();

            int channel = input.getChannel();
            if (context instanceof BlockCursor[]) {
                BlockCursor[] inputs = (BlockCursor[]) context;
                BlockCursor cursor = inputs[channel];

                if (cursor.isNull()) {
                    return null;
                }

                Class<?> javaType = cursor.getType().getJavaType();
                if (javaType == boolean.class) {
                    return cursor.getBoolean();
                }
                else if (javaType == long.class) {
                    return cursor.getLong();
                }
                else if (javaType == double.class) {
                    return cursor.getDouble();
                }
                else if (javaType == Slice.class) {
                    return cursor.getSlice();
                }
                else {
                    throw new UnsupportedOperationException("not yet implemented");
                }
            }
            else if (context instanceof RecordCursor) {
                RecordCursor cursor = (RecordCursor) context;
                if (cursor.isNull(channel)) {
                    return null;
                }

                Class<?> javaType = cursor.getType(input.getChannel()).getJavaType();
                if (javaType == boolean.class) {
                    return cursor.getBoolean(channel);
                }
                else if (javaType == long.class) {
                    return cursor.getLong(channel);
                }
                else if (javaType == double.class) {
                    return cursor.getDouble(channel);
                }
                else if (javaType == Slice.class) {
                    return Slices.wrappedBuffer(cursor.getString(channel));
                }
                else {
                    throw new UnsupportedOperationException("not yet implemented");
                }
            }
            throw new UnsupportedOperationException("Inputs or cursor myst be set");
        }

        @Override
        protected Object visitQualifiedNameReference(QualifiedNameReference node, Object context)
        {
            if (node.getName().getPrefix().isPresent()) {
                // not a symbol
                return node;
            }

            Symbol symbol = Symbol.fromQualifiedName(node.getName());
            return ((SymbolResolver) context).getValue(symbol);
        }

        @Override
        protected Object visitLiteral(Literal node, Object context)
        {
            return LiteralInterpreter.evaluate(metadata, session, node);
        }

        @Override
        protected Object visitIsNullPredicate(IsNullPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value instanceof Expression) {
                return new IsNullPredicate(toExpression(value, expressionTypes.get(node.getValue())));
            }

            return value == null;
        }

        @Override
        protected Object visitSearchedCaseExpression(SearchedCaseExpression node, Object context)
        {
            Expression resultClause = node.getDefaultValue();
            for (WhenClause whenClause : node.getWhenClauses()) {
                Object value = process(whenClause.getOperand(), context);
                if (value instanceof Expression) {
                    // TODO: optimize this case
                    return node;
                }

                if (Boolean.TRUE.equals(value)) {
                    resultClause = whenClause.getResult();
                    break;
                }
            }

            if (resultClause == null) {
                return null;
            }

            Object result = process(resultClause, context);
            if (result instanceof Expression) {
                return node;
            }
            return result;
        }

        @Override
        protected Object visitSimpleCaseExpression(SimpleCaseExpression node, Object context)
        {
            Object operand = process(node.getOperand(), context);
            if (operand instanceof Expression) {
                // TODO: optimize this case
                return node;
            }

            Expression resultClause = node.getDefaultValue();
            if (operand != null) {
                for (WhenClause whenClause : node.getWhenClauses()) {
                    Object value = process(whenClause.getOperand(), context);
                    if (value == null) {
                        continue;
                    }
                    if (value instanceof Expression) {
                        // TODO: optimize this case
                        return node;
                    }

                    if ((Boolean) invokeOperator(OperatorType.EQUAL, types(node.getOperand(), whenClause.getOperand()), ImmutableList.of(operand, value))) {
                        resultClause = whenClause.getResult();
                        break;
                    }
                }
            }
            if (resultClause == null) {
                return null;
            }

            Object result = process(resultClause, context);
            if (result instanceof Expression) {
                return node;
            }
            return result;
        }

        @Override
        protected Object visitCoalesceExpression(CoalesceExpression node, Object context)
        {
            for (Expression expression : node.getOperands()) {
                Object value = process(expression, context);

                if (value instanceof Expression) {
                    // TODO: optimize this case
                    return node;
                }

                if (value != null) {
                    return value;
                }
            }
            return null;
        }

        @Override
        protected Object visitInPredicate(InPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }

            Expression valueListExpression = node.getValueList();
            if (!(valueListExpression instanceof InListExpression)) {
                if (!optimize) {
                    throw new UnsupportedOperationException("IN predicate value list type not yet implemented: " + valueListExpression.getClass().getName());
                }
                return node;
            }
            InListExpression valueList = (InListExpression) valueListExpression;

            Set<Object> set = inListCache.get(valueList);

            // We use the presence of the node in the map to indicate that we've already done
            // the analysis below. If the value is null, it means that we can't apply the HashSet
            // optimization
            if (!inListCache.containsKey(valueList)) {
                if (Iterables.all(valueList.getValues(), isNonNullLiteralPredicate())) {
                    // if all elements are constant, create a set with them
                    set = new HashSet<>();
                    for (Expression expression : valueList.getValues()) {
                        set.add(process(expression, context));
                    }
                }
                inListCache.put(valueList, set);
            }

            if (set != null && !(value instanceof Expression)) {
                return set.contains(value);
            }

            boolean hasUnresolvedValue = false;
            if (value instanceof Expression) {
                hasUnresolvedValue = true;
            }

            boolean hasNullValue = false;
            boolean found = false;
            List<Object> values = new ArrayList<>(valueList.getValues().size());
            List<Type> types = new ArrayList<>(valueList.getValues().size());
            for (Expression expression : valueList.getValues()) {
                Object inValue = process(expression, context);
                if (value instanceof Expression || inValue instanceof Expression) {
                    hasUnresolvedValue = true;
                    values.add(inValue);
                    types.add(expressionTypes.get(expression));
                    continue;
                }

                if (inValue == null) {
                    hasNullValue = true;
                }
                else if (!found && (Boolean) invokeOperator(OperatorType.EQUAL, types(node.getValue(), expression), ImmutableList.of(value, inValue))) {
                    // in does not short-circuit so we must evaluate all value in the list
                    found = true;
                }
            }
            if (found) {
                return true;
            }

            if (hasUnresolvedValue) {
                Type type = expressionTypes.get(node.getValue());
                return new InPredicate(toExpression(value, type), new InListExpression(toExpressions(values, types)));
            }
            if (hasNullValue) {
                return null;
            }
            return false;
        }

        @Override
        protected Object visitNegativeExpression(NegativeExpression node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }
            if (value instanceof Expression) {
                return new NegativeExpression(toExpression(value, expressionTypes.get(node.getValue())));
            }

            OperatorInfo operatorInfo = metadata.resolveOperator(OperatorType.NEGATION, types(node.getValue()));

            MethodHandle handle = operatorInfo.getMethodHandle();
            if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == Session.class) {
                handle = handle.bindTo(session);
            }
            try {
                return handle.invokeWithArguments(value);
            }
            catch (Throwable throwable) {
                Throwables.propagateIfInstanceOf(throwable, RuntimeException.class);
                Throwables.propagateIfInstanceOf(throwable, Error.class);
                throw new RuntimeException(throwable.getMessage(), throwable);
            }
        }

        @Override
        protected Object visitArithmeticExpression(ArithmeticExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            if (left == null) {
                return null;
            }
            Object right = process(node.getRight(), context);
            if (right == null) {
                return null;
            }

            if (hasUnresolvedValue(left, right)) {
                return new ArithmeticExpression(node.getType(), toExpression(left, expressionTypes.get(node.getLeft())), toExpression(right, expressionTypes.get(node.getRight())));
            }

            return invokeOperator(OperatorType.valueOf(node.getType().name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitComparisonExpression(ComparisonExpression node, Object context)
        {
            ComparisonExpression.Type type = node.getType();

            Object left = process(node.getLeft(), context);
            if (left == null && !(type == ComparisonExpression.Type.IS_DISTINCT_FROM)) {
                return null;
            }

            Object right = process(node.getRight(), context);
            if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
                if (left == null && right == null) {
                    return false;
                }
                else if (left == null || right == null) {
                    return true;
                }
            }
            else if (right == null) {
                return null;
            }

            if (hasUnresolvedValue(left, right)) {
                return new ComparisonExpression(type, toExpression(left, expressionTypes.get(node.getLeft())), toExpression(right, expressionTypes.get(node.getRight())));
            }

            if (type == ComparisonExpression.Type.IS_DISTINCT_FROM) {
                type = ComparisonExpression.Type.NOT_EQUAL;
            }

            return invokeOperator(OperatorType.valueOf(type.name()), types(node.getLeft(), node.getRight()), ImmutableList.of(left, right));
        }

        @Override
        protected Object visitBetweenPredicate(BetweenPredicate node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }
            Object min = process(node.getMin(), context);
            if (min == null) {
                return null;
            }
            Object max = process(node.getMax(), context);
            if (max == null) {
                return null;
            }

            if (hasUnresolvedValue(value, min, max)) {
                return new BetweenPredicate(
                        toExpression(value, expressionTypes.get(node.getValue())),
                        toExpression(min, expressionTypes.get(node.getMin())),
                        toExpression(max, expressionTypes.get(node.getMax())));
            }

            return invokeOperator(OperatorType.BETWEEN, types(node.getValue(), node.getMin(), node.getMax()), ImmutableList.of(value, min, max));
        }

        @Override
        protected Object visitNullIfExpression(NullIfExpression node, Object context)
        {
            Object first = process(node.getFirst(), context);
            if (first == null) {
                return null;
            }
            Object second = process(node.getSecond(), context);
            if (second == null) {
                return first;
            }

            if (hasUnresolvedValue(first, second)) {
                return new NullIfExpression(toExpression(first, expressionTypes.get(node.getFirst())), toExpression(second, expressionTypes.get(node.getSecond())));
            }

            if ((Boolean) invokeOperator(OperatorType.EQUAL, types(node.getFirst(), node.getSecond()), ImmutableList.of(first, second))) {
                return null;
            }
            else {
                return first;
            }
        }

        @Override
        protected Object visitNotExpression(NotExpression node, Object context)
        {
            Object value = process(node.getValue(), context);
            if (value == null) {
                return null;
            }

            if (value instanceof Expression) {
                return new NotExpression(toExpression(value, expressionTypes.get(node.getValue())));
            }

            return !(Boolean) value;
        }

        @Override
        protected Object visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context)
        {
            Object left = process(node.getLeft(), context);
            Object right = process(node.getRight(), context);

            switch (node.getType()) {
                case AND: {
                    // if either left or right is false, result is always false regardless of nulls
                    if (Boolean.FALSE.equals(left) || Boolean.TRUE.equals(right)) {
                        return left;
                    }

                    if (Boolean.FALSE.equals(right) || Boolean.TRUE.equals(left)) {
                        return right;
                    }
                }
                case OR: {
                    // if either left or right is true, result is always true regardless of nulls
                    if (Boolean.TRUE.equals(left) || Boolean.FALSE.equals(right)) {
                        return left;
                    }

                    if (Boolean.TRUE.equals(right) || Boolean.FALSE.equals(left)) {
                        return right;
                    }
                }
            }

            if (left == null && right == null) {
                return null;
            }

            return new LogicalBinaryExpression(node.getType(),
                    toExpression(left, expressionTypes.get(node.getLeft())),
                    toExpression(right, expressionTypes.get(node.getRight())));
        }

        @Override
        protected Object visitBooleanLiteral(BooleanLiteral node, Object context)
        {
            return node.equals(BooleanLiteral.TRUE_LITERAL);
        }

        @Override
        protected Object visitFunctionCall(FunctionCall node, Object context)
        {
            List<Type> argumentTypes = new ArrayList<>();
            List<Object> argumentValues = new ArrayList<>();
            for (Expression expression : node.getArguments()) {
                Object value = process(expression, context);
                if (value == null) {
                    return null;
                }
                Type type = expressionTypes.get(expression);
                argumentValues.add(value);
                argumentTypes.add(type);
            }
            FunctionInfo function = metadata.resolveFunction(node.getName(), argumentTypes, false);

            // do not optimize non-deterministic functions
            if (optimize && (!function.isDeterministic() || hasUnresolvedValue(argumentValues))) {
                return new FunctionCall(node.getName(), node.getWindow().orNull(), node.isDistinct(), toExpressions(argumentValues, argumentTypes));
            }
            return invoke(session, function.getScalarFunction(), argumentValues);
        }

        @Override
        protected Object visitLikePredicate(LikePredicate node, Object context)
        {
            Object value = process(node.getValue(), context);

            if (value == null) {
                return null;
            }

            if (value instanceof Slice &&
                    node.getPattern() instanceof StringLiteral &&
                    (node.getEscape() instanceof StringLiteral || node.getEscape() == null)) {
                // fast path when we know the pattern and escape are constant
                return LikeUtils.regexMatches(getConstantPattern(node), (Slice) value);
            }

            Object pattern = process(node.getPattern(), context);

            if (pattern == null) {
                return null;
            }

            Object escape = null;
            if (node.getEscape() != null) {
                escape = process(node.getEscape(), context);

                if (escape == null) {
                    return null;
                }
            }

            if (value instanceof Slice &&
                    pattern instanceof Slice &&
                    (escape == null || escape instanceof Slice)) {
                Regex regex = LikeUtils.likeToPattern((Slice) pattern, (Slice) escape);

                return LikeUtils.regexMatches(regex, (Slice) value);
            }

            // if pattern is a constant without % or _ replace with a comparison
            if (pattern instanceof Slice && escape == null) {
                String stringPattern = ((Slice) pattern).toString(Charsets.UTF_8);
                if (!stringPattern.contains("%") && !stringPattern.contains("_")) {
                    return new ComparisonExpression(ComparisonExpression.Type.EQUAL,
                            toExpression(value, expressionTypes.get(node.getValue())),
                            toExpression(pattern, expressionTypes.get(node.getPattern())));
                }
            }

            Expression optimizedEscape = null;
            if (node.getEscape() != null) {
                optimizedEscape = toExpression(escape, expressionTypes.get(node.getEscape()));
            }

            return new LikePredicate(
                    toExpression(value, expressionTypes.get(node.getValue())),
                    toExpression(pattern, expressionTypes.get(node.getPattern())),
                    optimizedEscape);
        }

        private Regex getConstantPattern(LikePredicate node)
        {
            Regex result = likePatternCache.get(node);

            if (result == null) {
                StringLiteral pattern = (StringLiteral) node.getPattern();
                StringLiteral escape = (StringLiteral) node.getEscape();

                result = LikeUtils.likeToPattern(pattern.getSlice(), escape == null ? null : escape.getSlice());

                likePatternCache.put(node, result);
            }

            return result;
        }

        @Override
        public Object visitCast(Cast node, Object context)
        {
            Object value = process(node.getExpression(), context);

            if (value instanceof Expression) {
                return new Cast((Expression) value, node.getType());
            }

            if (value == null) {
                return null;
            }

            Type type = metadata.getType(node.getType());
            if (type == null) {
                throw new IllegalArgumentException("Unsupported type: " + node.getType());
            }

            OperatorInfo operatorInfo = metadata.getExactOperator(OperatorType.CAST, type, types(node.getExpression()));
            return invoke(session, operatorInfo.getMethodHandle(), ImmutableList.of(value));
        }

        @Override
        protected Object visitExpression(Expression node, Object context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }

        @Override
        protected Object visitNode(Node node, Object context)
        {
            throw new UnsupportedOperationException("Evaluator visitor can only handle Expression nodes");
        }

        private List<Type> types(Expression... types)
        {
            return ImmutableList.copyOf(Iterables.transform(ImmutableList.copyOf(types), Functions.forMap(expressionTypes)));
        }

        private boolean hasUnresolvedValue(Object... values)
        {
            return hasUnresolvedValue(ImmutableList.copyOf(values));
        }

        private boolean hasUnresolvedValue(List<Object> values)
        {
            return any(values, instanceOf(Expression.class));
        }

        private Object invokeOperator(OperatorType operatorType, List<? extends Type> argumentTypes, List<Object> argumentValues)
        {
            OperatorInfo operatorInfo = metadata.resolveOperator(operatorType, argumentTypes);
            return invoke(session, operatorInfo.getMethodHandle(), argumentValues);
        }

        private Object optimize(Node node, Object context)
        {
            checkState(optimize, "not optimizing");
            try {
                return process(node, context);
            }
            catch (RuntimeException e) {
                return node;
            }
        }
    }

    public static Object invoke(Session session, MethodHandle handle, List<Object> argumentValues)
    {
        if (handle.type().parameterCount() > 0 && handle.type().parameterType(0) == Session.class) {
            handle = handle.bindTo(session);
        }
        try {
            return handle.invokeWithArguments(argumentValues);
        }
        catch (Throwable throwable) {
            Throwables.propagateIfInstanceOf(throwable, RuntimeException.class);
            Throwables.propagateIfInstanceOf(throwable, Error.class);
            throw new RuntimeException(throwable.getMessage(), throwable);
        }
    }

    private static Predicate<Expression> isNonNullLiteralPredicate()
    {
        return new Predicate<Expression>()
        {
            @Override
            public boolean apply(@Nullable Expression input)
            {
                return input instanceof Literal && !(input instanceof NullLiteral);
            }
        };
    }
}

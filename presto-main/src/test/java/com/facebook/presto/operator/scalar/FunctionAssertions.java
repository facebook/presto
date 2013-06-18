/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator.scalar;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageIterator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.InMemoryRecordSet;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.Split;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.OperatorFactory;
import com.facebook.presto.sql.planner.InterpretedProjectionFunction;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolToInputRewriter;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.facebook.presto.util.LocalQueryRunner;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.OperatorAssertions.createOperator;
import static com.facebook.presto.sql.parser.SqlParser.createExpression;
import static com.facebook.presto.tuple.Tuples.createTuple;
import static com.facebook.presto.util.LocalQueryRunner.createDualLocalQueryRunner;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public final class FunctionAssertions
{
    public static final Session SESSION = new Session("user", "source", "catalog", "schema", "address", "agent");

    private static final ExpressionCompiler compiler = new ExpressionCompiler(new MetadataManager());

    private static final Operator SOURCE = createOperator(new Page(
            createLongsBlock(1234L),
            createStringsBlock("hello"),
            createDoublesBlock(12.34),
            createBooleansBlock(true),
            createLongsBlock(MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis())),
            createStringsBlock("%el%")));

    private static final Map<Input, Type> INPUT_TYPES = ImmutableMap.<Input, Type>builder()
            .put(new Input(0, 0), Type.LONG)
            .put(new Input(1, 0), Type.STRING)
            .put(new Input(2, 0), Type.DOUBLE)
            .put(new Input(3, 0), Type.BOOLEAN)
            .put(new Input(4, 0), Type.LONG)
            .put(new Input(5, 0), Type.STRING)
            .build();

    private static final Map<Symbol, Input> INPUT_MAPPING = ImmutableMap.<Symbol, Input>builder()
            .put(new Symbol("bound_long"), new Input(0, 0))
            .put(new Symbol("bound_string"), new Input(1, 0))
            .put(new Symbol("bound_double"), new Input(2, 0))
            .put(new Symbol("bound_boolean"), new Input(3, 0))
            .put(new Symbol("bound_timestamp"), new Input(4, 0))
            .put(new Symbol("bound_pattern"), new Input(5, 0))
            .build();

    private FunctionAssertions() {}

    public static void assertFunction(String projection, Object expected)
    {
        if (expected instanceof Integer) {
            expected = ((Integer) expected).longValue();
        }
        else if (expected instanceof Slice) {
            expected = ((Slice) expected).toString(Charsets.UTF_8);
        }
        assertEquals(selectSingleValue(projection), expected);
    }

    public static void assertFunctionNull(String projection)
    {
        assertNull(selectSingleValue(projection));
    }

    public static Object selectSingleValue(String projection)
    {
        return selectSingleValue(projection, createDualLocalQueryRunner(), SESSION);
    }

    public static Object selectSingleValue(String projection, Session session)
    {
        return selectSingleValue(projection, createDualLocalQueryRunner(session), session);
    }

    public static Object selectCompiledSingleValue(String projection)
    {
        checkNotNull(projection, "projection is null");

        // compile
        Operator compiledOperator = createCompiledOperatorFactory(projection).createOperator(SOURCE, SESSION);
        return execute(compiledOperator);
    }

    public static Object selectInterpretedSingleValue(String projection, Type expressionType)
    {
        checkNotNull(projection, "projection is null");

        // compile
        FilterAndProjectOperator interpretedOperator = createInterpretedOperator(projection, expressionType, SESSION);
        return execute(interpretedOperator);
    }

    public static void assertFilter(String expression, boolean expected)
    {
        assertFilter(expression, expected, SESSION);
    }

    public static void assertFilter(String expression, boolean expected, Session session)
    {
        Expression parsedExpression = FunctionAssertions.parseExpression(expression);

        FilterFunction filterFunction;
        try {
            filterFunction = compiler.compileFilterFunction(parsedExpression, INPUT_TYPES, session);
        }
        catch (Throwable e) {
            throw new RuntimeException("Error compiling " + expression, e);
        }

        boolean value = filterFunction.filter(createTuple(1234L),
                createTuple("hello"),
                createTuple(12.34),
                createTuple(true),
                createTuple(MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis())),
                createTuple("%el%"));
        assertEquals(value, expected);
    }

    private static Object selectSingleValue(String projection, LocalQueryRunner runner, Session session)
    {
        checkNotNull(projection, "projection is null");

        // compile operator
        OperatorFactory operatorFactory = createCompiledOperatorFactory(projection);

        // execute using table scan over plain old operator
        Operator operator = operatorFactory.createOperator(createTableScanOperator(SOURCE), session);
        Object directOperatorValue = execute(operator);
        Type expressionType = Type.fromRaw(operator.getTupleInfos().get(0).getTypes().get(0));

        // execute using table scan over record set
        Object recordValue = execute(operatorFactory.createOperator(createTableScanOperator(createRecordProjectOperator()), session));
        assertEquals(recordValue, directOperatorValue);

        // compile projection function
        ProjectionFunction projectFunction = createCompiledProjectFunction(projection, session);
        Object projectedValue = execute(projectFunction);
        assertEquals(projectedValue, directOperatorValue);

        // interpret
        FilterAndProjectOperator interpretedOperator = createInterpretedOperator(projection, expressionType, session);
        Object interpretedValue = execute(interpretedOperator);

        // verify interpreted and compiled value are the same
        assertEquals(interpretedValue, directOperatorValue);

        //
        // If the projection does not need bound values, execute query using full engine
        boolean needsBoundValues = parseExpression(projection).accept(new AstVisitor<Boolean, Object>()
        {
            @Override
            public Boolean visitInputReference(InputReference node, Object context)
            {
                return true;
            }
        }, null) == Boolean.TRUE;

        if (!needsBoundValues) {
            MaterializedResult result = runner.execute("SELECT " + projection + " FROM dual");
            assertEquals(result.getTupleInfo().getFieldCount(), 1);
            assertEquals(result.getMaterializedTuples().size(), 1);
            Object queryResult = Iterables.getOnlyElement(result.getMaterializedTuples()).getField(0);
            assertEquals(directOperatorValue, queryResult);
        }

        return directOperatorValue;
    }

    public static FilterAndProjectOperator createInterpretedOperator(String projection, Type expressionType)
    {
        return createInterpretedOperator(projection, expressionType, SESSION);
    }

    public static FilterAndProjectOperator createInterpretedOperator(String projection, Type expressionType, Session session)
    {
        ProjectionFunction projectionFunction = new InterpretedProjectionFunction(expressionType,
                createExpression(projection),
                INPUT_MAPPING,
                new MetadataManager(),
                session,
                INPUT_TYPES);

        return new FilterAndProjectOperator(SOURCE, FilterFunctions.TRUE_FUNCTION, ImmutableList.of(projectionFunction));
    }

    public static Operator createCompiledOperator(String projection)
    {
        return createCompiledOperatorFactory(projection).createOperator(SOURCE, SESSION);
    }

    public static Operator createCompiledOperator(String projection, Session session)
    {
        return createCompiledOperatorFactory(projection).createOperator(SOURCE, session);
    }

    private static OperatorFactory createCompiledOperatorFactory(String projection)
    {
        Expression parsedExpression = parseExpression(projection);

        // compile and execute
        OperatorFactory operatorFactory;
        try {
            operatorFactory = compiler.compileFilterAndProjectOperator(BooleanLiteral.TRUE_LITERAL, ImmutableList.of(parsedExpression), INPUT_TYPES);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + parsedExpression + ": " + e.getMessage(), e);
        }
        return operatorFactory;
    }

    public static ProjectionFunction createCompiledProjectFunction(String projection, Session session)
    {
        Expression parsedExpression = parseExpression(projection);

        // compile and execute
        try {
            return compiler.compileProjectionFunction(parsedExpression, INPUT_TYPES, session);
        }
        catch (Throwable e) {
            if (e instanceof UncheckedExecutionException) {
                e = e.getCause();
            }
            throw new RuntimeException("Error compiling " + parsedExpression + ": " + e.getMessage(), e);
        }
    }

    public static Object execute(Operator operator)
    {
        PageIterator pageIterator = operator.iterator(new OperatorStats());
        assertTrue(pageIterator.hasNext());
        Page page = pageIterator.next();
        assertFalse(pageIterator.hasNext());

        assertEquals(page.getPositionCount(), 1);
        assertEquals(page.getChannelCount(), 1);

        return getSingleCellValue(page.getBlock(0));
    }

    public static Object execute(ProjectionFunction projectionFunction)
    {
        RecordCursor cursor = createRecordProjectOperator().cursor();
        assertTrue(cursor.advanceNextPosition());

        BlockBuilder output = new BlockBuilder(projectionFunction.getTupleInfo());
        projectionFunction.project(cursor, output);

        assertFalse(cursor.advanceNextPosition());

        return getSingleCellValue(output.build());
    }

    private static Object getSingleCellValue(Block block)
    {
        assertEquals(block.getPositionCount(), 1);
        assertEquals(block.getTupleInfo().getFieldCount(), 1);

        BlockCursor cursor = block.cursor();
        assertTrue(cursor.advanceNextPosition());
        if (cursor.isNull(0)) {
            return null;
        }
        else {
            return cursor.getTuple().toValues().get(0);
        }
    }

    public static Expression parseExpression(String expression)
    {
        Expression parsedExpression = createExpression(expression);

        parsedExpression = TreeRewriter.rewriteWith(new SymbolToInputRewriter(INPUT_MAPPING), parsedExpression);
        return parsedExpression;
    }

    private static RecordProjectOperator createRecordProjectOperator()
    {
        return new RecordProjectOperator(new InMemoryRecordSet(
                ImmutableList.of(
                        ColumnType.LONG,
                        ColumnType.STRING,
                        ColumnType.DOUBLE,
                        ColumnType.LONG,
                        ColumnType.STRING),
                ImmutableList.of(ImmutableList.<Object>of(
                        1234L,
                        "hello",
                        12.34,
                        MILLISECONDS.toSeconds(new DateTime(2001, 8, 22, 3, 4, 5, 321, DateTimeZone.UTC).getMillis()),
                        "%el%"))));
    }

    private static TableScanOperator createTableScanOperator(final Operator source)
    {
        TableScanOperator tableScanOperator = new TableScanOperator(
                new DataStreamProvider()
                {
                    @Override
                    public Operator createDataStream(Split split, List<ColumnHandle> columns)
                    {
                        return source;
                    }
                },
                source.getTupleInfos(),
                ImmutableList.<ColumnHandle>of());

        tableScanOperator.addSplit(new Split()
        {
            @Override
            public boolean isRemotelyAccessible()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<HostAddress> getAddresses()
            {
                throw new UnsupportedOperationException();
            }

            @Override
            public Object getInfo()
            {
                throw new UnsupportedOperationException();
            }
        });

        return tableScanOperator;
    }
}

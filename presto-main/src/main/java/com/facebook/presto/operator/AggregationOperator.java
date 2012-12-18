package com.facebook.presto.operator;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.FixedWidthAggregationFunction;
import com.facebook.presto.operator.aggregation.VariableWidthAggregationFunction;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;


/**
 * Group input data and produce a single block for each sequence of identical values.
 */
public class AggregationOperator
        implements Operator
{
    private final Operator source;
    private final Step step;
    private final List<AggregationFunctionDefinition> functionDefinitions;
    private final List<TupleInfo> tupleInfos;

    public AggregationOperator(Operator source,
            Step step,
            List<AggregationFunctionDefinition> functionDefinitions)
    {
        Preconditions.checkNotNull(source, "source is null");
        Preconditions.checkNotNull(functionDefinitions, "functionDefinitions is null");

        this.source = source;
        this.step = step;
        this.functionDefinitions = ImmutableList.copyOf(functionDefinitions);

        ImmutableList.Builder<TupleInfo> tupleInfos = ImmutableList.builder();
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            if (step != Step.PARTIAL) {
                tupleInfos.add(functionDefinition.getFunction().getFinalTupleInfo());
            }
            else {
                tupleInfos.add(functionDefinition.getFunction().getIntermediateTupleInfo());
            }
        }
        this.tupleInfos = tupleInfos.build();
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public PageIterator iterator(OperatorStats operatorStats)
    {
        // wrapper each function with an aggregator
        ImmutableList.Builder<Aggregator> builder = ImmutableList.builder();
        for (AggregationFunctionDefinition functionDefinition : functionDefinitions) {
            builder.add(createAggregator(functionDefinition, step));
        }
        List<Aggregator> aggregates = builder.build();

        PageIterator iterator = source.iterator(operatorStats);
        while (iterator.hasNext()) {
            Page page = iterator.next();

            // process the row
            for (Aggregator aggregate : aggregates) {
                aggregate.addValue(page);
            }
        }

        // project results into output blocks
        Block[] blocks = new Block[aggregates.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = aggregates.get(i).getResult();
        }

        return PageIterators.singletonIterator(new Page(blocks));
    }

    @SuppressWarnings("rawtypes")
    public static Aggregator createAggregator(AggregationFunctionDefinition functionDefinition, Step step)
    {
        AggregationFunction function = functionDefinition.getFunction();
        if (function instanceof VariableWidthAggregationFunction) {
            return new VariableWidthAggregator((VariableWidthAggregationFunction) functionDefinition.getFunction(), functionDefinition.getChannel(), step);
        }
        else {
            return new FixedWidthAggregator((FixedWidthAggregationFunction) functionDefinition.getFunction(), functionDefinition.getChannel(), step);
        }
    }

    public interface Aggregator
    {
        TupleInfo getTupleInfo();

        void addValue(Page page);

        void addValue(BlockCursor... cursors);

        Block getResult();
    }

    private static class FixedWidthAggregator
            implements Aggregator
    {
        private final FixedWidthAggregationFunction function;
        private final int channel;
        private final Step step;
        private final Slice intermediateValue;

        private FixedWidthAggregator(FixedWidthAggregationFunction function, int channel, Step step)
        {
            this.function = function;
            this.channel = channel;
            this.step = step;
            this.intermediateValue = Slices.allocate(function.getFixedSize());
            function.initialize(intermediateValue, 0);
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                return function.getIntermediateTupleInfo();
            }
            else {
                return function.getFinalTupleInfo();
            }
        }

        @Override
        public void addValue(BlockCursor... cursors)
        {
            BlockCursor cursor;
            if (channel >= 0) {
                cursor = cursors[channel];
            }
            else {
                cursor = null;
            }

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                function.addIntermediate(cursor, intermediateValue, 0);
            }
            else {
                function.addInput(cursor, intermediateValue, 0);
            }
        }

        @Override
        public void addValue(Page page)
        {
            Block block;
            if (channel >= 0) {
                block = page.getBlock(channel);
            }
            else {
                block = null;
            }

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                BlockCursor cursor = block.cursor();
                while (cursor.advanceNextPosition()) {
                    function.addIntermediate(cursor, intermediateValue, 0);
                }
            }
            else {
                function.addInput(page.getPositionCount(), block, intermediateValue, 0);
            }
        }

        @Override
        public Block getResult()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                BlockBuilder output = new BlockBuilder(function.getIntermediateTupleInfo());
                function.evaluateIntermediate(intermediateValue, 0, output);
                return output.build();
            }
            else {
                BlockBuilder output = new BlockBuilder(function.getFinalTupleInfo());
                function.evaluateFinal(intermediateValue, 0, output);
                return output.build();
            }
        }
    }

    private static class VariableWidthAggregator<T>
            implements Aggregator
    {
        private final VariableWidthAggregationFunction<T> function;
        private final int channel;
        private final Step step;
        private T intermediateValue;

        private VariableWidthAggregator(VariableWidthAggregationFunction<T> function, int channel, Step step)
        {
            this.function = function;
            this.channel = channel;
            this.step = step;
            this.intermediateValue = function.initialize();
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                return function.getIntermediateTupleInfo();
            }
            else {
                return function.getFinalTupleInfo();
            }
        }

        @Override
        public void addValue(Page page)
        {
            Block block;
            if (channel >= 0) {
                block = page.getBlock(channel);
            }
            else {
                block = null;
            }

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                BlockCursor cursor = block.cursor();
                while (cursor.advanceNextPosition()) {
                    intermediateValue = function.addIntermediate(cursor, intermediateValue);
                }
            }
            else {
                intermediateValue = function.addInput(page.getPositionCount(), block, intermediateValue);
            }
        }

        @Override
        public void addValue(BlockCursor... cursors)
        {
            BlockCursor cursor;
            if (channel >= 0) {
                cursor = cursors[channel];
            }
            else {
                cursor = null;
            }

            // if this is a final aggregation, the input is an intermediate value
            if (step == Step.FINAL) {
                intermediateValue = function.addIntermediate(cursor, intermediateValue);
            }
            else {
                intermediateValue = function.addInput(cursor, intermediateValue);
            }
        }


        @Override
        public Block getResult()
        {
            // if this is a partial, the output is an intermediate value
            if (step == Step.PARTIAL) {
                BlockBuilder output = new BlockBuilder(function.getIntermediateTupleInfo());
                function.evaluateIntermediate(intermediateValue, output);
                return output.build();
            }
            else {
                BlockBuilder output = new BlockBuilder(function.getFinalTupleInfo());
                function.evaluateFinal(intermediateValue, output);
                return output.build();
            }
        }
    }
}

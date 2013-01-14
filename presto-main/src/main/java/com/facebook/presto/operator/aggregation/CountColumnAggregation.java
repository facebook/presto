package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class CountColumnAggregation
        implements FixedWidthAggregationFunction
{
    public static final CountColumnAggregation COUNT_COLUMN = new CountColumnAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_LONG.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_LONG;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, 0);
    }

    @Override
    public void addInput(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        // todo remove this assumption that the field is 0
        if (cursor.isNull(0)) {
            return;
        }

        // update current value
        long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, currentValue + 1);
    }

    @Override
    public void addInput(int positionCount, Block block, Slice valueSlice, int valueOffset)
    {
        // initialize with current value
        long count = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            // todo remove this assumption that the field is 0
            if (!cursor.isNull(0)) {
                // todo remove this assumption that the field is 0
                count++;
            }
        }

        // write new value
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, count);
    }

    @Override
    public void addIntermediate(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(0)) {
            return;
        }

        // update current value
        long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
        long newValue = cursor.getLong(0);
        SINGLE_LONG.setLong(valueSlice, valueOffset, 0, currentValue + newValue);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        evaluateFinal(valueSlice, valueOffset, output);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        long currentValue = SINGLE_LONG.getLong(valueSlice, valueOffset, 0);
        output.append(currentValue);
    }
}

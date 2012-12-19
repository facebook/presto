package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.tuple.TupleInfo;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;

public class DoubleSumAggregation
        implements FixedWidthAggregationFunction
{
    public static final DoubleSumAggregation DOUBLE_SUM = new DoubleSumAggregation();

    @Override
    public int getFixedSize()
    {
        return SINGLE_DOUBLE.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_DOUBLE;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null
        SINGLE_LONG.setNull(valueSlice, valueOffset, 0);
    }

    @Override
    public void addInput(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        // todo remove this assumption that the field is 0
        if (cursor.isNull(0)) {
            return;
        }

        // mark value not null
        SINGLE_LONG.setNotNull(valueSlice, valueOffset, 0);

        // update current value
        double currentValue = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);
        // todo remove this assumption that the field is 0
        double newValue = cursor.getDouble(0);
        SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, currentValue + newValue);
    }

    @Override
    public void addInput(int positionCount, Block block, Slice valueSlice, int valueOffset)
    {
        // initialize with current value
        boolean hasNonNull = !SINGLE_DOUBLE.isNull(valueSlice, valueOffset);
        double sum = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);

        // process block
        BlockCursor cursor = block.cursor();
        while (cursor.advanceNextPosition()) {
            // todo remove this assumption that the field is 0
            if (!cursor.isNull(0)) {
                hasNonNull = true;
                // todo remove this assumption that the field is 0
                sum += cursor.getDouble(0);
            }
        }

        // write new value
        if (hasNonNull) {
            SINGLE_DOUBLE.setNotNull(valueSlice, valueOffset, 0);
            SINGLE_DOUBLE.setDouble(valueSlice, valueOffset, 0, sum);
        }
    }

    @Override
    public void addIntermediate(BlockCursor cursor, Slice valueSlice, int valueOffset)
    {
        addInput(cursor, valueSlice, valueOffset);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        evaluateFinal(valueSlice, valueOffset, output);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!SINGLE_DOUBLE.isNull(valueSlice, valueOffset, 0)) {
            double currentValue = SINGLE_DOUBLE.getDouble(valueSlice, valueOffset, 0);
            output.append(currentValue);
        } else {
            output.appendNull();
        }
    }
}

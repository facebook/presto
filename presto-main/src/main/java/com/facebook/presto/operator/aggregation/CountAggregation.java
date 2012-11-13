package com.facebook.presto.operator.aggregation;

import com.facebook.presto.tuple.Tuple;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.block.BlockCursor;

import javax.inject.Provider;

public class CountAggregation
        implements AggregationFunction
{
    public static final Provider<AggregationFunction> PROVIDER = new Provider<AggregationFunction>() {
        @Override
        public CountAggregation get()
        {
            return new CountAggregation();
        }
    };

    private long count;

    @Override
    public TupleInfo getTupleInfo()
    {
        return TupleInfo.SINGLE_LONG;
    }

    @Override
    public void add(BlockCursor... cursors)
    {
        count++;
    }

    @Override
    public Tuple evaluate()
    {
        return getTupleInfo().builder()
                .append(count)
                .build();
    }
}

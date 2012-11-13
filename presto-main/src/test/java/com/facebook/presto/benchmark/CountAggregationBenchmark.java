package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchSchema.Orders;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.operator.ProjectionFunctions.singleColumn;
import static com.facebook.presto.operator.aggregation.AggregationFunctions.singleNodeAggregation;
import static com.facebook.presto.operator.aggregation.CountAggregation.countAggregation;

public class CountAggregationBenchmark
        extends AbstractOperatorBenchmark
{
    public CountAggregationBenchmark()
    {
        super("count_agg", 10, 100);
    }

    @Override
    protected Operator createBenchmarkedOperator(TpchBlocksProvider inputStreamProvider)
    {
        BlockIterable orderKey = inputStreamProvider.getBlocks(Orders.ORDERKEY, BlocksFileEncoding.RAW);
        AlignmentOperator alignmentOperator = new AlignmentOperator(orderKey);
        return new AggregationOperator(alignmentOperator, ImmutableList.of(singleNodeAggregation(countAggregation(0, 0))), ImmutableList.of(singleColumn(Type.FIXED_INT_64, 0, 0)));
    }

    public static void main(String[] args)
    {
        new CountAggregationBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}

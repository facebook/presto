package com.facebook.presto.benchmark;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.SourceHashProvider;
import com.facebook.presto.serde.BlocksFileEncoding;
import io.airlift.units.DataSize;

import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class HashBuildBenchmark
        extends AbstractOperatorBenchmark
{
    public HashBuildBenchmark()
    {
        super("hash_build", 4, 5);
    }

    @Override
    protected Operator createBenchmarkedOperator()
    {
        return null;
    }

    @Override
    protected long[] execute(OperatorStats operatorStats)
    {
        BlockIterable orderOrderKey = getBlockIterable("orders", "orderkey", BlocksFileEncoding.RAW);
        BlockIterable totalPrice = getBlockIterable("orders", "totalprice", BlocksFileEncoding.RAW);
        AlignmentOperator ordersTableScan = new AlignmentOperator(orderOrderKey, totalPrice);
        SourceHashProvider sourceHashProvider = new SourceHashProvider(ordersTableScan, 0, 1_500_000, new DataSize(100, MEGABYTE), operatorStats);
        sourceHashProvider.get();
        return new long[] {0, 0};
    }

    public static void main(String[] args)
    {
        new HashBuildBenchmark().runBenchmark(
                new SimpleLineBenchmarkResultWriter(System.out)
        );
    }
}

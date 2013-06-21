package com.facebook.presto.split;

import com.facebook.presto.metadata.TablePartition;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.Partition;
import com.facebook.presto.spi.PartitionKey;
import com.facebook.presto.split.NativeSplitManager.NativePartition;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;

import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class PartitionFunction
    implements Function<TablePartition, Partition>
{
    private final Map<String, ColumnHandle> columnHandles;
    private final Multimap<String, ? extends PartitionKey> allPartitionKeys;


    PartitionFunction(Map<String, ColumnHandle> columnHandles,
                             Multimap<String, ? extends PartitionKey> allPartitionKeys)
    {
        this.columnHandles = checkNotNull(columnHandles, "columnHandles is null");
        this.allPartitionKeys = checkNotNull(allPartitionKeys, "allPartitionKeys is null");
    }

    @Override
    public Partition apply(TablePartition tablePartition)
    {
        String partitionName = tablePartition.getPartitionName();

        ImmutableMap.Builder<ColumnHandle, Object> builder = ImmutableMap.builder();
        for (PartitionKey partitionKey : allPartitionKeys.get(partitionName)) {
            ColumnHandle columnHandle = columnHandles.get(partitionKey.getName());
            checkArgument(columnHandles != null, "Invalid partition key for column %s in partition %s", partitionKey.getName(), tablePartition.getPartitionName());

            String value = partitionKey.getValue();
            switch (partitionKey.getType()) {
            case BOOLEAN:
                if (value.length() == 0) {
                    builder.put(columnHandle, false);
                }
                else {
                    builder.put(columnHandle, Boolean.parseBoolean(value));
                }
                break;
            case LONG:
                if (value.length() == 0) {
                    builder.put(columnHandle, 0L);
                }
                else {
                    builder.put(columnHandle, Long.parseLong(value));
                }
                break;
            case DOUBLE:
                if (value.length() == 0) {
                    builder.put(columnHandle, 0L);
                }
                else {
                    builder.put(columnHandle, Double.parseDouble(value));
                }
                break;
            case STRING:
                builder.put(columnHandle, value);
                break;
            }
        }

        return new NativePartition(tablePartition.getPartitionId(), builder.build());
    }
}

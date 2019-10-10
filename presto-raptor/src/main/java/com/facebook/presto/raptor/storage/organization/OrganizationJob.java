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
package com.facebook.presto.raptor.storage.organization;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.raptor.metadata.ColumnInfo;
import com.facebook.presto.raptor.metadata.MetadataDao;
import com.facebook.presto.raptor.metadata.ShardInfo;
import com.facebook.presto.raptor.metadata.ShardManager;
import com.facebook.presto.raptor.metadata.TableColumn;
import com.facebook.presto.raptor.metadata.TableMetadata;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.UUID;

import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static java.util.Collections.nCopies;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

class OrganizationJob
        implements Runnable
{
    private static final Logger log = Logger.get(OrganizationJob.class);

    private final MetadataDao metadataDao;
    private final ShardManager shardManager;
    private final ShardCompactor compactor;
    private final OrganizationSet organizationSet;

    public OrganizationJob(OrganizationSet organizationSet, MetadataDao metadataDao, ShardManager shardManager, ShardCompactor compactor)
    {
        this.metadataDao = requireNonNull(metadataDao, "metadataDao is null");
        this.shardManager = requireNonNull(shardManager, "shardManager is null");
        this.compactor = requireNonNull(compactor, "compactor is null");
        this.organizationSet = requireNonNull(organizationSet, "organizationSet is null");
    }

    @Override
    public void run()
    {
        try {
            runJob(organizationSet.getTableId(), organizationSet.isTableSupportsDeltaDelete(), organizationSet.getBucketNumber(), organizationSet.getShardsMap());
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void runJob(long tableId, boolean tableSupportsDeltaDelete, OptionalInt bucketNumber, Map<UUID, Optional<UUID>> shardUuidsMap)
            throws IOException
    {
        long transactionId = shardManager.beginTransaction();
        try {
            runJob(transactionId, tableSupportsDeltaDelete, bucketNumber, tableId, shardUuidsMap);
        }
        catch (Throwable e) {
            shardManager.rollbackTransaction(transactionId);
            throw e;
        }
    }

    private void runJob(long transactionId, boolean tableSupportsDeltaDelete, OptionalInt bucketNumber, long tableId, Map<UUID, Optional<UUID>> shardUuidsMap)
            throws IOException
    {
        TableMetadata metadata = getTableMetadata(tableId);
        List<ShardInfo> newShards = performCompaction(transactionId, bucketNumber, shardUuidsMap, metadata);
        log.info("Compacted shards %s into %s", shardUuidsMap, newShards.stream().map(ShardInfo::getShardUuid).collect(toList()));
        shardManager.replaceShardUuids(transactionId, tableSupportsDeltaDelete, tableId, metadata.getColumns(), shardUuidsMap, newShards, OptionalLong.empty());
    }

    private TableMetadata getTableMetadata(long tableId)
    {
        List<TableColumn> sortColumns = metadataDao.listSortColumns(tableId);

        List<Long> sortColumnIds = sortColumns.stream()
                .map(TableColumn::getColumnId)
                .collect(toList());

        List<ColumnInfo> columns = metadataDao.listTableColumns(tableId).stream()
                .map(TableColumn::toColumnInfo)
                .collect(toList());
        return new TableMetadata(tableId, columns, sortColumnIds);
    }

    private List<ShardInfo> performCompaction(long transactionId, OptionalInt bucketNumber, Map<UUID, Optional<UUID>> shardUuidsMap, TableMetadata tableMetadata)
            throws IOException
    {
        if (tableMetadata.getSortColumnIds().isEmpty()) {
            return compactor.compact(transactionId, bucketNumber, shardUuidsMap, tableMetadata.getColumns());
        }
        return compactor.compactSorted(
                transactionId,
                bucketNumber,
                shardUuidsMap,
                tableMetadata.getColumns(),
                tableMetadata.getSortColumnIds(),
                nCopies(tableMetadata.getSortColumnIds().size(), ASC_NULLS_FIRST));
    }
}

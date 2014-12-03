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
package com.facebook.presto.raptor.storage;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static com.facebook.presto.raptor.storage.StorageService.createParents;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrcRowSinkProvider
        implements RowSinkProvider
{
    private final List<Long> columnIds;
    private final List<StorageType> storageTypes;
    private final Optional<Long> sampleWeightColumnId;
    private final StorageService storageService;
    private final List<UUID> shardUuids;

    public OrcRowSinkProvider(List<Long> columnIds, List<StorageType> storageTypes, Optional<Long> sampleWeightColumnId, StorageService storageManagerService)
    {
        this.columnIds = ImmutableList.copyOf(checkNotNull(columnIds, "columnIds is null"));
        this.storageTypes = ImmutableList.copyOf(checkNotNull(storageTypes, "storageTypes is null"));
        this.sampleWeightColumnId = checkNotNull(sampleWeightColumnId, "sampleWeightColumnId is null");
        this.storageService = checkNotNull(storageManagerService, "storageManagerService is null");
        this.shardUuids = new ArrayList<>();
    }

    @Override
    public RowSink getRowSink()
    {
        UUID shardUuid = UUID.randomUUID();
        File stagingFile = storageService.getStagingFile(shardUuid);
        createParents(stagingFile);

        shardUuids.add(shardUuid);
        return new OrcRowSink(columnIds, storageTypes, sampleWeightColumnId, stagingFile);
    }

    @Override
    public List<UUID> getShardUuids()
    {
        return shardUuids;
    }
}

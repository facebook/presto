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
package com.facebook.presto.raptor.metadata;

import java.util.Optional;
import java.util.Set;
import java.util.UUID;

public interface ShardManager
{
    /**
     * Commit an unpartitioned table.
     */
    void commitTable(long tableId, Iterable<ShardNode> shardNodes, Optional<String> externalBatchId);

    /**
     * Return the shard nodes a given table.
     */
    Iterable<ShardNodes> getShardNodes(long tableId);

    /**
     * Return the shards for a given node
     */
    Set<UUID> getNodeShards(String nodeIdentifier);

    /**
     * Drop all shards in a given table.
     */
    void dropTableShards(long tableId);

    /**
     * Assign a shard to a node.
     */
    void assignShard(UUID shardUuid, String nodeIdentifier);
}

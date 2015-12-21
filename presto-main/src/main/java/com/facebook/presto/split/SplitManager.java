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
package com.facebook.presto.split;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.LegacyTableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.FixedSplitSource;
import com.facebook.presto.spi.TransactionalConnectorSplitManager;
import com.google.common.collect.ImmutableList;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

public class SplitManager
{
    private final ConcurrentMap<String, TransactionalConnectorSplitManager> splitManagers = new ConcurrentHashMap<>();

    public void addConnectorSplitManager(String connectorId, TransactionalConnectorSplitManager connectorSplitManager)
    {
        checkState(splitManagers.putIfAbsent(connectorId, connectorSplitManager) == null, "SplitManager for connector '%s' is already registered", connectorId);
    }

    public SplitSource getSplits(Session session, TableLayoutHandle layout)
    {
        String connectorId = layout.getConnectorId();
        TransactionalConnectorSplitManager splitManager = getConnectorSplitManager(connectorId);

        // assumes connectorId and catalog are the same
        ConnectorSession connectorSession = session.toConnectorSession(connectorId);

        ConnectorSplitSource source;
        if (layout.getConnectorHandle() instanceof LegacyTableLayoutHandle) {
            LegacyTableLayoutHandle handle = (LegacyTableLayoutHandle) layout.getConnectorHandle();
            if (handle.getPartitions().isEmpty()) {
                return new ConnectorAwareSplitSource(connectorId, new FixedSplitSource(connectorId, ImmutableList.<ConnectorSplit>of()));
            }

            source = splitManager.getPartitionSplits(layout.getTransactionHandle(), connectorSession, handle.getTable(), handle.getPartitions());
        }
        else {
            source = splitManager.getSplits(layout.getTransactionHandle(), connectorSession, layout.getConnectorHandle());
        }

        return new ConnectorAwareSplitSource(connectorId, source);
    }

    public TransactionalConnectorSplitManager getConnectorSplitManager(String connectorId)
    {
        TransactionalConnectorSplitManager result = splitManagers.get(connectorId);
        checkArgument(result != null, "No split manager for connector '%s'", connectorId);

        return result;
    }
}

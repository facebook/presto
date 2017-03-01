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

package com.facebook.presto.plugin.memory;

import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.connector.Connector;
import com.facebook.presto.spi.connector.ConnectorContext;
import com.facebook.presto.spi.connector.ConnectorFactory;

import java.util.Map;

import static com.google.common.base.MoreObjects.firstNonNull;

public class MemoryConnectorFactory
        implements ConnectorFactory
{
    private final int defaultSplitsPerNode;

    public MemoryConnectorFactory()
    {
        this(Runtime.getRuntime().availableProcessors());
    }

    public MemoryConnectorFactory(int defaultSplitsPerNode)
    {
        this.defaultSplitsPerNode = defaultSplitsPerNode;
    }

    @Override
    public String getName()
    {
        return "memory";
    }

    @Override
    public ConnectorHandleResolver getHandleResolver()
    {
        return new MemoryHandleResolver();
    }

    @Override
    public Connector create(String connectorId, Map<String, String> requiredConfig, ConnectorContext context)
    {
        int splitsPerNode = getSplitsPerNode(requiredConfig);

        return new MemoryConnector(
                new MemoryMetadata(connectorId),
                new MemorySplitManager(context.getNodeManager(), splitsPerNode),
                new MemoryPageSourceProvider(),
                new MemoryPageSinkProvider());
    }

    private int getSplitsPerNode(Map<String, String> properties)
    {
        try {
            return Integer.parseInt(firstNonNull(properties.get("in-memory.splits-per-node"), String.valueOf(defaultSplitsPerNode)));
        }
        catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid property in-memory.splits-per-node");
        }
    }
}

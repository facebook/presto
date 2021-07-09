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
package com.facebook.presto.ttl.fetcherManager;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.execution.scheduler.NodeSchedulerConfig;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.InternalNodeManager;
import com.facebook.presto.spi.ttl.NodeInfo;
import com.facebook.presto.spi.ttl.NodeTTL;
import com.facebook.presto.spi.ttl.TTLFetcher;
import com.facebook.presto.spi.ttl.TTLFetcherFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.threadsNamed;
import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Sets.difference;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;

public class ConfidenceBasedTTLFetcherManager
        implements TTLFetcherManager
{
    private static final Logger log = Logger.get(ConfidenceBasedTTLFetcherManager.class);
    private static final File TTL_FETCHER_CONFIG = new File("etc/ttl-fetcher.properties");
    private static final String TTL_FETCHER_PROPERTY_NAME = "ttl-fetcher.factory";
    private final AtomicReference<TTLFetcher> ttlFetcher = new AtomicReference<>();
    private final InternalNodeManager nodeManager;
    private final ConcurrentHashMap<InternalNode, NodeTTL> nodeTtlMap = new ConcurrentHashMap<>();
    private final boolean isWorkScheduledOnCoordinator;
    private final Map<String, TTLFetcherFactory> ttlFetcherFactories = new ConcurrentHashMap<>();
    private ScheduledExecutorService refreshTtlExecutor;
    private final TTLFetcherManagerConfig ttlFetcherManagerConfig;

    @Inject
    public ConfidenceBasedTTLFetcherManager(InternalNodeManager nodeManager, NodeSchedulerConfig schedulerConfig, TTLFetcherManagerConfig ttlFetcherManagerConfig)
    {
        this.nodeManager = nodeManager;
        this.isWorkScheduledOnCoordinator = schedulerConfig.isIncludeCoordinator();
        this.ttlFetcherManagerConfig = ttlFetcherManagerConfig;
    }

    public void scheduleRefresh()
    {
        refreshTtlExecutor = newSingleThreadScheduledExecutor(threadsNamed("refresh-ttl-executor-%s"));
        refreshTtlExecutor.scheduleWithFixedDelay(() -> {
            try {
                refreshTtlInfo();
            }
            catch (Exception e) {
                log.error(e, "Error fetching TTLs!");
            }
        }, ttlFetcherManagerConfig.getInitialDelayBeforeRefresh().toMillis(), ttlFetcher.get().getRefreshInterval().toMillis(), TimeUnit.MILLISECONDS);
    }

    @PreDestroy
    public void stop()
    {
        refreshTtlExecutor.shutdownNow();
    }

    @VisibleForTesting
    private synchronized void refreshTtlInfo()
    {
        AllNodes allNodes = nodeManager.getAllNodes();
        Set<InternalNode> activeWorkers = Sets.difference(allNodes.getActiveNodes(), allNodes.getActiveResourceManagers());

        if (!isWorkScheduledOnCoordinator) {
            activeWorkers = Sets.difference(activeWorkers, allNodes.getActiveCoordinators());
        }

        Map<NodeInfo, InternalNode> internalNodeMap = activeWorkers
                .stream()
                .collect(toImmutableMap(node -> new NodeInfo(node.getNodeIdentifier(), node.getHost()), Function.identity()));

        log.info("TTL FETCHERS: %s", ttlFetcher);
        Map<NodeInfo, NodeTTL> ttlInfo = ttlFetcher.get().getTTLInfo(ImmutableSet.copyOf(internalNodeMap.keySet()));

        nodeTtlMap.putAll(ttlInfo.entrySet().stream().collect(toImmutableMap(e -> internalNodeMap.get(e.getKey()), Map.Entry::getValue)));

        log.info("Pre-filter nodeTtlMap: %s", nodeTtlMap);

        Set<InternalNode> deadNodes = difference(nodeTtlMap.keySet(), activeWorkers).immutableCopy();
        nodeTtlMap.keySet().removeAll(deadNodes);

        log.info("TTLs refreshed, nodeTtlMap: %s", nodeTtlMap);
    }

    public Optional<NodeTTL> getTTLInfo(InternalNode node)
    {
        return nodeTtlMap.containsKey(node) ? Optional.of(nodeTtlMap.get(node)) : Optional.empty();
    }

    @Override
    public Map<InternalNode, NodeTTL> getAllTTLs()
    {
        return ImmutableMap.copyOf(nodeTtlMap);
    }

    @Override
    public void addTTLFetcherFactory(TTLFetcherFactory ttlFetcherFactory)
    {
        requireNonNull(ttlFetcherFactory, "ttlFetcherFactory is null");

        if (ttlFetcherFactories.putIfAbsent(ttlFetcherFactory.getName(), ttlFetcherFactory) != null) {
            throw new IllegalArgumentException(format("TTL Fetcher factory '%s' is already registered", ttlFetcherFactory.getName()));
        }
    }

    @Override
    public void loadTTLFetcher()
            throws Exception
    {
        String factoryName = "infinite";
        Map<String, String> properties = ImmutableMap.of();

        if (TTL_FETCHER_CONFIG.exists()) {
            properties = new HashMap<>(loadProperties(TTL_FETCHER_CONFIG));
            factoryName = properties.remove(TTL_FETCHER_PROPERTY_NAME);

            checkArgument(!isNullOrEmpty(factoryName),
                    "TTL Fetcher configuration %s does not contain %s", TTL_FETCHER_CONFIG.getAbsoluteFile(), TTL_FETCHER_PROPERTY_NAME);
        }

        load(factoryName, properties);

        if (ttlFetcher.get().needsPeriodicRefresh()) {
            scheduleRefresh();
        }
        else {
            refreshTtlInfo();
        }
    }

    private void load(String factoryName, Map<String, String> properties)
    {
        log.info("-- Loading TTL Fetcher factory --");

        TTLFetcherFactory ttlFetcherFactory = ttlFetcherFactories.get(factoryName);
        checkState(ttlFetcherFactory != null, "TTL Fetcher factory %s is not registered", factoryName);

        TTLFetcher ttlFetcher = ttlFetcherFactory.create(properties);
        checkState(this.ttlFetcher.compareAndSet(null, ttlFetcher), "TTL Fetcher has already been set!");

        log.info("-- Loaded TTL fetcher %s --", factoryName);
    }
}

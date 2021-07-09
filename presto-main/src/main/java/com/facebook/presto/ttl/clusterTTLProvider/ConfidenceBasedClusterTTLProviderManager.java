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
package com.facebook.presto.ttl.clusterTTLProvider;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ttl.ClusterTTLProvider;
import com.facebook.presto.spi.ttl.ClusterTTLProviderFactory;
import com.facebook.presto.spi.ttl.ConfidenceBasedTTLInfo;
import com.facebook.presto.ttl.fetcherManager.TTLFetcherManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.util.PropertiesUtil.loadProperties;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ConfidenceBasedClusterTTLProviderManager
        implements ClusterTTLProviderManager
{
    private static final Logger log = Logger.get(ConfidenceBasedClusterTTLProviderManager.class);
    private static final File CLUSTER_TTL_PROVIDER_CONFIG = new File("etc/cluster-ttl-provider.properties");
    private static final String CLUSTER_TTL_PROVIDER_PROPERTY_NAME = "cluster-ttl-provider.factory";
    private final AtomicReference<ClusterTTLProvider> clusterTTLProvider = new AtomicReference<>();
    private final TTLFetcherManager ttlFetcherManager;
    private final Map<String, ClusterTTLProviderFactory> clusterTTLProviderFactories = new ConcurrentHashMap<>();

    @Inject
    public ConfidenceBasedClusterTTLProviderManager(TTLFetcherManager ttlFetcherManager)
    {
        this.ttlFetcherManager = ttlFetcherManager;
    }

    @Override
    public ConfidenceBasedTTLInfo getClusterTTL()
    {
        return clusterTTLProvider.get().getClusterTTL(ImmutableList.copyOf(ttlFetcherManager.getAllTTLs().values()));
    }

    @Override
    public void addClusterTTLProviderFactory(ClusterTTLProviderFactory clusterTTLProviderFactory)
    {
        requireNonNull(clusterTTLProviderFactory, "clusterTTLProviderFactory is null");
        if (clusterTTLProviderFactories.putIfAbsent(clusterTTLProviderFactory.getName(), clusterTTLProviderFactory) != null) {
            throw new IllegalArgumentException(format("Query Prerequisites '%s' is already registered", clusterTTLProviderFactory.getName()));
        }
    }

    @Override
    public void loadClusterTTLProvider()
            throws Exception
    {
        if (CLUSTER_TTL_PROVIDER_CONFIG.exists()) {
            Map<String, String> properties = new HashMap<>(loadProperties(CLUSTER_TTL_PROVIDER_CONFIG));
            String factoryName = properties.remove(CLUSTER_TTL_PROVIDER_PROPERTY_NAME);

            checkArgument(!isNullOrEmpty(factoryName),
                    "Cluster TTL Provider configuration %s does not contain %s", CLUSTER_TTL_PROVIDER_CONFIG.getAbsoluteFile(), CLUSTER_TTL_PROVIDER_PROPERTY_NAME);
            load(factoryName, properties);
        }
        else {
            load("infinite", ImmutableMap.of());
        }
    }

    private void load(String factoryName, Map<String, String> properties)
    {
        log.info("-- Loading Cluster TTL Provider factory --");

        ClusterTTLProviderFactory clusterTTLProviderFactory = clusterTTLProviderFactories.get(factoryName);
        checkState(clusterTTLProviderFactory != null, "Cluster TTL Provider factory %s is not registered", factoryName);

        ClusterTTLProvider clusterTTLProvider = clusterTTLProviderFactory.create(properties);
        checkState(this.clusterTTLProvider.compareAndSet(null, clusterTTLProvider), "Cluster TTL Provider has already been set!");

        log.info("-- Loaded Cluster TTL Provider %s --", factoryName);
    }
}

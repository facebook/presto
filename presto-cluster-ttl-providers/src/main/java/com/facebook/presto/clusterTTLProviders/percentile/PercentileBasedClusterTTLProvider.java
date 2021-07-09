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

package com.facebook.presto.clusterTTLProviders.percentile;

import com.facebook.presto.spi.ttl.ClusterTTLProvider;
import com.facebook.presto.spi.ttl.ConfidenceBasedTTLInfo;
import com.facebook.presto.spi.ttl.NodeTTL;
import com.google.inject.Inject;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static java.util.stream.Collectors.toList;

public class PercentileBasedClusterTTLProvider
        implements ClusterTTLProvider
{
    private final Comparator<ConfidenceBasedTTLInfo> ttlComparator = Comparator.comparing(ConfidenceBasedTTLInfo::getExpiryInstant);
    private final int percentile;

    @Inject
    public PercentileBasedClusterTTLProvider(PercentileBasedClusterTTLProviderConfig config)
    {
        this.percentile = config.getPercentile();
    }

    @Override
    public ConfidenceBasedTTLInfo getClusterTTL(List<NodeTTL> nodeTTLs)
    {
        List<ConfidenceBasedTTLInfo> temp = nodeTTLs.stream()
                .map(this::getMinTTL)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .sorted(ttlComparator).collect(toList());

        if (temp.size() == 0) {
            return new ConfidenceBasedTTLInfo(0, 100);
        }

        return temp.get(Math.floorDiv(temp.size() * percentile, 100));
    }

    private Optional<ConfidenceBasedTTLInfo> getMinTTL(NodeTTL nodeTTL)
    {
        return nodeTTL.getTTLInfo().stream().min(ttlComparator);
    }
}

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

package com.facebook.presto.clusterTTLProviders.infinite;

import com.facebook.presto.spi.ttl.ClusterTTLProvider;
import com.facebook.presto.spi.ttl.ConfidenceBasedTTLInfo;
import com.facebook.presto.spi.ttl.NodeTTL;

import java.util.List;

import static com.facebook.presto.spi.ttl.ConfidenceBasedTTLInfo.getInfiniteTTL;

public class InfiniteTTLProvider
        implements ClusterTTLProvider
{
    @Override
    public ConfidenceBasedTTLInfo getClusterTTL(List<NodeTTL> nodeTTLs)
    {
        return getInfiniteTTL();
    }
}

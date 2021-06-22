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

package com.facebook.presto.ttl;

import com.facebook.presto.metadata.InternalNode;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

public class NoOpTTLFetcher
        implements TTLFetcher
{
    public NoOpTTLFetcher()
    {
    }

    @Override
    public Map<InternalNode, NodeTTL> getTTLInfo(Set<InternalNode> nodes)
    {
        return nodes.stream().collect(toImmutableMap(identity(), node -> new NodeTTL(ImmutableSet.of())));
    }
}

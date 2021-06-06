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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.DistinctLimitNode;
import com.facebook.presto.spi.plan.PlanNode;

import static com.google.common.base.Preconditions.checkState;

public class DistinctLimitMatcher
        implements Matcher
{
    private final long limit;
    private final boolean partial;

    public DistinctLimitMatcher(long limit, boolean partial)
    {
        this.limit = limit;
        this.partial = partial;
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        if (!(node instanceof DistinctLimitNode)) {
            return false;
        }
        DistinctLimitNode distinctLimitNode = (DistinctLimitNode) node;
        return distinctLimitNode.getLimit() == limit && distinctLimitNode.isPartial() == partial;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node));
        return MatchResult.match();
    }
}

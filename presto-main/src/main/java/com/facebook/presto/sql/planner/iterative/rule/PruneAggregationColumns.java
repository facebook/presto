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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class PruneAggregationColumns
        extends ProjectOffPushDownRule<AggregationNode>
{
    public PruneAggregationColumns()
    {
        super(aggregation());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(
            PlanNodeIdAllocator idAllocator,
            AggregationNode aggregationNode,
            Set<Symbol> referencedOutputs)
    {
        List<Aggregation> prunedAggregations = aggregationNode.getAggregations().stream()
                .filter(aggregation -> referencedOutputs.contains(aggregation.getOutputSymbol()))
                .collect(toImmutableList());

        if (prunedAggregations.size() == aggregationNode.getAggregations().size()) {
            return Optional.empty();
        }

        // PruneAggregationSourceColumns will subsequently project off any newly unused inputs.
        return Optional.of(
                new AggregationNode(
                        aggregationNode.getId(),
                        aggregationNode.getSource(),
                        prunedAggregations,
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashSymbol(),
                        aggregationNode.getGroupIdSymbol()));
    }
}

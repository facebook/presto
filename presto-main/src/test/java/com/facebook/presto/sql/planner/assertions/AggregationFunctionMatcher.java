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
import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.ConversionUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.OrderBy;

import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class AggregationFunctionMatcher
        implements RvalueMatcher
{
    private final ExpectedValueProvider<FunctionCall> callMaker;
    private final FunctionManager functionManager;

    public AggregationFunctionMatcher(FunctionManager functionManager, ExpectedValueProvider<FunctionCall> callMaker)
    {
        this.callMaker = requireNonNull(callMaker, "functionCall is null");
        this.functionManager = requireNonNull(functionManager, "functionManager is null");
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        if (!(node instanceof AggregationNode)) {
            return result;
        }

        AggregationNode aggregationNode = (AggregationNode) node;

        FunctionCall expectedCall = callMaker.getExpectedValue(symbolAliases);
        for (Map.Entry<Symbol, Aggregation> assignment : aggregationNode.getAggregations().entrySet()) {
            if (compare(expectedCall, assignment.getValue())) {
                checkState(!result.isPresent(), "Ambiguous function calls in %s", aggregationNode);
                result = Optional.of(assignment.getKey());
            }
        }

        return result;
    }

    private boolean compare(FunctionCall expected, Aggregation actual)
    {
        return expected.getName().getSuffix() == functionManager.getFunctionMetadata(actual.getFunctionHandle()).getName() &&
                expected.isDistinct() == actual.isDistinct() &&
                expected.getArguments() == actual.getArguments() &&
                expected.getOrderBy().map(OrderBy::getSortItems).map(ConversionUtils::fromSortItems) == actual.getOrderBy() &&
                expected.getFilter() == actual.getFilter();
    }

    @Override
    public String toString()
    {
        return callMaker.toString();
    }
}

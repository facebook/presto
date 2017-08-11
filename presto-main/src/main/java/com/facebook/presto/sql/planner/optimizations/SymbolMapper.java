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
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.util.ValueNodesSymbolsPrunner.pruneValuesNode;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

public class SymbolMapper
{
    private final Map<Symbol, Symbol> mapping;

    public SymbolMapper(Map<Symbol, Symbol> mapping)
    {
        this.mapping = ImmutableMap.copyOf(requireNonNull(mapping, "mapping is null"));
    }

    public Symbol map(Symbol symbol)
    {
        Symbol canonical = symbol;
        while (mapping.containsKey(canonical) && !mapping.get(canonical).equals(canonical)) {
            canonical = mapping.get(canonical);
        }
        return canonical;
    }

    public List<Expression> map(List<Expression> values)
    {
        return values.stream()
                .map(this::map)
                .collect(toImmutableList());
    }

    public Expression map(Expression value)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Symbol canonical = map(Symbol.from(node));
                return canonical.toSymbolReference();
            }
        }, value);
    }

    public <T extends PlanNode> T map(T planNode, PlanNode ... sources)
    {
        return map(planNode, ImmutableList.copyOf(sources));
    }

    public <T extends PlanNode> T map(T planNode, List<PlanNode> sources)
    {
        return map(planNode, inject(sources), PlanNode::getId);
    }

    public <T extends PlanNode> T map(T planNode, PlanNodeIdAllocator idAllocator, PlanNode ... sources)
    {
        return map(planNode, idAllocator, ImmutableList.copyOf(sources));
    }

    public <T extends PlanNode> T map(T planNode, PlanNodeIdAllocator idAllocator, List<PlanNode> sources)
    {
        return map(planNode, inject(sources), node -> idAllocator.getNextId());
    }

    private <T extends PlanNode> T map(T planNode, RecursionStrategy recursionStrategy, PlanNodeIdGenerationStrategy planNodeIdGenerationStrategy)
    {
        return (T) SimplePlanRewriter.rewriteWith(new Rewriter(recursionStrategy, planNodeIdGenerationStrategy), planNode, null);
    }

    private class Rewriter
            extends SimplePlanRewriter<Void>
    {
        private final RecursionStrategy recursionStrategy;
        private final PlanNodeIdGenerationStrategy planNodeIdGenerationStrategy;

        public Rewriter(RecursionStrategy recursionStrategy, PlanNodeIdGenerationStrategy planNodeIdGenerationStrategy)
        {
            this.recursionStrategy = requireNonNull(recursionStrategy, "recursionStrategy is null");
            this.planNodeIdGenerationStrategy = planNodeIdGenerationStrategy;
        }

        @Override
        protected PlanNode visitPlan(PlanNode node, RewriteContext<Void> context)
        {
            // in case you need to have support for some plan node, please see UnaliasSymbolReferences for mapping code
            throw new UnsupportedOperationException("Symbol mapping not yet supported for: " + node.getClass().getName());
        }

        @Override
        public PlanNode visitTopN(TopNNode node, RewriteContext<Void> context)
        {
            PlanNode source = getOnlyElement(recursionStrategy.process(node, context));

            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, SortOrder> orderings = ImmutableMap.builder();
            Set<Symbol> seenCanonicals = new HashSet<>(node.getOrderBy().size());
            for (Symbol symbol : node.getOrderBy()) {
                Symbol canonical = map(symbol);
                if (seenCanonicals.add(canonical)) {
                    seenCanonicals.add(canonical);
                    symbols.add(canonical);
                    orderings.put(canonical, node.getOrderings().get(symbol));
                }
            }

            return new TopNNode(
                    planNodeIdGenerationStrategy.getPlanNodeId(node),
                    source,
                    node.getCount(),
                    symbols.build(),
                    orderings.build(),
                    node.getStep());
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Void> context)
        {
            PlanNode source = getOnlyElement(recursionStrategy.process(node, context));

            ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
            for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();
                Aggregation aggregation = entry.getValue();

                aggregations.put(map(symbol), new Aggregation(
                        (FunctionCall) map(aggregation.getCall()),
                        aggregation.getSignature(),
                        aggregation.getMask().map(SymbolMapper.this::map)));
            }

            List<List<Symbol>> groupingSets = node.getGroupingSets().stream()
                    .map(this::mapAndDistinct)
                    .collect(toImmutableList());

            return new AggregationNode(
                    planNodeIdGenerationStrategy.getPlanNodeId(node),
                    source,
                    aggregations.build(),
                    groupingSets,
                    node.getStep(),
                    node.getHashSymbol().map(SymbolMapper.this::map),
                    node.getGroupIdSymbol().map(SymbolMapper.this::map));
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            Expression originalConstraint = null;
            if (node.getOriginalConstraint() != null) {
                originalConstraint = map(node.getOriginalConstraint());
            }
            return new TableScanNode(
                    planNodeIdGenerationStrategy.getPlanNodeId(node),
                    node.getTable(),
                    mapAndDistinct(node.getOutputSymbols()),
                    mapAndDistinct(node.getAssignments()),
                    node.getLayout(),
                    node.getCurrentConstraint(),
                    originalConstraint);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Void> context)
        {
            PlanNode source = getOnlyElement(recursionStrategy.process(node, context));
            return new ProjectNode(
                    planNodeIdGenerationStrategy.getPlanNodeId(node),
                    source,
                    mapAndDistinct(node.getAssignments()));
        }

        @Override
        public PlanNode visitApply(ApplyNode node, RewriteContext<Void> context)
        {
            List<PlanNode> sources = recursionStrategy.process(node, context);
            checkState(sources.size() == 2, "Apply node expects two sources only, but got: %s", sources.size());

            return new ApplyNode(
                    planNodeIdGenerationStrategy.getPlanNodeId(node),
                    sources.get(0),
                    sources.get(1),
                    mapAndDistinct(node.getSubqueryAssignments()),
                    mapAndDistinct(node.getCorrelation()),
                    node.getOriginSubquery());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<Void> context)
        {
            ValuesNode intermediate = new ValuesNode(
                    node.getId(),
                    node.getOutputSymbols().stream()
                        .map(SymbolMapper.this::map)
                        .collect(toImmutableList()),
                    node.getRows().stream()
                            .map(SymbolMapper.this::map)
                            .collect(toImmutableList()));

            Set<Symbol> added = new HashSet<>(); // makes sure that each symbol is added exactly once
            return pruneValuesNode(intermediate, planNodeIdGenerationStrategy.getPlanNodeId(node), added::add);
        }

        private List<Symbol> mapAndDistinct(List<Symbol> outputs)
        {
            Set<Symbol> added = new HashSet<>();
            ImmutableList.Builder<Symbol> builder = ImmutableList.builder();
            for (Symbol symbol : outputs) {
                Symbol canonical = map(symbol);
                if (added.add(canonical)) {
                    builder.add(canonical);
                }
            }
            return builder.build();
        }

        private <T> Map<Symbol, T> mapAndDistinct(Map<Symbol, T> map)
        {
            Map<Symbol, T> result = new HashMap<>();
            for (Symbol symbol : map.keySet()) {
                Symbol canonical = map(symbol);
                if (!result.containsKey(canonical)) {
                    result.put(canonical, map.get(symbol));
                }
            }
            return ImmutableMap.copyOf(result);
        }

        private Assignments mapAndDistinct(Assignments assignments)
        {
            Set<Symbol> added = new HashSet<>();
            Assignments.Builder builder = Assignments.builder();
            for (Symbol symbol : assignments.getSymbols()) {
                Symbol canonical = map(symbol);
                if (added.add(canonical)) {
                    builder.put(canonical, map(assignments.get(symbol)));
                }
            }
            return builder.build();
        }
    }

    public static SymbolMapper.Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private ImmutableMap.Builder<Symbol, Symbol> mappings = ImmutableMap.builder();

        public Builder put(Symbol from, Symbol to)
        {
            mappings.put(from, to);
            return this;
        }

        public SymbolMapper build()
        {
            return new SymbolMapper(mappings.build());
        }
    }

    private static RecursionStrategy inject(List<PlanNode> sources)
    {
        return (node, context) -> sources;
    }

    @FunctionalInterface
    private interface RecursionStrategy
    {
        List<PlanNode> process(PlanNode node, SimplePlanRewriter.RewriteContext<Void> context);
    }

    @FunctionalInterface
    private interface PlanNodeIdGenerationStrategy
    {
        PlanNodeId getPlanNodeId(PlanNode originNode);
    }
}

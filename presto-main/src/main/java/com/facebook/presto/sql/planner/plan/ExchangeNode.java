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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.OrderingScheme;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.PartitioningHandle;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_ARBITRARY_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_BROADCAST_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_PASSTHROUGH_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.optimizations.AddExchanges.toVariableReferences;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.util.MoreLists.listOfListsCopy;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

@Immutable
public class ExchangeNode
        extends PlanNode
{
    public enum Type
    {
        GATHER,
        REPARTITION,
        REPLICATE
    }

    public enum Scope
    {
        LOCAL(false),
        REMOTE_STREAMING(true),
        REMOTE_MATERIALIZED(true),
        /**/;

        private boolean remote;

        Scope(boolean remote)
        {
            this.remote = remote;
        }

        public boolean isRemote()
        {
            return remote;
        }

        public boolean isLocal()
        {
            return !isRemote();
        }
    }

    private final Type type;
    private final Scope scope;

    private final List<PlanNode> sources;

    private final PartitioningScheme partitioningScheme;

    // for each source, the list of inputs corresponding to each output
    private final List<List<VariableReferenceExpression>> inputs;

    private final Optional<OrderingScheme> orderingScheme;

    @JsonCreator
    public ExchangeNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("scope") Scope scope,
            @JsonProperty("partitioningScheme") PartitioningScheme partitioningScheme,
            @JsonProperty("sources") List<PlanNode> sources,
            @JsonProperty("inputs") List<List<VariableReferenceExpression>> inputs,
            @JsonProperty("orderingScheme") Optional<OrderingScheme> orderingScheme)
    {
        super(id);

        requireNonNull(type, "type is null");
        requireNonNull(scope, "scope is null");
        requireNonNull(sources, "sources is null");
        requireNonNull(partitioningScheme, "partitioningScheme is null");
        requireNonNull(inputs, "inputs is null");
        requireNonNull(orderingScheme, "orderingScheme is null");

        checkArgument(!inputs.isEmpty(), "inputs is empty");
        checkArgument(inputs.stream().allMatch(inputSymbols -> inputSymbols.size() == partitioningScheme.getOutputLayout().size()), "Input symbols do not match output symbols");
        checkArgument(inputs.size() == sources.size(), "Must have same number of input lists as sources");
        for (int i = 0; i < inputs.size(); i++) {
            checkArgument(sources.get(i).getOutputSymbols().stream().map(Symbol::getName).collect(toImmutableSet()).containsAll(inputs.get(i).stream().map(VariableReferenceExpression::getName).collect(toImmutableSet())), "Source does not supply all required input variables");
        }

        checkArgument(!scope.isLocal() || partitioningScheme.getPartitioning().getArguments().stream().allMatch(ArgumentBinding::isVariable),
                "local exchanges do not support constant partition function arguments");

        checkArgument(!scope.isRemote() || type == REPARTITION || !partitioningScheme.isReplicateNullsAndAny(), "Only REPARTITION can replicate remotely");
        checkArgument(scope != REMOTE_MATERIALIZED || type == REPARTITION, "Only REPARTITION can be REMOTE_MATERIALIZED: %s", type);

        orderingScheme.ifPresent(ordering -> {
            PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
            checkArgument(!scope.isRemote() || partitioningHandle.equals(SINGLE_DISTRIBUTION), "remote merging exchange requires single distribution");
            checkArgument(!scope.isLocal() || partitioningHandle.equals(FIXED_PASSTHROUGH_DISTRIBUTION), "local merging exchange requires passthrough distribution");
            checkArgument(partitioningScheme.getOutputLayout().containsAll(ordering.getOrderBy().stream().map(VariableReferenceExpression::getName).map(Symbol::new).collect(toImmutableList())), "Partitioning scheme does not supply all required ordering symbols");
        });
        this.type = type;
        this.sources = sources;
        this.scope = scope;
        this.partitioningScheme = partitioningScheme;
        this.inputs = listOfListsCopy(inputs);
        this.orderingScheme = orderingScheme;
    }

    public static ExchangeNode systemPartitionedExchange(PlanNodeId id, Scope scope, PlanNode child, List<VariableReferenceExpression> partitioningColumns, Optional<VariableReferenceExpression> hashColumn, TypeProvider types)
    {
        return systemPartitionedExchange(id, scope, child, partitioningColumns, hashColumn, types, false);
    }

    public static ExchangeNode systemPartitionedExchange(PlanNodeId id, Scope scope, PlanNode child, List<VariableReferenceExpression> partitioningColumns, Optional<VariableReferenceExpression> hashColumn, TypeProvider types, boolean replicateNullsAndAny)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                Partitioning.create(FIXED_HASH_DISTRIBUTION, partitioningColumns),
                hashColumn,
                types,
                replicateNullsAndAny);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, Partitioning partitioning, Optional<VariableReferenceExpression> hashColumn, TypeProvider types)
    {
        return partitionedExchange(id, scope, child, partitioning, hashColumn, types, false);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, Partitioning partitioning, Optional<VariableReferenceExpression> hashColumn, TypeProvider types, boolean replicateNullsAndAny)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(
                        partitioning,
                        toVariableReferences(child.getOutputSymbols(), types),
                        hashColumn,
                        replicateNullsAndAny,
                        Optional.empty()),
                types);
    }

    public static ExchangeNode partitionedExchange(PlanNodeId id, Scope scope, PlanNode child, PartitioningScheme partitioningScheme, TypeProvider types)
    {
        if (partitioningScheme.getPartitioning().getHandle().isSingleNode()) {
            return gatheringExchange(id, scope, child, types);
        }
        return new ExchangeNode(
                id,
                REPARTITION,
                scope,
                partitioningScheme,
                ImmutableList.of(child),
                ImmutableList.of(partitioningScheme.getOutputLayout()),
                Optional.empty());
    }

    public static ExchangeNode replicatedExchange(PlanNodeId id, Scope scope, PlanNode child, TypeProvider types)
    {
        return new ExchangeNode(
                id,
                REPLICATE,
                scope,
                new PartitioningScheme(Partitioning.create(FIXED_BROADCAST_DISTRIBUTION, ImmutableList.of()), toVariableReferences(child.getOutputSymbols(), types)),
                ImmutableList.of(child),
                ImmutableList.of(toVariableReferences(child.getOutputSymbols(), types)),
                Optional.empty());
    }

    public static ExchangeNode gatheringExchange(PlanNodeId id, Scope scope, PlanNode child, TypeProvider types)
    {
        return new ExchangeNode(
                id,
                GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), toVariableReferences(child.getOutputSymbols(), types)),
                ImmutableList.of(child),
                ImmutableList.of(toVariableReferences(child.getOutputSymbols(), types)),
                Optional.empty());
    }

    public static ExchangeNode roundRobinExchange(PlanNodeId id, Scope scope, PlanNode child, TypeProvider types)
    {
        return partitionedExchange(
                id,
                scope,
                child,
                new PartitioningScheme(Partitioning.create(FIXED_ARBITRARY_DISTRIBUTION, ImmutableList.of()), toVariableReferences(child.getOutputSymbols(), types)),
                types);
    }

    public static ExchangeNode mergingExchange(PlanNodeId id, Scope scope, PlanNode child, OrderingScheme orderingScheme, TypeProvider types)
    {
        PartitioningHandle partitioningHandle = scope.isLocal() ? FIXED_PASSTHROUGH_DISTRIBUTION : SINGLE_DISTRIBUTION;
        return new ExchangeNode(
                id,
                GATHER,
                scope,
                new PartitioningScheme(Partitioning.create(partitioningHandle, ImmutableList.of()), toVariableReferences(child.getOutputSymbols(), types)),
                ImmutableList.of(child),
                ImmutableList.of(toVariableReferences(child.getOutputSymbols(), types)),
                Optional.of(orderingScheme));
    }

    @JsonProperty
    public Type getType()
    {
        return type;
    }

    @JsonProperty
    public Scope getScope()
    {
        return scope;
    }

    @Override
    @JsonProperty
    public List<PlanNode> getSources()
    {
        return sources;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return partitioningScheme.getOutputLayout().stream().map(variable -> new Symbol(variable.getName())).collect(toImmutableList());
    }

    @Override
    public List<VariableReferenceExpression> getOutputVariables()
    {
        return partitioningScheme.getOutputLayout();
    }

    @JsonProperty
    public PartitioningScheme getPartitioningScheme()
    {
        return partitioningScheme;
    }

    @JsonProperty
    public Optional<OrderingScheme> getOrderingScheme()
    {
        return orderingScheme;
    }

    @JsonProperty
    public List<List<VariableReferenceExpression>> getInputs()
    {
        return inputs;
    }

    public List<List<Symbol>> getInputsAsSymbols()
    {
        return inputs.stream().map(symbols -> symbols.stream().map(variable -> new Symbol(variable.getName())).collect(toImmutableList())).collect(toImmutableList());
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitExchange(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new ExchangeNode(getId(), type, scope, partitioningScheme, newChildren, inputs, orderingScheme);
    }
}

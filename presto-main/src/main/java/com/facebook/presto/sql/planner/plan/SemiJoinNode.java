package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SemiJoinNode
        extends PlanNode
{
    private final PlanNode source;
    private final PlanNode filteringSource;
    private final Symbol sourceJoinSymbol;
    private final Symbol filteringSourceJoinSymbol;
    private final Symbol semiJoinOutput;

    @JsonCreator
    public SemiJoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("filteringSource") PlanNode filteringSource,
            @JsonProperty("sourceJoinSymbol") Symbol sourceJoinSymbol,
            @JsonProperty("filteringSourceJoinSymbol") Symbol filteringSourceJoinSymbol,
            @JsonProperty("semiJoinOutput") Symbol semiJoinOutput)
    {
        super(id);
        this.source = checkNotNull(source, "source is null");
        this.filteringSource = checkNotNull(filteringSource, "filteringSource is null");
        this.sourceJoinSymbol = checkNotNull(sourceJoinSymbol, "sourceJoinSymbol is null");
        this.filteringSourceJoinSymbol = checkNotNull(filteringSourceJoinSymbol, "filteringSourceJoinSymbol is null");
        this.semiJoinOutput = checkNotNull(semiJoinOutput, "semiJoinOutput is null");
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("filteringSource")
    public PlanNode getFilteringSource()
    {
        return filteringSource;
    }

    @JsonProperty("sourceJoinSymbol")
    public Symbol getSourceJoinSymbol()
    {
        return sourceJoinSymbol;
    }

    @JsonProperty("filteringSourceJoinSymbol")
    public Symbol getFilteringSourceJoinSymbol()
    {
        return filteringSourceJoinSymbol;
    }

    @JsonProperty("semiJoinOutput")
    public Symbol getSemiJoinOutput()
    {
        return semiJoinOutput;
    }
    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source, filteringSource);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
                .add(semiJoinOutput)
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSemiJoin(this, context);
    }
}

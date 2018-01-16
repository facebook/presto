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

import com.facebook.presto.matching.Explore;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.Property;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Streams;

import java.util.List;
import java.util.stream.Stream;

import static com.facebook.presto.matching.Explore.explore;
import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.matching.Property.property;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;

public class Patterns
{
    private Patterns() {}

    public static Pattern<AggregationNode> aggregation()
    {
        return typeOf(AggregationNode.class);
    }

    public static Pattern<ApplyNode> applyNode()
    {
        return typeOf(ApplyNode.class);
    }

    public static Pattern<DeleteNode> delete()
    {
        return typeOf(DeleteNode.class);
    }

    public static Pattern<ExchangeNode> exchange()
    {
        return typeOf(ExchangeNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<IndexSourceNode> indexSource()
    {
        return typeOf(IndexSourceNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<LateralJoinNode> lateralJoin()
    {
        return typeOf(LateralJoinNode.class);
    }

    public static Pattern<LimitNode> limit()
    {
        return typeOf(LimitNode.class);
    }

    public static Pattern<MarkDistinctNode> markDistinct()
    {
        return typeOf(MarkDistinctNode.class);
    }

    public static Pattern<OutputNode> output()
    {
        return typeOf(OutputNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<SampleNode> sample()
    {
        return typeOf(SampleNode.class);
    }

    public static Pattern<SemiJoinNode> semiJoin()
    {
        return typeOf(SemiJoinNode.class);
    }

    public static Pattern<SortNode> sort()
    {
        return typeOf(SortNode.class);
    }

    public static Pattern<TableFinishNode> tableFinish()
    {
        return typeOf(TableFinishNode.class);
    }

    public static Pattern<TableScanNode> tableScan()
    {
        return typeOf(TableScanNode.class);
    }

    public static Pattern<TableWriterNode> tableWriterNode()
    {
        return typeOf(TableWriterNode.class);
    }

    public static Pattern<TopNNode> topN()
    {
        return typeOf(TopNNode.class);
    }

    public static Pattern<UnionNode> union()
    {
        return typeOf(UnionNode.class);
    }

    public static Pattern<ValuesNode> values()
    {
        return typeOf(ValuesNode.class);
    }

    public static Pattern<WindowNode> window()
    {
        return typeOf(WindowNode.class);
    }

    public static Explore<PlanNode, Rule.Context, PlanNode> source()
    {
        return explore(
                "source",
                (node, context) -> {
                    if (node.getSources().size() != 1) {
                        return Stream.of();
                    }
                    return context.getLookup().resolveGroup(getOnlyElement(node.getSources()));
                });
    }

    public static Explore<PlanNode, Rule.Context, List<PlanNode>> sources()
    {
        return explore(
                "sources",
                (PlanNode node, Rule.Context context) -> {
                    List<Stream<PlanNode>> sourceStreams = node.getSources().stream()
                            .map(source -> context.getLookup().resolveGroup(source))
                            .collect(toImmutableList());

                    return Streams.stream(new AbstractIterator<List<PlanNode>>() {
                        @Override
                        protected List<PlanNode> computeNext()
                        {
                            List<PlanNode> sources = sourceStreams.stream()
                                    .map(source -> source.findFirst()
                                            .orElse(null)).collect(toImmutableList());
                            sourceStreams.stream().forEach(sourceStream -> sourceStream.skip(1));

                            if (sources.contains(null)) {
                                return endOfData();
                            }
                            return sources;
                        }
                    });
                });
    }

    public static class Aggregation
    {
        public static <C> Property<AggregationNode, C, List<Symbol>> groupingKeys()
        {
            return property("groupingKeys", AggregationNode::getGroupingKeys);
        }

        public static <C> Property<AggregationNode, C, AggregationNode.Step> step()
        {
            return property("step", AggregationNode::getStep);
        }
    }

    public static class Apply
    {
        public static <C> Property<ApplyNode, C, List<Symbol>> correlation()
        {
            return property("correlation", ApplyNode::getCorrelation);
        }
    }

    public static class LateralJoin
    {
        public static <C> Property<LateralJoinNode, C, List<Symbol>> correlation()
        {
            return property("correlation", LateralJoinNode::getCorrelation);
        }
    }

    public static class Limit
    {
        public static <C> Property<LimitNode, C, Long> count()
        {
            return property("count", LimitNode::getCount);
        }
    }

    public static class Sample
    {
        public static <C> Property<SampleNode, C, Double> sampleRatio()
        {
            return property("sampleRatio", SampleNode::getSampleRatio);
        }

        public static <C> Property<SampleNode, C, SampleNode.Type> sampleType()
        {
            return property("sampleType", SampleNode::getSampleType);
        }
    }

    public static class TopN
    {
        public static <C> Property<TopNNode, C, TopNNode.Step> step()
        {
            return property("step", TopNNode::getStep);
        }
    }

    public static class Values
    {
        public static <C> Property<ValuesNode, C, List<List<Expression>>> rows()
        {
            return property("rows", ValuesNode::getRows);
        }
    }
}

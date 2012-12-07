package com.facebook.presto.sql.planner;

import com.facebook.presto.metadata.Node;
import com.facebook.presto.metadata.NodeManager;
import com.facebook.presto.server.TableScanPlanFragmentSource;
import com.facebook.presto.split.Split;
import com.facebook.presto.split.SplitAssignments;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import javax.inject.Inject;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;

import static com.google.common.collect.Iterables.transform;

public class DistributedExecutionPlanner
{
    private final NodeManager nodeManager;
    private final SplitManager splitManager;
    private final Random random = new Random();

    @Inject
    public DistributedExecutionPlanner(NodeManager nodeManager, SplitManager splitManager)
    {
        this.nodeManager = nodeManager;
        this.splitManager = splitManager;
    }

    public StageExecutionPlan plan(SubPlan root)
    {
        PlanFragment currentFragment = root.getFragment();

        // get partitions for this fragment
        List<Partition> partitions;
        if (currentFragment.isPartitioned()) {
            // partitioned plan is based on an underlying table scan or distributed aggregation
            partitions = currentFragment.getRoot().accept(new Visitor(), null);
        }
        else {
            // create a single partition on a random node for this fragment
            ArrayList<Node> nodes = new ArrayList<>(nodeManager.getActiveNodes());
            Preconditions.checkState(!nodes.isEmpty(), "Cluster does not have any active nodes");
            Collections.shuffle(nodes, random);
            Node node = nodes.get(0);
            partitions = ImmutableList.of(new Partition(node, ImmutableList.<PlanFragmentSource>of()));
        }

        // create child stages
        ImmutableList.Builder<StageExecutionPlan> dependencies = ImmutableList.builder();
        for (SubPlan childPlan : root.getChildren()) {
            StageExecutionPlan dependency = plan(childPlan);
            dependencies.add(dependency);
        }

        return new StageExecutionPlan(currentFragment, partitions, dependencies.build());
    }

    private final class Visitor
        extends PlanVisitor<Void, List<Partition>>
    {
        @Override
        public List<Partition> visitTableScan(TableScanNode node, Void context)
        {
            // get splits for table
            Iterable<SplitAssignments> splitAssignments = splitManager.getSplitAssignments(node.getTable());

            // divide splits amongst the nodes
            Multimap<Node, Split> nodeSplits = SplitAssignments.randomNodeAssignment(random, splitAssignments);

            // create a partition for each node
            ImmutableList.Builder<Partition> partitions = ImmutableList.builder();
            for (Entry<Node, Collection<Split>> entry : nodeSplits.asMap().entrySet()) {
                List<PlanFragmentSource> sources = ImmutableList.copyOf(transform(entry.getValue(), new Function<Split, PlanFragmentSource>()
                {
                    @Override
                    public PlanFragmentSource apply(Split split)
                    {
                        return new TableScanPlanFragmentSource(split);
                    }
                }));
                partitions.add(new Partition(entry.getKey(), sources));
            }
            return partitions.build();
        }

        @Override
        public List<Partition> visitJoin(JoinNode node, Void context)
        {
            List<Partition> leftPartitions = node.getLeft().accept(this, context);
            List<Partition> rightPartitions = node.getRight().accept(this, context);
            if (!leftPartitions.isEmpty() && !rightPartitions.isEmpty()) {
                throw new IllegalArgumentException("Both left and right join nodes are partitioned"); // TODO: "partitioned" may not be the right term
            }
            if (!leftPartitions.isEmpty()) {
                return leftPartitions;
            }
            else {
                return rightPartitions;
            }
        }

        @Override
        public List<Partition> visitExchange(ExchangeNode node, Void context)
        {
            // exchange node is unpartitioned
            return ImmutableList.of();
        }

        @Override
        public List<Partition> visitAggregation(AggregationNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public List<Partition> visitFilter(FilterNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public List<Partition> visitProject(ProjectNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public List<Partition> visitTopN(TopNNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public List<Partition> visitOutput(OutputNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public List<Partition> visitLimit(LimitNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        public List<Partition> visitSort(SortNode node, Void context)
        {
            return node.getSource().accept(this, context);
        }

        @Override
        protected List<Partition> visitPlan(PlanNode node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node.getClass().getName());
        }
    }
}

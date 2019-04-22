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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.execution.QueryManagerConfig;
import com.facebook.presto.execution.scheduler.BucketNodeMap;
import com.facebook.presto.execution.warnings.WarningCollector;
import com.facebook.presto.metadata.InsertTableHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.NewTableLayout;
import com.facebook.presto.metadata.PartitioningMetadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayout;
import com.facebook.presto.metadata.TableLayout.TablePartitioning;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorNewTableLayout;
import com.facebook.presto.spi.Constraint;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.PrestoWarning;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.facebook.presto.spi.predicate.NullableValue;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Partitioning.ArgumentBinding;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.MetadataDeleteNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.RemoteSourceNode;
import com.facebook.presto.sql.planner.plan.RowNumberNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.StatisticsWriterNode;
import com.facebook.presto.sql.planner.plan.TableFinishNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode.InsertHandle;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.planner.sanity.PlanSanityChecker;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.SystemSessionProperties.getQueryMaxStageCount;
import static com.facebook.presto.SystemSessionProperties.isDynamicSchduleForGroupedExecution;
import static com.facebook.presto.SystemSessionProperties.isForceSingleNodeOutput;
import static com.facebook.presto.spi.StandardErrorCode.QUERY_HAS_TOO_MANY_STAGES;
import static com.facebook.presto.spi.StandardWarningCode.TOO_MANY_STAGES;
import static com.facebook.presto.spi.connector.NotPartitionedPartitionHandle.NOT_PARTITIONED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.sql.planner.SchedulingOrderVisitor.scheduleOrder;
import static com.facebook.presto.sql.planner.SymbolsExtractor.extractOutputSymbols;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.COORDINATOR_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_MATERIALIZED;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPLICATE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.gatheringExchange;
import static com.facebook.presto.sql.planner.planPrinter.PlanPrinter.jsonFragmentPlan;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.in;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Iterables.concat;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Maps.filterKeys;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

/**
 * Splits a logical plan into fragments that can be shipped and executed on distributed nodes
 */
public class PlanFragmenter
{
    private static final String TOO_MANY_STAGES_MESSAGE = "If the query contains multiple DISTINCTs, please set the 'use_mark_distinct' session property to false. " +
            "If the query contains multiple CTEs that are referenced more than once, please create temporary table(s) for one or more of the CTEs.";

    private final Metadata metadata;
    private final NodePartitioningManager nodePartitioningManager;
    private final QueryManagerConfig config;
    private final SqlParser sqlParser;

    @Inject
    public PlanFragmenter(Metadata metadata, NodePartitioningManager nodePartitioningManager, QueryManagerConfig queryManagerConfig, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
        this.config = requireNonNull(queryManagerConfig, "queryManagerConfig is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
    }

    public SubPlan createSubPlans(Session session, Plan plan, boolean forceSingleNode, PlanNodeIdAllocator idAllocator, WarningCollector warningCollector)
    {
        Fragmenter fragmenter = new Fragmenter(
                session,
                metadata,
                plan.getStatsAndCosts(),
                new PlanSanityChecker(forceSingleNode),
                warningCollector,
                sqlParser,
                idAllocator,
                new SymbolAllocator(plan.getTypes().allTypes()));

        FragmentProperties properties = new FragmentProperties(new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), plan.getRoot().getOutputSymbols()));
        if (forceSingleNode || isForceSingleNodeOutput(session)) {
            properties = properties.setSingleNodeDistribution();
        }
        PlanNode root = SimplePlanRewriter.rewriteWith(fragmenter, plan.getRoot(), properties);

        SubPlan subPlan = fragmenter.buildRootFragment(root, properties);
        subPlan = reassignPartitioningHandleIfNecessary(session, subPlan);
        subPlan = analyzeGroupedExecution(session, subPlan);

        checkState(!isForceSingleNodeOutput(session) || subPlan.getFragment().getPartitioning().isSingleNode(), "Root of PlanFragment is not single node");

        // TODO: Remove query_max_stage_count session property and use queryManagerConfig.getMaxStageCount() here
        sanityCheckFragmentedPlan(subPlan, warningCollector, getQueryMaxStageCount(session), config.getStageCountWarningThreshold());

        return subPlan;
    }

    private void sanityCheckFragmentedPlan(SubPlan subPlan, WarningCollector warningCollector, int maxStageCount, int stageCountSoftLimit)
    {
        subPlan.sanityCheck();
        int fragmentCount = subPlan.getAllFragments().size();
        if (fragmentCount > maxStageCount) {
            throw new PrestoException(QUERY_HAS_TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the allowed maximum (%s). " + TOO_MANY_STAGES_MESSAGE,
                    fragmentCount, maxStageCount));
        }
        if (fragmentCount > stageCountSoftLimit) {
            warningCollector.add(new PrestoWarning(TOO_MANY_STAGES, format(
                    "Number of stages in the query (%s) exceeds the soft limit (%s). " + TOO_MANY_STAGES_MESSAGE,
                    fragmentCount, stageCountSoftLimit)));
        }
    }

    private SubPlan analyzeGroupedExecution(Session session, SubPlan subPlan)
    {
        PlanFragment fragment = subPlan.getFragment();
        GroupedExecutionProperties properties = fragment.getRoot().accept(new GroupedExecutionTagger(session, metadata, nodePartitioningManager), null);
        if (properties.isSubTreeUseful()) {
            boolean preferDynamic = fragment.getRemoteSourceNodes().stream().allMatch(node -> node.getExchangeType() == REPLICATE)
                    && isDynamicSchduleForGroupedExecution(session);
            BucketNodeMap bucketNodeMap = nodePartitioningManager.getBucketNodeMap(session, fragment.getPartitioning(), preferDynamic);
            if (bucketNodeMap.isDynamic()) {
                fragment = fragment.withDynamicLifespanScheduleGroupedExecution(properties.getCapableTableScanNodes());
            }
            else {
                fragment = fragment.withFixedLifespanScheduleGroupedExecution(properties.getCapableTableScanNodes());
            }
        }
        ImmutableList.Builder<SubPlan> result = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            result.add(analyzeGroupedExecution(session, child));
        }
        return new SubPlan(fragment, result.build());
    }

    private SubPlan reassignPartitioningHandleIfNecessary(Session session, SubPlan subPlan)
    {
        return reassignPartitioningHandleIfNecessaryHelper(session, subPlan, subPlan.getFragment().getPartitioning());
    }

    private SubPlan reassignPartitioningHandleIfNecessaryHelper(Session session, SubPlan subPlan, PartitioningHandle newOutputPartitioningHandle)
    {
        PlanFragment fragment = subPlan.getFragment();

        PlanNode newRoot = fragment.getRoot();
        // If the fragment's partitioning is SINGLE or COORDINATOR_ONLY, leave the sources as is (this is for single-node execution)
        if (!fragment.getPartitioning().isSingleNode()) {
            PartitioningHandleReassigner partitioningHandleReassigner = new PartitioningHandleReassigner(fragment.getPartitioning(), metadata, session);
            newRoot = SimplePlanRewriter.rewriteWith(partitioningHandleReassigner, newRoot);
        }
        PartitioningScheme outputPartitioningScheme = fragment.getPartitioningScheme();
        Partitioning newOutputPartitioning = outputPartitioningScheme.getPartitioning();
        if (outputPartitioningScheme.getPartitioning().getHandle().getConnectorId().isPresent()) {
            // Do not replace the handle if the source's output handle is a system one, e.g. broadcast.
            newOutputPartitioning = newOutputPartitioning.withAlternativePartitiongingHandle(newOutputPartitioningHandle);
        }
        PlanFragment newFragment = new PlanFragment(
                fragment.getId(),
                newRoot,
                fragment.getSymbols(),
                fragment.getPartitioning(),
                fragment.getPartitionedSources(),
                new PartitioningScheme(
                        newOutputPartitioning,
                        outputPartitioningScheme.getOutputLayout(),
                        outputPartitioningScheme.getHashColumn(),
                        outputPartitioningScheme.isReplicateNullsAndAny(),
                        outputPartitioningScheme.getBucketToPartition()),
                fragment.getStageExecutionDescriptor(),
                fragment.getStatsAndCosts(),
                fragment.getJsonRepresentation());

        ImmutableList.Builder<SubPlan> childrenBuilder = ImmutableList.builder();
        for (SubPlan child : subPlan.getChildren()) {
            childrenBuilder.add(reassignPartitioningHandleIfNecessaryHelper(session, child, fragment.getPartitioning()));
        }
        return new SubPlan(newFragment, childrenBuilder.build());
    }

    private static class Fragmenter
            extends SimplePlanRewriter<FragmentProperties>
    {
        private static final int ROOT_FRAGMENT_ID = 0;

        private final Session session;
        private final Metadata metadata;
        private final PlanNodeIdAllocator idAllocator;
        private final SymbolAllocator symbolAllocator;
        private final StatsAndCosts statsAndCosts;
        private final PlanSanityChecker planSanityChecker;
        private final WarningCollector warningCollector;
        private final SqlParser sqlParser;
        private final LiteralEncoder literalEncoder;
        private int nextFragmentId = ROOT_FRAGMENT_ID + 1;

        public Fragmenter(
                Session session,
                Metadata metadata,
                StatsAndCosts statsAndCosts,
                PlanSanityChecker planSanityChecker,
                WarningCollector warningCollector,
                SqlParser sqlParser,
                PlanNodeIdAllocator idAllocator,
                SymbolAllocator symbolAllocator)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.statsAndCosts = requireNonNull(statsAndCosts, "statsAndCosts is null");
            this.planSanityChecker = requireNonNull(planSanityChecker, "planSanityChecker is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
            this.sqlParser = requireNonNull(sqlParser, "sqlParser is null");
            this.idAllocator = requireNonNull(idAllocator, "idAllocator is null");
            this.symbolAllocator = requireNonNull(symbolAllocator, "symbolAllocator is null");
            this.literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());
        }

        public SubPlan buildRootFragment(PlanNode root, FragmentProperties properties)
        {
            return buildFragment(root, properties, new PlanFragmentId(ROOT_FRAGMENT_ID));
        }

        private PlanFragmentId nextFragmentId()
        {
            return new PlanFragmentId(nextFragmentId++);
        }

        private SubPlan buildFragment(PlanNode root, FragmentProperties properties, PlanFragmentId fragmentId)
        {
            List<PlanNodeId> schedulingOrder = scheduleOrder(root);
            checkArgument(
                    properties.getPartitionedSources().equals(ImmutableSet.copyOf(schedulingOrder)),
                    "Expected scheduling order (%s) to contain an entry for all partitioned sources (%s)",
                    schedulingOrder,
                    properties.getPartitionedSources());

            Map<Symbol, Type> fragmentSymbolTypes = filterKeys(symbolAllocator.getTypes().allTypes(), in(extractOutputSymbols(root)));
            planSanityChecker.validatePlanFragment(root, session, metadata, sqlParser, TypeProvider.viewOf(fragmentSymbolTypes), warningCollector);
            PlanFragment fragment = new PlanFragment(
                    fragmentId,
                    root,
                    fragmentSymbolTypes,
                    properties.getPartitioningHandle(),
                    schedulingOrder,
                    properties.getPartitioningScheme(),
                    StageExecutionDescriptor.ungroupedExecution(),
                    statsAndCosts.getForSubplan(root),
                    Optional.of(jsonFragmentPlan(root, fragmentSymbolTypes, metadata.getFunctionManager(), session)));

            return new SubPlan(fragment, properties.getChildren());
        }

        @Override
        public PlanNode visitOutput(OutputNode node, RewriteContext<FragmentProperties> context)
        {
            if (isForceSingleNodeOutput(session)) {
                context.get().setSingleNodeDistribution();
            }

            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExplainAnalyze(ExplainAnalyzeNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitStatisticsWriterNode(StatisticsWriterNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableFinish(TableFinishNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitMetadataDelete(MetadataDeleteNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setCoordinatorOnlyDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<FragmentProperties> context)
        {
            PartitioningHandle partitioning = node.getLayout()
                    .map(layout -> metadata.getLayout(session, layout))
                    .flatMap(TableLayout::getTablePartitioning)
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);

            context.get().addSourceDistribution(node.getId(), partitioning, metadata, session);
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitTableWriter(TableWriterNode node, RewriteContext<FragmentProperties> context)
        {
            if (node.getPartitioningScheme().isPresent()) {
                context.get().setDistribution(node.getPartitioningScheme().get().getPartitioning().getHandle(), metadata, session);
            }
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitValues(ValuesNode node, RewriteContext<FragmentProperties> context)
        {
            context.get().setSingleNodeDistribution();
            return context.defaultRewrite(node, context.get());
        }

        @Override
        public PlanNode visitExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            switch (exchange.getScope()) {
                case LOCAL:
                    return context.defaultRewrite(exchange, context.get());
                case REMOTE_STREAMING:
                    return createRemoteStreamingExchange(exchange, context);
                case REMOTE_MATERIALIZED:
                    return createRemoteMaterializedExchange(exchange, context);
                default:
                    throw new IllegalArgumentException("Unexpected exchange scope: " + exchange.getScope());
            }
        }

        private PlanNode createRemoteStreamingExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            checkArgument(exchange.getScope() == REMOTE_STREAMING, "Unexpected exchange scope: %s", exchange.getScope());

            PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();

            if (exchange.getType() == ExchangeNode.Type.GATHER) {
                context.get().setSingleNodeDistribution();
            }
            else if (exchange.getType() == ExchangeNode.Type.REPARTITION) {
                context.get().setDistribution(partitioningScheme.getPartitioning().getHandle(), metadata, session);
            }

            ImmutableList.Builder<SubPlan> builder = ImmutableList.builder();
            for (int sourceIndex = 0; sourceIndex < exchange.getSources().size(); sourceIndex++) {
                FragmentProperties childProperties = new FragmentProperties(partitioningScheme.translateOutputLayout(exchange.getInputs().get(sourceIndex)));
                builder.add(buildSubPlan(exchange.getSources().get(sourceIndex), childProperties, context));
            }

            List<SubPlan> children = builder.build();
            context.get().addChildren(children);

            List<PlanFragmentId> childrenIds = children.stream()
                    .map(SubPlan::getFragment)
                    .map(PlanFragment::getId)
                    .collect(toImmutableList());

            return new RemoteSourceNode(exchange.getId(), childrenIds, exchange.getOutputSymbols(), exchange.getOrderingScheme(), exchange.getType());
        }

        private PlanNode createRemoteMaterializedExchange(ExchangeNode exchange, RewriteContext<FragmentProperties> context)
        {
            checkArgument(exchange.getType() == REPARTITION, "Unexpected exchange type: %s", exchange.getType());
            checkArgument(exchange.getScope() == REMOTE_MATERIALIZED, "Unexpected exchange scope: %s", exchange.getScope());

            PartitioningScheme partitioningScheme = exchange.getPartitioningScheme();
            checkArgument(!partitioningScheme.getHashColumn().isPresent(), "precomputed hashes are not supported in materializing exchanges");

            PartitioningHandle partitioningHandle = partitioningScheme.getPartitioning().getHandle();
            ConnectorId connectorId = partitioningHandle.getConnectorId()
                    .orElseThrow(() -> new IllegalArgumentException("Unsupported partitioning handle: " + partitioningHandle));

            Partitioning partitioning = partitioningScheme.getPartitioning();
            PartitioningSymbolAssignments partitioningSymbolAssignments = assignPartitioningSymbols(partitioning);
            Map<Symbol, ColumnMetadata> symbolToColumnMap = assignTemporaryTableColumnNames(exchange.getOutputSymbols(), partitioningSymbolAssignments.getConstants().keySet());
            List<Symbol> partitioningSymbols = partitioningSymbolAssignments.getSymbols();
            List<String> partitionColumns = partitioningSymbols.stream()
                    .map(symbol -> symbolToColumnMap.get(symbol).getName())
                    .collect(toImmutableList());
            PartitioningMetadata partitioningMetadata = new PartitioningMetadata(partitioningHandle, partitionColumns);

            TableHandle temporaryTableHandle = metadata.createTemporaryTable(
                    session,
                    connectorId.getCatalogName(),
                    ImmutableList.copyOf(symbolToColumnMap.values()),
                    Optional.of(partitioningMetadata));

            TableScanNode scan = createTemporaryTableScan(
                    temporaryTableHandle,
                    exchange.getOutputSymbols(),
                    symbolToColumnMap,
                    partitioningMetadata);

            checkArgument(
                    !exchange.getPartitioningScheme().isReplicateNullsAndAny(),
                    "materialized remote exchange is not supported when replicateNullsAndAny is needed");
            TableFinishNode write = createTemporaryTableWrite(
                    temporaryTableHandle,
                    symbolToColumnMap,
                    exchange.getOutputSymbols(),
                    exchange.getInputs(),
                    exchange.getSources(),
                    partitioningSymbolAssignments.getConstants(),
                    partitioningMetadata);

            FragmentProperties writeProperties = new FragmentProperties(new PartitioningScheme(
                    Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()),
                    write.getOutputSymbols()));
            writeProperties.setCoordinatorOnlyDistribution();

            List<SubPlan> children = ImmutableList.of(buildSubPlan(write, writeProperties, context));
            context.get().addChildren(children);

            return visitTableScan(scan, context);
        }

        private PartitioningSymbolAssignments assignPartitioningSymbols(Partitioning partitioning)
        {
            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            ImmutableMap.Builder<Symbol, RowExpression> constants = ImmutableMap.builder();
            for (ArgumentBinding argumentBinding : partitioning.getArguments()) {
                Symbol symbol;
                if (argumentBinding.isConstant()) {
                    NullableValue constant = argumentBinding.getConstant();
                    RowExpression expression = constant(constant.getValue(), constant.getType());
                    symbol = symbolAllocator.newSymbol("constant_partition", constant.getType());
                    constants.put(symbol, expression);
                }
                else {
                    symbol = argumentBinding.getColumn();
                }
                symbols.add(symbol);
            }
            return new PartitioningSymbolAssignments(symbols.build(), constants.build());
        }

        private Map<Symbol, ColumnMetadata> assignTemporaryTableColumnNames(Collection<Symbol> outputSymbols, Collection<Symbol> constantPartitioningSymbols)
        {
            ImmutableMap.Builder<Symbol, ColumnMetadata> result = ImmutableMap.builder();
            int column = 0;
            for (Symbol outputSymbol : concat(outputSymbols, constantPartitioningSymbols)) {
                String columnName = format("_c%d_%s", column, outputSymbol.getName());
                result.put(outputSymbol, new ColumnMetadata(columnName, symbolAllocator.getTypes().get(outputSymbol)));
                column++;
            }
            return result.build();
        }

        private TableScanNode createTemporaryTableScan(
                TableHandle tableHandle,
                List<Symbol> outputSymbols,
                Map<Symbol, ColumnMetadata> symbolToColumnMap,
                PartitioningMetadata expectedPartitioningMetadata)
        {
            Map<String, ColumnHandle> columnHandles = metadata.getColumnHandles(session, tableHandle);
            Map<Symbol, ColumnMetadata> outputColumns = outputSymbols.stream()
                    .collect(toImmutableMap(identity(), symbolToColumnMap::get));
            Set<ColumnHandle> outputColumnHandles = outputColumns.values().stream()
                    .map(ColumnMetadata::getName)
                    .map(columnHandles::get)
                    .collect(toImmutableSet());

            List<TableLayoutResult> layouts = metadata.getLayouts(session, tableHandle, Constraint.alwaysTrue(), Optional.of(outputColumnHandles));
            checkArgument(layouts.size() == 1, "temporary table is expected to have exactly one layout");
            TableLayoutResult selectedLayout = getOnlyElement(layouts);
            verify(selectedLayout.getUnenforcedConstraint().equals(TupleDomain.all()), "temporary table layout shouldn't enforce any constraints");
            verify(!selectedLayout.getLayout().getColumns().isPresent(), "temporary table layout must provide all the columns");
            TablePartitioning expectedPartitioning = new TablePartitioning(
                    expectedPartitioningMetadata.getPartitioningHandle(),
                    expectedPartitioningMetadata.getPartitionColumns().stream()
                            .map(columnHandles::get)
                            .collect(toImmutableList()));
            verify(selectedLayout.getLayout().getTablePartitioning().equals(Optional.of(expectedPartitioning)), "invalid temporary table partitioning");

            TableLayoutHandle layoutHandle = selectedLayout.getLayout().getHandle();
            Map<Symbol, ColumnHandle> assignments = outputSymbols.stream()
                    .collect(toImmutableMap(identity(), symbol -> columnHandles.get(outputColumns.get(symbol).getName())));

            return new TableScanNode(
                    idAllocator.getNextId(),
                    tableHandle,
                    outputSymbols,
                    assignments,
                    Optional.of(layoutHandle),
                    TupleDomain.all(),
                    TupleDomain.all(),
                    true);
        }

        private TableFinishNode createTemporaryTableWrite(
                TableHandle tableHandle,
                Map<Symbol, ColumnMetadata> symbolToColumnMap,
                List<Symbol> outputs,
                List<List<Symbol>> inputs,
                List<PlanNode> sources,
                Map<Symbol, RowExpression> constantExpressions,
                PartitioningMetadata partitioningMetadata)
        {
            if (!constantExpressions.isEmpty()) {
                List<Symbol> constantSymbols = ImmutableList.copyOf(constantExpressions.keySet());

                // update outputs
                outputs = ImmutableList.<Symbol>builder()
                        .addAll(outputs)
                        .addAll(constantSymbols)
                        .build();

                // update inputs
                inputs = inputs.stream()
                        .map(input -> ImmutableList.<Symbol>builder()
                                .addAll(input)
                                .addAll(constantSymbols)
                                .build())
                        .collect(toImmutableList());

                // update sources
                sources = sources.stream()
                        .map(source -> {
                            AssignmentsUtils.Builder assignments = AssignmentsUtils.builder();
                            source.getOutputSymbols().forEach(symbol -> assignments.put(symbol, new VariableReferenceExpression(symbol.getName(), symbolAllocator.getTypes().get(symbol))));
                            constantSymbols.forEach(symbol -> assignments.put(symbol, constantExpressions.get(symbol)));
                            return new ProjectNode(idAllocator.getNextId(), source, assignments.build());
                        })
                        .collect(toImmutableList());
            }

            NewTableLayout insertLayout = metadata.getInsertLayout(session, tableHandle)
                    // TODO: support insert into non partitioned table
                    .orElseThrow(() -> new IllegalArgumentException("insertLayout for the temporary table must be present"));

            PartitioningHandle partitioningHandle = partitioningMetadata.getPartitioningHandle();
            List<String> partitionColumns = partitioningMetadata.getPartitionColumns();
            ConnectorNewTableLayout expectedNewTableLayout = new ConnectorNewTableLayout(partitioningHandle.getConnectorHandle(), partitionColumns);
            verify(insertLayout.getLayout().equals(expectedNewTableLayout), "unexpected new table layout");

            Map<String, Symbol> columnNameToSymbol = symbolToColumnMap.entrySet().stream()
                    .collect(toImmutableMap(entry -> entry.getValue().getName(), Map.Entry::getKey));
            List<Symbol> partitioningSymbols = partitionColumns.stream()
                    .map(columnNameToSymbol::get)
                    .collect(toImmutableList());

            InsertTableHandle insertTableHandle = metadata.beginInsert(session, tableHandle);
            List<String> outputColumnNames = outputs.stream()
                    .map(symbolToColumnMap::get)
                    .map(ColumnMetadata::getName)
                    .collect(toImmutableList());

            SchemaTableName temporaryTableName = metadata.getTableMetadata(session, tableHandle).getTable();
            InsertHandle insertHandle = new InsertHandle(insertTableHandle, new SchemaTableName(temporaryTableName.getSchemaName(), temporaryTableName.getTableName()));

            return new TableFinishNode(
                    idAllocator.getNextId(),
                    gatheringExchange(
                            idAllocator.getNextId(),
                            LOCAL,
                            gatheringExchange(
                                    idAllocator.getNextId(),
                                    REMOTE_STREAMING,
                                    new TableWriterNode(
                                            idAllocator.getNextId(),
                                            gatheringExchange(
                                                    idAllocator.getNextId(),
                                                    LOCAL,
                                                    new ExchangeNode(
                                                            idAllocator.getNextId(),
                                                            REPARTITION,
                                                            REMOTE_STREAMING,
                                                            new PartitioningScheme(
                                                                    Partitioning.create(partitioningHandle, partitioningSymbols),
                                                                    outputs,
                                                                    Optional.empty(),
                                                                    false,
                                                                    Optional.empty()),
                                                            sources,
                                                            inputs,
                                                            Optional.empty())),
                                            insertHandle,
                                            symbolAllocator.newSymbol("partialrows", BIGINT),
                                            symbolAllocator.newSymbol("fragment", VARBINARY),
                                            outputs,
                                            outputColumnNames,
                                            Optional.of(new PartitioningScheme(
                                                    Partitioning.create(partitioningHandle, partitioningSymbols),
                                                    outputs,
                                                    Optional.empty(),
                                                    false,
                                                    Optional.empty())),
                                            Optional.empty(),
                                            Optional.empty()))),
                    insertHandle,
                    symbolAllocator.newSymbol("rows", BIGINT),
                    Optional.empty(),
                    Optional.empty());
        }

        private SubPlan buildSubPlan(PlanNode node, FragmentProperties properties, RewriteContext<FragmentProperties> context)
        {
            PlanFragmentId planFragmentId = nextFragmentId();
            PlanNode child = context.rewrite(node, properties);
            return buildFragment(child, properties, planFragmentId);
        }
    }

    private static class FragmentProperties
    {
        private final List<SubPlan> children = new ArrayList<>();

        private final PartitioningScheme partitioningScheme;

        private Optional<PartitioningHandle> partitioningHandle = Optional.empty();
        private final Set<PlanNodeId> partitionedSources = new HashSet<>();

        public FragmentProperties(PartitioningScheme partitioningScheme)
        {
            this.partitioningScheme = partitioningScheme;
        }

        public List<SubPlan> getChildren()
        {
            return children;
        }

        public FragmentProperties setSingleNodeDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isSingleNode()) {
                // already single node distribution
                return this;
            }

            checkState(!partitioningHandle.isPresent(),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    SINGLE_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(SINGLE_DISTRIBUTION);

            return this;
        }

        public FragmentProperties setDistribution(PartitioningHandle distribution, Metadata metadata, Session session)
        {
            if (!partitioningHandle.isPresent()) {
                partitioningHandle = Optional.of(distribution);
                return this;
            }

            PartitioningHandle currentPartitioning = this.partitioningHandle.get();

            if (isCompatibleSystemPartitioning(distribution)) {
                return this;
            }

            if (currentPartitioning.equals(SOURCE_DISTRIBUTION)) {
                this.partitioningHandle = Optional.of(distribution);
                return this;
            }

            // If already system SINGLE or COORDINATOR_ONLY, leave it as is (this is for single-node execution)
            if (currentPartitioning.isSingleNode()) {
                return this;
            }

            if (currentPartitioning.equals(distribution)) {
                return this;
            }

            Optional<PartitioningHandle> commonPartitioning = metadata.getCommonPartitioning(session, currentPartitioning, distribution);
            if (commonPartitioning.isPresent()) {
                partitioningHandle = commonPartitioning;
                return this;
            }

            throw new IllegalStateException(format(
                    "Cannot set distribution to %s. Already set to %s",
                    distribution,
                    this.partitioningHandle));
        }

        private boolean isCompatibleSystemPartitioning(PartitioningHandle distribution)
        {
            ConnectorPartitioningHandle currentHandle = partitioningHandle.get().getConnectorHandle();
            ConnectorPartitioningHandle distributionHandle = distribution.getConnectorHandle();
            if ((currentHandle instanceof SystemPartitioningHandle) &&
                    (distributionHandle instanceof SystemPartitioningHandle)) {
                return ((SystemPartitioningHandle) currentHandle).getPartitioning() ==
                        ((SystemPartitioningHandle) distributionHandle).getPartitioning();
            }
            return false;
        }

        public FragmentProperties setCoordinatorOnlyDistribution()
        {
            if (partitioningHandle.isPresent() && partitioningHandle.get().isCoordinatorOnly()) {
                // already single node distribution
                return this;
            }

            // only system SINGLE can be upgraded to COORDINATOR_ONLY
            checkState(!partitioningHandle.isPresent() || partitioningHandle.get().equals(SINGLE_DISTRIBUTION),
                    "Cannot overwrite partitioning with %s (currently set to %s)",
                    COORDINATOR_DISTRIBUTION,
                    partitioningHandle);

            partitioningHandle = Optional.of(COORDINATOR_DISTRIBUTION);

            return this;
        }

        public FragmentProperties addSourceDistribution(PlanNodeId source, PartitioningHandle distribution, Metadata metadata, Session session)
        {
            requireNonNull(source, "source is null");
            requireNonNull(distribution, "distribution is null");

            partitionedSources.add(source);
            return setDistribution(distribution, metadata, session);
        }

        public FragmentProperties addChildren(List<SubPlan> children)
        {
            this.children.addAll(children);

            return this;
        }

        public PartitioningScheme getPartitioningScheme()
        {
            return partitioningScheme;
        }

        public PartitioningHandle getPartitioningHandle()
        {
            return partitioningHandle.get();
        }

        public Set<PlanNodeId> getPartitionedSources()
        {
            return partitionedSources;
        }
    }

    private static class GroupedExecutionTagger
            extends PlanVisitor<GroupedExecutionProperties, Void>
    {
        private final Session session;
        private final Metadata metadata;
        private final NodePartitioningManager nodePartitioningManager;
        private final boolean groupedExecutionForAggregation;

        public GroupedExecutionTagger(Session session, Metadata metadata, NodePartitioningManager nodePartitioningManager)
        {
            this.session = requireNonNull(session, "session is null");
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.nodePartitioningManager = requireNonNull(nodePartitioningManager, "nodePartitioningManager is null");
            this.groupedExecutionForAggregation = SystemSessionProperties.isGroupedExecutionForAggregationEnabled(session);
        }

        @Override
        protected GroupedExecutionProperties visitPlan(PlanNode node, Void context)
        {
            if (node.getSources().isEmpty()) {
                return GroupedExecutionProperties.notCapable();
            }
            return processChildren(node);
        }

        @Override
        public GroupedExecutionProperties visitJoin(JoinNode node, Void context)
        {
            GroupedExecutionProperties left = node.getLeft().accept(this, null);
            GroupedExecutionProperties right = node.getRight().accept(this, null);

            if (!node.getDistributionType().isPresent()) {
                // This is possible when the optimizers is invoked with `forceSingleNode` set to true.
                return GroupedExecutionProperties.notCapable();
            }

            if ((node.getType() == JoinNode.Type.RIGHT || node.getType() == JoinNode.Type.FULL) && !right.currentNodeCapable) {
                // For a plan like this, if the fragment participates in grouped execution,
                // the LookupOuterOperator corresponding to the RJoin will not work execute properly.
                //
                // * The operator has to execute as not-grouped because it can only look at the "used" flags in
                //   join build after all probe has finished.
                // * The operator has to execute as grouped the subsequent LJoin expects that incoming
                //   operators are grouped. Otherwise, the LJoin won't be able to throw out the build side
                //   for each group as soon as the group completes.
                //
                //       LJoin
                //       /   \
                //   RJoin   Scan
                //   /   \
                // Scan Remote
                //
                // TODO:
                // The RJoin can still execute as grouped if there is no subsequent operator that depends
                // on the RJoin being executed in a grouped manner. However, this is not currently implemented.
                // Support for this scenario is already implemented in the execution side.
                return GroupedExecutionProperties.notCapable();
            }

            switch (node.getDistributionType().get()) {
                case REPLICATED:
                    // Broadcast join maintains partitioning for the left side.
                    // Right side of a broadcast is not capable of grouped execution because it always comes from a remote exchange.
                    checkState(!right.currentNodeCapable);
                    return left;
                case PARTITIONED:
                    if (left.currentNodeCapable && right.currentNodeCapable) {
                        return new GroupedExecutionProperties(
                                true,
                                true,
                                ImmutableList.<PlanNodeId>builder()
                                        .addAll(left.capableTableScanNodes)
                                        .addAll(right.capableTableScanNodes)
                                        .build());
                    }
                    // right.subTreeUseful && !left.currentNodeCapable:
                    //   It's not particularly helpful to do grouped execution on the right side
                    //   because the benefit is likely cancelled out due to required buffering for hash build.
                    //   In theory, it could still be helpful (e.g. when the underlying aggregation's intermediate group state maybe larger than aggregation output).
                    //   However, this is not currently implemented. JoinBridgeManager need to support such a lifecycle.
                    // !right.currentNodeCapable:
                    //   The build/right side needs to buffer fully for this JOIN, but the probe/left side will still stream through.
                    //   As a result, there is no reason to change currentNodeCapable or subTreeUseful to false.
                    //
                    return left;
                default:
                    throw new UnsupportedOperationException("Unknown distribution type: " + node.getDistributionType());
            }
        }

        @Override
        public GroupedExecutionProperties visitAggregation(AggregationNode node, Void context)
        {
            GroupedExecutionProperties properties = node.getSource().accept(this, null);
            if (groupedExecutionForAggregation && properties.isCurrentNodeCapable()) {
                switch (node.getStep()) {
                    case SINGLE:
                    case FINAL:
                        return new GroupedExecutionProperties(true, true, properties.capableTableScanNodes);
                    case PARTIAL:
                    case INTERMEDIATE:
                        return properties;
                }
            }
            return GroupedExecutionProperties.notCapable();
        }

        @Override
        public GroupedExecutionProperties visitWindow(WindowNode node, Void context)
        {
            return processWindowFunction(node);
        }

        @Override
        public GroupedExecutionProperties visitRowNumber(RowNumberNode node, Void context)
        {
            return processWindowFunction(node);
        }

        @Override
        public GroupedExecutionProperties visitTopNRowNumber(TopNRowNumberNode node, Void context)
        {
            return processWindowFunction(node);
        }

        private GroupedExecutionProperties processWindowFunction(PlanNode node)
        {
            GroupedExecutionProperties properties = getOnlyElement(node.getSources()).accept(this, null);
            if (groupedExecutionForAggregation && properties.isCurrentNodeCapable()) {
                return new GroupedExecutionProperties(true, true, properties.capableTableScanNodes);
            }
            return GroupedExecutionProperties.notCapable();
        }

        @Override
        public GroupedExecutionProperties visitTableScan(TableScanNode node, Void context)
        {
            Optional<TablePartitioning> tablePartitioning = metadata.getLayout(session, node.getLayout().get()).getTablePartitioning();
            if (!tablePartitioning.isPresent()) {
                return GroupedExecutionProperties.notCapable();
            }
            List<ConnectorPartitionHandle> partitionHandles = nodePartitioningManager.listPartitionHandles(session, tablePartitioning.get().getPartitioningHandle());
            if (ImmutableList.of(NOT_PARTITIONED).equals(partitionHandles)) {
                return new GroupedExecutionProperties(false, false, ImmutableList.of());
            }
            else {
                return new GroupedExecutionProperties(true, false, ImmutableList.of(node.getId()));
            }
        }

        private GroupedExecutionProperties processChildren(PlanNode node)
        {
            // Each fragment has a partitioning handle, which is derived from leaf nodes in the fragment.
            // Leaf nodes with different partitioning handle are not allowed to share a single fragment
            // (except for special cases as detailed in addSourceDistribution).
            // As a result, it is not necessary to check the compatibility between node.getSources because
            // they are guaranteed to be compatible.

            // * If any child is "not capable", return "not capable"
            // * When all children are capable ("capable and useful" or "capable but not useful")
            //   * if any child is "capable and useful", return "capable and useful"
            //   * if no children is "capable and useful", return "capable but not useful"
            boolean anyUseful = false;
            ImmutableList.Builder<PlanNodeId> capableTableScanNodes = ImmutableList.builder();
            for (PlanNode source : node.getSources()) {
                GroupedExecutionProperties properties = source.accept(this, null);
                if (!properties.isCurrentNodeCapable()) {
                    return GroupedExecutionProperties.notCapable();
                }
                anyUseful |= properties.isSubTreeUseful();
                capableTableScanNodes.addAll(properties.capableTableScanNodes);
            }
            return new GroupedExecutionProperties(true, anyUseful, capableTableScanNodes.build());
        }
    }

    private static class GroupedExecutionProperties
    {
        // currentNodeCapable:
        //   Whether grouped execution is possible with the current node.
        //   For example, a table scan is capable iff it supports addressable split discovery.
        // subTreeUseful:
        //   Whether grouped execution is beneficial in the current node, or any node below it.
        //   For example, a JOIN can benefit from grouped execution because build can be flushed early, reducing peak memory requirement.
        //
        // In the current implementation, subTreeUseful implies currentNodeCapable.
        // In theory, this doesn't have to be the case. Take an example where a GROUP BY feeds into the build side of a JOIN.
        // Even if JOIN cannot take advantage of grouped execution, it could still be beneficial to execute the GROUP BY with grouped execution
        // (e.g. when the underlying aggregation's intermediate group state may be larger than aggregation output).

        private final boolean currentNodeCapable;
        private final boolean subTreeUseful;
        private final List<PlanNodeId> capableTableScanNodes;

        public GroupedExecutionProperties(boolean currentNodeCapable, boolean subTreeUseful, List<PlanNodeId> capableTableScanNodes)
        {
            this.currentNodeCapable = currentNodeCapable;
            this.subTreeUseful = subTreeUseful;
            this.capableTableScanNodes = ImmutableList.copyOf(requireNonNull(capableTableScanNodes, "capableTableScanNodes is null"));
            // Verify that `subTreeUseful` implies `currentNodeCapable`
            checkArgument(!subTreeUseful || currentNodeCapable);
            checkArgument(currentNodeCapable == !capableTableScanNodes.isEmpty());
        }

        public static GroupedExecutionProperties notCapable()
        {
            return new GroupedExecutionProperties(false, false, ImmutableList.of());
        }

        public boolean isCurrentNodeCapable()
        {
            return currentNodeCapable;
        }

        public boolean isSubTreeUseful()
        {
            return subTreeUseful;
        }

        public List<PlanNodeId> getCapableTableScanNodes()
        {
            return capableTableScanNodes;
        }
    }

    private static final class PartitioningHandleReassigner
            extends SimplePlanRewriter<Void>
    {
        private final PartitioningHandle fragmentPartitioningHandle;
        private final Metadata metadata;
        private final Session session;

        public PartitioningHandleReassigner(PartitioningHandle fragmentPartitioningHandle, Metadata metadata, Session session)
        {
            this.fragmentPartitioningHandle = fragmentPartitioningHandle;
            this.metadata = metadata;
            this.session = session;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Void> context)
        {
            PartitioningHandle partitioning = node.getLayout()
                    .map(layout -> metadata.getLayout(session, layout))
                    .flatMap(TableLayout::getTablePartitioning)
                    .map(TablePartitioning::getPartitioningHandle)
                    .orElse(SOURCE_DISTRIBUTION);
            if (partitioning.equals(fragmentPartitioningHandle)) {
                // do nothing if the current scan node's partitioning matches the fragment's
                return node;
            }

            TableLayoutHandle newTableLayoutHandle = metadata.getAlternativeLayoutHandle(session, node.getLayout().get(), fragmentPartitioningHandle);
            return new TableScanNode(
                    node.getId(),
                    node.getTable(),
                    node.getOutputSymbols(),
                    node.getAssignments(),
                    Optional.of(newTableLayoutHandle),
                    node.getCurrentConstraint(),
                    node.getEnforcedConstraint(),
                    node.isTemporaryTable());
        }
    }

    private static class PartitioningSymbolAssignments
    {
        private final List<Symbol> symbols;
        private final Map<Symbol, RowExpression> constants;

        private PartitioningSymbolAssignments(List<Symbol> symbols, Map<Symbol, RowExpression> constants)
        {
            this.symbols = ImmutableList.copyOf(requireNonNull(symbols, "symbols is null"));
            this.constants = ImmutableMap.copyOf(requireNonNull(constants, "constants is null"));
            checkArgument(
                    ImmutableSet.copyOf(symbols).containsAll(constants.keySet()),
                    "partitioningSymbols list must contain all partitioning symbols including constants");
        }

        public List<Symbol> getSymbols()
        {
            return symbols;
        }

        public Map<Symbol, RowExpression> getConstants()
        {
            return constants;
        }
    }
}

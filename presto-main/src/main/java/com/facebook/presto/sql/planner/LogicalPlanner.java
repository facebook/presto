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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.TupleDescriptor;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableCommitNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.CreateName;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.InsertReference;
import static com.facebook.presto.sql.planner.plan.TableWriterNode.WriterTarget;
import static com.google.common.base.Preconditions.checkState;

public class LogicalPlanner
{
    private final PlanNodeIdAllocator idAllocator;

    private final ConnectorSession session;
    private final List<PlanOptimizer> planOptimizers;
    private final SymbolAllocator symbolAllocator = new SymbolAllocator();
    private final Metadata metadata;

    public LogicalPlanner(ConnectorSession session,
            List<PlanOptimizer> planOptimizers,
            PlanNodeIdAllocator idAllocator,
            Metadata metadata)
    {
        Preconditions.checkNotNull(session, "session is null");
        Preconditions.checkNotNull(planOptimizers, "planOptimizers is null");
        Preconditions.checkNotNull(idAllocator, "idAllocator is null");
        Preconditions.checkNotNull(metadata, "metadata is null");

        this.session = session;
        this.planOptimizers = planOptimizers;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
    }

    public Plan plan(Analysis analysis)
    {
        RelationPlan plan;
        if (analysis.getCreateTableDestination().isPresent()) {
            plan = createTableCreationPlan(analysis);
        }
        else if (analysis.getInsertTarget().isPresent()) {
            plan = createInsertPlan(analysis);
        }
        else {
            plan = createRelationPlan(analysis);
        }

        PlanNode root = createOutputPlan(plan, analysis);

        // make sure we produce a valid plan. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        for (PlanOptimizer optimizer : planOptimizers) {
            root = optimizer.optimize(root, session, symbolAllocator.getTypes(), symbolAllocator, idAllocator);
        }

        // make sure we produce a valid plan after optimizations run. This is mainly to catch programming errors
        PlanSanityChecker.validate(root);

        return new Plan(root, symbolAllocator);
    }

    private RelationPlan createTableCreationPlan(Analysis analysis)
    {
        QualifiedTableName destination = analysis.getCreateTableDestination().get();

        RelationPlan plan = createRelationPlan(analysis);

        TableMetadata tableMetadata = createTableMetadata(destination, getOutputTableColumns(plan), plan.getSampleWeight().isPresent());
        checkState(!plan.getSampleWeight().isPresent() || metadata.canCreateSampledTables(session, destination.getCatalogName()), "Cannot write sampled data to a store that doesn't support sampling");

        return createTableWriterPlan(
                analysis,
                plan,
                tableMetadata,
                new CreateName(destination.getCatalogName(), tableMetadata));
    }

    private RelationPlan createInsertPlan(Analysis analysis)
    {
        TableHandle target = analysis.getInsertTarget().get();

        return createTableWriterPlan(
                analysis,
                createRelationPlan(analysis),
                metadata.getTableMetadata(target),
                new InsertReference(target));
    }

    private RelationPlan createTableWriterPlan(Analysis analysis, RelationPlan plan, TableMetadata tableMetadata, WriterTarget target)
    {
        List<Symbol> writerOutputs = ImmutableList.of(
                symbolAllocator.newSymbol("partialrows", BIGINT),
                symbolAllocator.newSymbol("fragment", VARCHAR));

        TableWriterNode writerNode = new TableWriterNode(
                idAllocator.getNextId(),
                plan.getRoot(),
                target,
                plan.getOutputSymbols(),
                getColumnNames(tableMetadata),
                writerOutputs,
                plan.getSampleWeight());

        List<Symbol> outputs = ImmutableList.of(symbolAllocator.newSymbol("rows", BIGINT));

        TableCommitNode commitNode = new TableCommitNode(
                idAllocator.getNextId(),
                writerNode,
                target,
                outputs);

        return new RelationPlan(commitNode, analysis.getOutputDescriptor(), outputs, Optional.<Symbol>absent());
    }

    private PlanNode createOutputPlan(RelationPlan plan, Analysis analysis)
    {
        ImmutableList.Builder<Symbol> outputs = ImmutableList.builder();
        ImmutableList.Builder<String> names = ImmutableList.builder();

        int columnNumber = 0;
        TupleDescriptor outputDescriptor = analysis.getOutputDescriptor();
        for (Field field : outputDescriptor.getVisibleFields()) {
            String name = field.getName().or("_col" + columnNumber);
            names.add(name);

            int fieldIndex = outputDescriptor.indexOf(field);
            Symbol symbol = plan.getSymbol(fieldIndex);
            outputs.add(symbol);

            columnNumber++;
        }

        return new OutputNode(idAllocator.getNextId(), plan.getRoot(), names.build(), outputs.build());
    }

    private RelationPlan createRelationPlan(Analysis analysis)
    {
        return new RelationPlanner(analysis, symbolAllocator, idAllocator, metadata, session)
                .process(analysis.getQuery(), null);
    }

    private TableMetadata createTableMetadata(QualifiedTableName table, List<ColumnMetadata> columns, boolean sampled)
    {
        String owner = session.getUser();
        ConnectorTableMetadata metadata = new ConnectorTableMetadata(table.asSchemaTableName(), columns, owner, sampled);
        // TODO: first argument should actually be connectorId
        return new TableMetadata(table.getCatalogName(), metadata);
    }

    private static List<ColumnMetadata> getOutputTableColumns(RelationPlan plan)
    {
        ImmutableList.Builder<ColumnMetadata> columns = ImmutableList.builder();
        int ordinalPosition = 0;
        for (Field field : plan.getDescriptor().getVisibleFields()) {
            columns.add(new ColumnMetadata(field.getName().get(), field.getType(), ordinalPosition, false));
            ordinalPosition++;
        }
        return columns.build();
    }

    private static List<String> getColumnNames(TableMetadata tableMetadata)
    {
        ImmutableList.Builder<String> list = ImmutableList.builder();
        for (ColumnMetadata column : tableMetadata.getColumns()) {
            list.add(column.getName());
        }
        return list.build();
    }
}

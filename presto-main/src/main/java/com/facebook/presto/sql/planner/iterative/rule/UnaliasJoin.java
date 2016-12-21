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
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import java.util.Optional;
import java.util.stream.Collectors;

public class UnaliasJoin
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        JoinNode parent = (JoinNode) node;

        PlanNode left = lookup.resolve(parent.getLeft());
        PlanNode right = lookup.resolve(parent.getRight());
        if (!(left instanceof ProjectNode) && !(right instanceof ProjectNode)) {
            return Optional.empty();
        }

        Renamer leftRenames = Renamer.empty();
        if (left instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) left;
            leftRenames = Renamer.from(project);
            left = leftRenames.renameOutputs(project);
        }

        Renamer rightRenames = Renamer.empty();
        if (right instanceof ProjectNode) {
            ProjectNode project = (ProjectNode) right;
            rightRenames = Renamer.from(project);
            right = rightRenames.renameOutputs(project);
        }

        Renamer renames = Renamer.merge(leftRenames, rightRenames);

        if (!renames.hasRenames()) {
            return Optional.empty();
        }

        JoinNode rewrittenJoin = new JoinNode(
                parent.getId(),
                parent.getType(),
                left,
                right,
                parent.getCriteria().stream()
                        .map(c -> new JoinNode.EquiJoinClause(
                                renames.rename(c.getLeft()),
                                renames.rename(c.getRight())))
                        .collect(Collectors.toList()),
                parent.getFilter().map(renames::rename),
                parent.getLeftHashSymbol().map(renames::rename),
                parent.getRightHashSymbol().map(renames::rename));

        // compute assignments to preserve original output names
        Assignments.Builder finalAssignments = Assignments.builder();
        for (Symbol symbol : parent.getOutputSymbols()) {
            finalAssignments.put(symbol, renames.rename(symbol).toSymbolReference());
        }

        return Optional.of(new ProjectNode(idAllocator.getNextId(), rewrittenJoin, finalAssignments.build()));
    }
}

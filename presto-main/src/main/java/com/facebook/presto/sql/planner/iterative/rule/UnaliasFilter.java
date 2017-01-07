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
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;

import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Given:
 * <pre>
 * - Filter :: [x1, ..., xn, y1, ..., ym]
 *         f1(x1, ..., xn, y1, ..., ym)
 *         ...
 *     - Project :: [x1, ..., xn, y1, ..., ym]
 *             x1 = k1
 *             ...
 *             xn = kn
 *             y1 = g1(k1, ..., kn)
 *             ...
 *             ym = gm(k1, ..., kn)
 *         - X ::= [k1, ..., kn]
 * </pre>
 * <p>
 * Where x1, ..., xn are simple renames in the child projection, into:
 * <p>
 * <pre>
 * - Project :: [x1, ..., xn, y1, ..., ym]
 *         x1 = k1
 *         ...
 *         xn = kn
 *         y1 = y1
 *         ...
 *         yn = ym
 *     - Filter
 *             f1(k1, ... kn, y1, ..., ym)
 *             ...
 *         - Project [k1, ..., kn, y1, ..., ym]
 *                 k1 = k1
 *                 ...
 *                 kn = kn
 *                 y1 = g1(k1, ..., kn)
 *                 ...
 *                 ym = gm(k1, ..., kn)
 *             - X
 * </pre>
 */
public class UnaliasFilter
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof FilterNode)) {
            return Optional.empty();
        }

        FilterNode parent = (FilterNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode child = (ProjectNode) source;

        Renamer renamer = Renamer.from(child);

        if (!renamer.hasRenames()) {
            return Optional.empty();
        }

        return Optional.of(
                new ProjectNode(
                        idAllocator.getNextId(),
                        new FilterNode(
                                parent.getId(),
                                renamer.renameOutputs(child),
                                renamer.rename(parent.getPredicate())),
                        // compute assignments to preserve original output names
                        Assignments.copyOf(
                                parent.getOutputSymbols().stream()
                                        .collect(Collectors.toMap(
                                                Function.identity(),
                                                symbol -> renamer.rename(symbol).toSymbolReference())))));
    }
}

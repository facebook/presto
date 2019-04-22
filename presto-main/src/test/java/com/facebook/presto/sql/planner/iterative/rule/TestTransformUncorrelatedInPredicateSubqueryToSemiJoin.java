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

import com.facebook.presto.sql.planner.AssignmentsUtils;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.tree.ExistsPredicate;
import com.facebook.presto.sql.tree.InPredicate;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static java.util.Collections.emptyList;

public class TestTransformUncorrelatedInPredicateSubqueryToSemiJoin
        extends BaseRuleTest
{
    @Test
    public void testDoesNotFireOnNoCorrelation()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToSemiJoin())
                .on(p -> p.apply(
                        AssignmentsUtils.of(),
                        emptyList(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireOnNonInPredicateSubquery()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToSemiJoin())
                .on(p -> p.apply(
                        AssignmentsUtils.of(p.symbol("x"), new ExistsPredicate(new LongLiteral("1"))),
                        emptyList(),
                        p.values(),
                        p.values()))
                .doesNotFire();
    }

    @Test
    public void testFiresForInPredicate()
    {
        tester().assertThat(new TransformUncorrelatedInPredicateSubqueryToSemiJoin())
                .on(p -> p.apply(
                        AssignmentsUtils.of(
                                p.symbol("x"),
                                new InPredicate(
                                        new SymbolReference("y"),
                                        new SymbolReference("z"))),
                        emptyList(),
                        p.values(p.symbol("y")),
                        p.values(p.symbol("z"))))
                .matches(node(SemiJoinNode.class, values("y"), values("z")));
    }
}

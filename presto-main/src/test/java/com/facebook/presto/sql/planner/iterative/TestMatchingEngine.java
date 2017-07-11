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

package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.matching.MatchingEngine;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.DummyMetadata;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.stream.Collectors.toSet;
import static org.testng.Assert.assertEquals;

public class TestMatchingEngine
{
    private final PlanBuilder planBuilder = new PlanBuilder(new PlanNodeIdAllocator(), new DummyMetadata());

    @Test
    public void testWithPlanNodeHierarchy()
    {
        Rule projectRule1 = new NoOpRule(Pattern.typeOf(ProjectNode.class));
        Rule projectRule2 = new NoOpRule(Pattern.typeOf(ProjectNode.class));
        Rule filterRule = new NoOpRule(Pattern.typeOf(FilterNode.class));
        Rule anyRule = new NoOpRule(Pattern.any());

        MatchingEngine matchingEngine = MatchingEngine.builder()
                .register(projectRule1)
                .register(projectRule2)
                .register(filterRule)
                .register(anyRule)
                .build();

        ProjectNode projectNode = planBuilder.project(Assignments.of(), planBuilder.values());
        FilterNode filterNode = planBuilder.filter(BooleanLiteral.TRUE_LITERAL, planBuilder.values());
        ValuesNode valuesNode = planBuilder.values();

        assertEquals(
                matchingEngine.getCandidates(projectNode).collect(toSet()),
                ImmutableSet.of(projectRule1, projectRule2, anyRule));
        assertEquals(
                matchingEngine.getCandidates(filterNode).collect(toSet()),
                ImmutableSet.of(filterRule, anyRule));
        assertEquals(
                matchingEngine.getCandidates(valuesNode).collect(toSet()),
                ImmutableSet.of(anyRule));
    }

    @Test
    public void testInterfacesHierarchy()
    {
        Rule a = new NoOpRule(Pattern.typeOf(A.class));
        Rule b = new NoOpRule(Pattern.typeOf(B.class));
        Rule ab = new NoOpRule(Pattern.typeOf(AB.class));

        MatchingEngine matchingEngine = MatchingEngine.builder()
                .register(a)
                .register(b)
                .register(ab)
                .build();

        assertEquals(
                matchingEngine.getCandidates(new A() {}).collect(toSet()),
                ImmutableSet.of(a));
        assertEquals(
                matchingEngine.getCandidates(new B() {}).collect(toSet()),
                ImmutableSet.of(b));
        assertEquals(
                matchingEngine.getCandidates(new AB()).collect(toSet()),
                ImmutableSet.of(ab, a, b));
    }

    private static class NoOpRule
            implements Rule
    {
        private final Pattern pattern;

        private NoOpRule(Pattern pattern)
        {
            this.pattern = pattern;
        }

        @Override
        public Pattern getPattern()
        {
            return pattern;
        }

        @Override
        public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
        {
            return Optional.empty();
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("pattern", pattern)
                    .toString();
        }
    }

    private interface A {}
    private interface B {}
    private static class AB implements A, B {}
}

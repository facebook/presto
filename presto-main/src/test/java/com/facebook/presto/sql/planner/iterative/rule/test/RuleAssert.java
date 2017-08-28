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
package com.facebook.presto.sql.planner.iterative.rule.test;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.matching.Match;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.StatsRecorder;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Memo;
import com.facebook.presto.sql.planner.iterative.PlanNodeMatcher;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.Trait;
import com.facebook.presto.sql.planner.iterative.TraitType;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.planner.assertions.PlanAssert.assertPlan;
import static com.facebook.presto.transaction.TransactionBuilder.transaction;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.fail;

public class RuleAssert
{
    private final Metadata metadata;
    private final CostCalculator costCalculator;
    private Session session;
    private Set<Rule<?>> beforeRules = ImmutableSet.of();
    private final Rule<?> rule;

    private final PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();

    private Map<Symbol, Type> symbols;
    private PlanNode plan;
    private final TransactionManager transactionManager;
    private final AccessControl accessControl;

    public RuleAssert(Metadata metadata, CostCalculator costCalculator, Session session, Rule rule, TransactionManager transactionManager, AccessControl accessControl)
    {
        this.metadata = metadata;
        this.costCalculator = costCalculator;
        this.session = session;
        this.rule = rule;
        this.transactionManager = transactionManager;
        this.accessControl = accessControl;
    }

    public RuleAssert setSystemProperty(String key, String value)
    {
        return withSession(Session.builder(session)
                .setSystemProperty(key, value)
                .build());
    }

    public RuleAssert withSession(Session session)
    {
        this.session = session;
        return this;
    }

    public RuleAssert withBefore(Set<Rule<?>> rules)
    {
        beforeRules = ImmutableSet.<Rule<?>>builder()
                .addAll(beforeRules)
                .addAll(rules)
                .build();
        return this;
    }

    public RuleAssert on(Function<PlanBuilder, PlanNode> planProvider)
    {
        checkArgument(plan == null, "plan has already been set");

        PlanBuilder builder = new PlanBuilder(idAllocator, metadata);
        plan = planProvider.apply(builder);
        symbols = builder.getSymbols();
        return this;
    }

    public RuleAssert doesNotFire()
    {
        RuleApplication ruleApplication = applyRule();

        if (ruleApplication.wasRuleApplied()) {
            fail(String.format(
                    "Expected %s to not fire for:\n%s",
                    rule.getClass().getName(),
                    inTransaction(session -> PlanPrinter.textLogicalPlan(plan, ruleApplication.types, metadata, costCalculator, session, 2))));
        }

        return this;
    }

    public RuleAssert matches(PlanMatchPattern pattern)
    {
        RuleApplication ruleApplication = applyRule();
        Map<Symbol, Type> types = ruleApplication.types;

        if (!ruleApplication.wasRuleApplied()) {
            fail(String.format(
                    "%s did not fire for:\n%s",
                    rule.getClass().getName(),
                    formatPlan(plan, types)));
        }

        PlanNode actual = ruleApplication.getResult();

        if (actual == plan) { // plans are not comparable, so we can only ensure they are not the same instance
            fail(String.format(
                    "%s: rule fired but return the original plan:\n%s",
                    rule.getClass().getName(),
                    formatPlan(plan, types)));
        }

        if (!ImmutableSet.copyOf(plan.getOutputSymbols()).equals(ImmutableSet.copyOf(actual.getOutputSymbols()))) {
            fail(String.format(
                    "%s: output schema of transformed and original plans are not equivalent\n" +
                            "\texpected: %s\n" +
                            "\tactual:   %s",
                    rule.getClass().getName(),
                    plan.getOutputSymbols(),
                    actual.getOutputSymbols()));
        }

        inTransaction(session -> {
            Map<PlanNodeId, PlanNodeCost> planNodeCosts = costCalculator.calculateCostForPlan(session, types, actual);
            assertPlan(session, metadata, costCalculator, new Plan(actual, types, planNodeCosts), ruleApplication.lookup, pattern);
            return null;
        });

        return this;
    }

    public <T extends Trait> RuleAssert satisfies(T trait)
    {
        RuleApplication ruleApplication = applyRule();
        if (!ruleApplication.satisfies(trait)) {
            Optional<T> actual = ruleApplication.lookup.resolveTrait(ruleApplication.planNode, trait.getType());
            fail(String.format("Plan node does not satisfy trait: %s, but it has: %s", trait, actual));
        }
        return this;
    }

    public <T extends Trait> RuleAssert doesNotSatisfy(T trait)
    {
        RuleApplication ruleApplication = applyRule();
        if (ruleApplication.satisfies(trait)) {
            Optional<T> actual = ruleApplication.lookup.resolveTrait(ruleApplication.planNode, trait.getType());
            fail(String.format("Expected plan node to not satisfy trait: %s, but it has: %s", trait, actual.get()));
        }
        return this;
    }

    public <T extends Trait> RuleAssert hasNo(TraitType<T> traitType)
    {
        RuleApplication ruleApplication = applyRule();
        Optional<T> trait = ruleApplication.lookup.resolveTrait(ruleApplication.planNode, traitType);
        if (trait.isPresent()) {
            fail(String.format("Expected plan node to not have trait, but found: %s", trait.get()));
        }
        return this;
    }

    private RuleApplication applyRule()
    {
        SymbolAllocator symbolAllocator = new SymbolAllocator(symbols);
        Memo memo;
        if (beforeRules.isEmpty()) {
            memo = new Memo(idAllocator, plan);
        }
        else {
            IterativeOptimizer iterativeOptimizer = new IterativeOptimizer(new StatsRecorder(), beforeRules);
            memo = iterativeOptimizer.explore(plan, session, symbolAllocator, idAllocator);
        }

        Lookup lookup = memo.getLookup();

        PlanNode memoRoot = memo.getNode(memo.getRootGroup());

        return inTransaction(session -> applyRule(rule, memoRoot, ruleContext(symbolAllocator, lookup, session)));
    }

    private static <T> RuleApplication applyRule(Rule<T> rule, PlanNode planNode, Rule.Context context)
    {
        PlanNodeMatcher matcher = new PlanNodeMatcher(context.getLookup());
        Match<T> match = matcher.match(rule.getPattern(), planNode);

        Rule.Result result;
        if (!rule.isEnabled(context.getSession()) || match.isEmpty()) {
            result = Rule.Result.empty();
        }
        else {
            result = rule.apply(match.value(), match.captures(), context);
        }

        return new RuleApplication(context.getLookup(), context.getSymbolAllocator().getTypes(), planNode, result);
    }

    private String formatPlan(PlanNode plan, Map<Symbol, Type> types)
    {
        return inTransaction(session -> PlanPrinter.textLogicalPlan(plan, types, metadata, costCalculator, session, 2));
    }

    private <T> T inTransaction(Function<Session, T> transactionSessionConsumer)
    {
        return transaction(transactionManager, accessControl)
                .singleStatement()
                .execute(session, session -> {
                    // metadata.getCatalogHandle() registers the catalog for the transaction
                    session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
                    return transactionSessionConsumer.apply(session);
                });
    }

    private Rule.Context ruleContext(SymbolAllocator symbolAllocator, Lookup lookup, Session session)
    {
        return new Rule.Context()
        {
            @Override
            public Lookup getLookup()
            {
                return lookup;
            }

            @Override
            public PlanNodeIdAllocator getIdAllocator()
            {
                return idAllocator;
            }

            @Override
            public SymbolAllocator getSymbolAllocator()
            {
                return symbolAllocator;
            }

            @Override
            public Session getSession()
            {
                return session;
            }
        };
    }

    private static class RuleApplication
    {
        private final Lookup lookup;
        private final Map<Symbol, Type> types;
        private final PlanNode planNode;
        private final Rule.Result result;

        public RuleApplication(Lookup lookup, Map<Symbol, Type> types, PlanNode planNode, Rule.Result result)
        {
            this.lookup = requireNonNull(lookup, "lookup is null");
            this.types = requireNonNull(types, "types is null");
            this.planNode = requireNonNull(planNode, "planNode is null");
            this.result = requireNonNull(result, "result is null");
        }

        private boolean wasRuleApplied()
        {
            return result.isPresent();
        }

        public PlanNode getResult()
        {
            return result.getTransformedPlan().orElseThrow(() -> new IllegalStateException("Rule was not applied"));
        }

        public <T extends Trait> boolean satisfies(T trait)
        {
            return lookup.isTraitSatisfied(planNode, trait);
        }
    }
}

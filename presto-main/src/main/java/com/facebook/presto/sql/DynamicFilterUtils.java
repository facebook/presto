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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DeferredSymbolReference;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class DynamicFilterUtils
{
    private DynamicFilterUtils() {}

    public static Expression stripDynamicFilters(Expression expression)
    {
        return combineConjuncts(extractConjuncts(expression)
                .stream()
                .filter(conjunct -> !isDynamicFilter(conjunct))
                .collect(toImmutableList()));
    }

    public static boolean isDynamicFilter(Expression expression)
    {
        if (!(expression instanceof ComparisonExpression)) {
            return false;
        }

        ComparisonExpression comparison = (ComparisonExpression) expression;
        checkState(!(comparison.getLeft() instanceof DeferredSymbolReference && comparison.getRight() instanceof DeferredSymbolReference), "Dynamic filter cannot have DeferredSymbolReferences");
        return comparison.getLeft() instanceof DeferredSymbolReference || comparison.getRight() instanceof DeferredSymbolReference;
    }

    public static ExtractDynamicFiltersResult extractDynamicFilters(Expression expression)
    {
        List<Expression> filters = extractConjuncts(expression);

        List<Expression> staticFilters = new ArrayList<>(filters.size());
        List<Expression> dynamicFilters = new ArrayList<>(filters.size());

        for (Expression filter : filters) {
            if (isDynamicFilter(filter)) {
                dynamicFilters.add(filter);
            }
            else {
                staticFilters.add(filter);
            }
        }

        return new ExtractDynamicFiltersResult(
                combineConjuncts(staticFilters),
                dynamicFilters.stream().map(DynamicFilter::from).collect(toImmutableSet()));
    }

    public static class ExtractDynamicFiltersResult
    {
        private final Expression staticFilters;
        private final Set<DynamicFilter> dynamicFilters;

        public ExtractDynamicFiltersResult(Expression staticFilters, Set<DynamicFilter> dynamicFilters)
        {
            this.staticFilters = requireNonNull(staticFilters, "staticFilters is null");
            this.dynamicFilters = ImmutableSet.copyOf(requireNonNull(dynamicFilters, "dynamicFilters is null"));
        }

        public Expression getStaticFilters()
        {
            return staticFilters;
        }

        public Set<DynamicFilter> getDynamicFilters()
        {
            return dynamicFilters;
        }
    }
}

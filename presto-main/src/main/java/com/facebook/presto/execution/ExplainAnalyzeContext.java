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
package com.facebook.presto.execution;

import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.StatsCalculator;

import javax.inject.Inject;

import static java.util.Objects.requireNonNull;

public class ExplainAnalyzeContext
{
    private final QueryPerformanceFetcher queryPerformanceFetcher;
    private final StatsCalculator statsCalculator;
    private final CostCalculator costCalculator;

    @Inject
    public ExplainAnalyzeContext(
            QueryPerformanceFetcher queryPerformanceFetcher,
            StatsCalculator statsCalculator,
            CostCalculator costCalculator)
    {
        this.queryPerformanceFetcher = requireNonNull(queryPerformanceFetcher, "queryPerformanceFetcher is null");
        this.statsCalculator = requireNonNull(statsCalculator, "statsCalculator is null");
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    public QueryPerformanceFetcher getQueryPerformanceFetcher()
    {
        return queryPerformanceFetcher;
    }

    public StatsCalculator getStatsCalculator()
    {
        return statsCalculator;
    }

    public CostCalculator getCostCalculator()
    {
        return costCalculator;
    }
}

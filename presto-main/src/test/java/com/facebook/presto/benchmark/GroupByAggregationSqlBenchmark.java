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
package com.facebook.presto.benchmark;

import com.facebook.presto.util.LocalQueryRunner;

import java.util.concurrent.ExecutorService;

import static com.facebook.presto.benchmark.BenchmarkQueryRunner.createLocalQueryRunner;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

public class GroupByAggregationSqlBenchmark
        extends AbstractSqlBenchmark
{
    public GroupByAggregationSqlBenchmark(LocalQueryRunner localQueryRunner)
    {
        super(localQueryRunner, "sql_groupby_agg", 15, 100, "select orderstatus, sum(totalprice) from orders group by orderstatus");
    }

    public static void main(String[] args)
    {
        ExecutorService executor = newCachedThreadPool(daemonThreadsNamed("test"));
        new GroupByAggregationSqlBenchmark(createLocalQueryRunner(executor)).runBenchmark(new SimpleLineBenchmarkResultWriter(System.out));
    }
}

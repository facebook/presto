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
package com.facebook.presto.operator;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.MarkDistinctOperator.MarkDistinctOperatorFactory;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.OperatorAssertion.appendSampleWeight;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_BOOLEAN;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_LONG;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestMarkDistinctOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSampledMarkDistinct()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();
        input = appendSampleWeight(input, 2);

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, ImmutableList.of(SINGLE_LONG, SINGLE_LONG), ImmutableList.of(0), Optional.of(1));
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult.Builder expected = resultBuilder(SINGLE_LONG, SINGLE_LONG, SINGLE_BOOLEAN);
        for (int i = 0; i < 100; i++) {
            expected.row(i, 1, true);
            expected.row(i, 1, false);
            expected.row(i, 2, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operator, input, expected.build());
    }

    @Test
    public void testMarkDistinct()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(SINGLE_LONG)
                .addSequencePage(100, 0)
                .addSequencePage(100, 0)
                .build();

        OperatorFactory operatorFactory = new MarkDistinctOperatorFactory(0, ImmutableList.of(SINGLE_LONG), ImmutableList.of(0), Optional.<Integer>absent());
        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult.Builder expected = resultBuilder(SINGLE_LONG, SINGLE_BOOLEAN);
        for (int i = 0; i < 100; i++) {
            expected.row(i, true);
            expected.row(i, false);
        }

        OperatorAssertion.assertOperatorEqualsIgnoreOrder(operator, input, expected.build());
    }
}

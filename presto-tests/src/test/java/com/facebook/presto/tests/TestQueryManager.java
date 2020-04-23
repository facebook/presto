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
package com.facebook.presto.tests;

import com.facebook.presto.common.PrestoException;
import com.facebook.presto.dispatcher.DispatchManager;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.execution.QueryState;
import com.facebook.presto.execution.TestingSessionContext;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.StandardErrorCode.EXCEEDED_CPU_LIMIT;
import static com.facebook.presto.common.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.execution.QueryState.FAILED;
import static com.facebook.presto.execution.QueryState.RUNNING;
import static com.facebook.presto.execution.TestQueryRunnerUtil.createQuery;
import static com.facebook.presto.execution.TestQueryRunnerUtil.waitForQueryState;
import static com.facebook.presto.tests.tpch.TpchQueryRunnerBuilder.builder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestQueryManager
{
    private DistributedQueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        queryRunner = TpchQueryRunnerBuilder.builder().build();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        queryRunner.close();
        queryRunner = null;
    }

    @Test(timeOut = 60_000L)
    public void testFailQuery()
            throws Exception
    {
        DispatchManager dispatchManager = queryRunner.getCoordinator().getDispatchManager();
        QueryId queryId = dispatchManager.createQueryId();
        dispatchManager.createQuery(
                queryId,
                "slug",
                new TestingSessionContext(TEST_SESSION),
                "SELECT * FROM lineitem")
                .get();

        // wait until query starts running
        while (true) {
            QueryState state = dispatchManager.getQueryInfo(queryId).getState();
            if (state.isDone()) {
                fail("unexpected query state: " + state);
            }
            if (state == RUNNING) {
                break;
            }
            Thread.sleep(100);
        }

        // cancel query
        QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
        queryManager.failQuery(queryId, new PrestoException(GENERIC_INTERNAL_ERROR, "mock exception"));
        QueryInfo queryInfo = queryManager.getFullQueryInfo(queryId);
        assertEquals(queryInfo.getState(), FAILED);
        assertEquals(queryInfo.getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertNotNull(queryInfo.getFailureInfo());
        assertEquals(queryInfo.getFailureInfo().getMessage(), "mock exception");
    }

    @Test(timeOut = 60_000L)
    public void testQueryCpuLimit()
            throws Exception
    {
        try (DistributedQueryRunner queryRunner = builder().setSingleExtraProperty("query.max-cpu-time", "1ms").build()) {
            QueryId queryId = createQuery(queryRunner, TEST_SESSION, "SELECT COUNT(*) FROM lineitem");
            waitForQueryState(queryRunner, queryId, FAILED);
            QueryManager queryManager = queryRunner.getCoordinator().getQueryManager();
            BasicQueryInfo queryInfo = queryManager.getQueryInfo(queryId);
            assertEquals(queryInfo.getState(), FAILED);
            assertEquals(queryInfo.getErrorCode(), EXCEEDED_CPU_LIMIT.toErrorCode());
        }
    }
}

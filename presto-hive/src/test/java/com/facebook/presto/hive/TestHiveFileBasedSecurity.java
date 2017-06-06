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
package com.facebook.presto.hive;

import com.facebook.presto.Session;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.HiveQueryRunner.createQueryRunner;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.tpch.TpchTable.getTables;
import static org.testng.Assert.assertEquals;

public class TestHiveFileBasedSecurity
{
    private QueryRunner queryRunner;

    @BeforeClass
    public void setUp()
            throws Exception
    {
        String path = this.getClass().getResource("security.json").getPath();
        queryRunner = createQueryRunner(getTables(), ImmutableMap.of(), "file", ImmutableMap.of("security.config-file", path));
    }

    @AfterClass
    public void tearDown()
    {
        queryRunner.close();
    }

    @Test
    public void testAdminCanRead()
    {
        Session admin = getSession("user");
        queryRunner.execute(admin, "SELECT * FROM orders");
    }

    @Test(expectedExceptions = RuntimeException.class, expectedExceptionsMessageRegExp = ".*Access Denied: Cannot select from table tpch.orders.*")
    public void testNonAdminCannotRead()
    {
        Session bob = getSession("bob");
        queryRunner.execute(bob, "SELECT * FROM orders");
    }

    @Test
    public void testShowGrants()
            throws Exception
    {
        List<TpchTable<?>> tables = getTables().stream()
                .filter(t -> t.getTableName().equals("orders"))
                .collect(toImmutableList());
        String path = this.getClass().getResource("security.json").getPath();
        QueryRunner queryRunner = createQueryRunner(tables, ImmutableMap.of(), "file", ImmutableMap.of("security.config-file", path));
        Session user = getSession("user");

        assertEquals(queryRunner.execute(user, "SHOW GRANTS").getRowCount(), 8);
        assertEquals(queryRunner.execute(user, "SHOW GRANTS ON tpch.orders").getRowCount(), 4);
        assertEquals(queryRunner.execute(user, "SHOW GRANTS ON tpch_bucketed.orders").getRowCount(), 4);

        queryRunner.close();
    }

    private Session getSession(String user)
    {
        return testSessionBuilder()
                .setCatalog(queryRunner.getDefaultSession().getCatalog().get())
                .setSchema(queryRunner.getDefaultSession().getSchema().get())
                .setIdentity(new Identity(user, Optional.empty())).build();
    }
}

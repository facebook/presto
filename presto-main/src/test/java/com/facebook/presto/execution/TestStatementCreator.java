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

import com.facebook.presto.Session;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.QualifiedName;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.table;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

public class TestStatementCreator
{
    StatementCreator statementCreator = new StatementCreator(new SqlParser());

    @Test
    public void testSelectStatement() throws Exception
    {
        assertEquals(statementCreator.createStatement("select * from foo", TEST_SESSION),
                     simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatement() throws Exception
    {
        Session session = TEST_SESSION.withPreparedStatement("my_query", "select * from foo");
        assertEquals(statementCreator.createStatement("execute my_query", session),
                     simpleQuery(selectList(new AllColumns()), table(QualifiedName.of("foo"))));
    }

    @Test
    public void testExecuteStatementDoesNotExist() throws Exception
    {
        try {
            statementCreator.createStatement("execute my_query", TEST_SESSION);
            fail();
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), StandardErrorCode.NOT_FOUND.toErrorCode());
        }
    }
}

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
package com.facebook.presto.tests.hive;

import com.teradata.tempto.Requirement;
import com.teradata.tempto.RequirementsProvider;
import com.teradata.tempto.configuration.Configuration;
import com.teradata.tempto.fulfillment.table.MutableTableRequirement;
import com.teradata.tempto.fulfillment.table.hive.HiveTableDefinition;
import com.teradata.tempto.query.QueryResult;
import org.testng.annotations.Test;

import static com.facebook.presto.tests.TestGroups.HIVE_CONNECTOR;
import static com.facebook.presto.tests.TestGroups.SMOKE;
import static com.teradata.tempto.Requirements.compose;
import static com.teradata.tempto.fulfillment.table.MutableTableRequirement.State.CREATED;
import static com.teradata.tempto.fulfillment.table.MutableTablesState.mutableTablesState;
import static com.teradata.tempto.fulfillment.table.hive.InlineDataSource.createResourceDataSource;
import static com.teradata.tempto.fulfillment.table.hive.tpch.TpchTableDefinitions.NATION;
import static com.teradata.tempto.query.QueryExecutor.query;
import static com.teradata.tempto.query.QueryType.UPDATE;
import static org.assertj.core.api.Assertions.assertThat;

public class TestTablePartitioningInsertInto
        extends HivePartitioningTest
        implements RequirementsProvider
{
    private static final String PARTITIONED_NATION_NAME = "partitioned_nation_read_test";
    private static final String TARGET_NATION_NAME = "target_nation_test";

    private static final int NUMBER_OF_LINES_PER_SPLIT = 5;
    private static final String DATA_REVISION = "1";
    private static final HiveTableDefinition PARTITIONED_NATION =
            HiveTableDefinition.builder(PARTITIONED_NATION_NAME)
                    .setCreateTableDDLTemplate("" +
                            "CREATE %EXTERNAL% TABLE %NAME%(" +
                            "   p_nationkey     BIGINT," +
                            "   p_name          STRING," +
                            "   p_comment       STRING) " +
                            "PARTITIONED BY (p_regionkey INT)" +
                            "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ")
                    .addPartition("p_regionkey=1", createResourceDataSource(PARTITIONED_NATION_NAME, DATA_REVISION, partitionDataFileResource(1)))
                    .addPartition("p_regionkey=2", createResourceDataSource(PARTITIONED_NATION_NAME, DATA_REVISION, partitionDataFileResource(2)))
                    .addPartition("p_regionkey=3", createResourceDataSource(PARTITIONED_NATION_NAME, DATA_REVISION, partitionDataFileResource(3)))
                    .build();

    private static String partitionDataFileResource(int region)
    {
        return "com/facebook/presto/tests/hive/data/partitioned_nation/nation_region_" + region + ".textfile";
    }

    @Override
    public Requirement getRequirements(Configuration configuration)
    {
        return compose(
                MutableTableRequirement.builder(PARTITIONED_NATION).build(),
                MutableTableRequirement.builder(NATION).withState(CREATED).withName(TARGET_NATION_NAME).build());
    }

    @Test(groups = {HIVE_CONNECTOR, SMOKE})
    public void selectFromPartitionedNation()
            throws Exception
    {
        // read all data
        testQuerySplitsNumber("p_nationkey < 40", 3);

        // read no partitions
        testQuerySplitsNumber("p_regionkey = 42", 0);

        // read one partition
        testQuerySplitsNumber("p_regionkey = 2 AND p_nationkey < 40", 1);
        // read two partitions
        testQuerySplitsNumber("p_regionkey = 2 AND p_nationkey < 40 or p_regionkey = 3", 2);
        // read all (three) partitions
        testQuerySplitsNumber("p_regionkey = 2 OR p_nationkey < 40", 3);

        // range read two partitions
        testQuerySplitsNumber("p_regionkey <= 2", 2);
        testQuerySplitsNumber("p_regionkey <= 1 OR p_regionkey >= 3", 2);
    }

    private void testQuerySplitsNumber(String condition, int expectedProcessedSplits)
            throws Exception
    {
        String partitionedNation = mutableTablesState().get(PARTITIONED_NATION_NAME).getNameInDatabase();
        String targetNation = mutableTablesState().get(TARGET_NATION_NAME).getNameInDatabase();
        String query = String.format(
                "INSERT INTO %s SELECT p_nationkey, p_name, p_regionkey, p_comment FROM %s WHERE %s",
                targetNation,
                partitionedNation,
                condition);
        QueryResult queryResult = query(query, UPDATE);

        long processedLinesCount = getProcessedLinesCount(query, queryResult);
        assertThat(processedLinesCount).isEqualTo(expectedProcessedSplits * NUMBER_OF_LINES_PER_SPLIT);
    }
}

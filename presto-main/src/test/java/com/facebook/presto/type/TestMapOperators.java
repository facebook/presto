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
package com.facebook.presto.type;

import com.facebook.presto.operator.scalar.FunctionAssertions;
import com.facebook.presto.operator.scalar.MapConstructor;
import com.facebook.presto.spi.type.SqlTimestamp;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;

public class TestMapOperators
{
    private FunctionAssertions functionAssertions;

    @BeforeClass
    public void setUp()
    {
        functionAssertions = new FunctionAssertions();
        functionAssertions.getMetadata().getFunctionRegistry().addFunctions(ImmutableList.of(new MapConstructor(1, new TypeRegistry())));
        functionAssertions.getMetadata().getFunctionRegistry().addFunctions(ImmutableList.of(new MapConstructor(2, new TypeRegistry())));
    }

    private void assertFunction(String projection, Object expected)
    {
        functionAssertions.assertFunction(projection, expected);
    }

    @Test
    public void testConstructor()
            throws Exception
    {
        assertFunction("MAP(1, 2, 3, 4)", ImmutableMap.of(1L, 2L, 3L, 4L));
        Map<Long, Long> map = new HashMap<>();
        map.put(1L, 2L);
        map.put(3L, null);
        assertFunction("MAP(1, 2, 3, NULL)", map);
        assertFunction("MAP(1, 2.0, 3, 4.0)", ImmutableMap.of(1L, 2.0, 3L, 4.0));
        assertFunction("MAP(1.0, ARRAY[1, 2], 2.0, ARRAY[3])", ImmutableMap.of(1.0, ImmutableList.of(1L, 2L), 2.0, ImmutableList.of(3L)));
        assertFunction("MAP('puppies', 'kittens')", ImmutableMap.of("puppies", "kittens"));
        assertFunction("MAP(TRUE, 2, FALSE, 4)", ImmutableMap.of(true, 2L, false, 4L));
        assertFunction("MAP('1', from_unixtime(1), '100', from_unixtime(100))", ImmutableMap.of(
                "1",
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                "100",
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey())));
        assertFunction("MAP(from_unixtime(1), 1.0, from_unixtime(100), 100.0)", ImmutableMap.of(
                new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()),
                1.0,
                new SqlTimestamp(100_000, TEST_SESSION.getTimeZoneKey()),
                100.0));
    }

    @Test
    public void testSubscript()
            throws Exception
    {
        assertFunction("MAP(1, 2, 3, 4)[3]", 4L);
        assertFunction("MAP(1, 2, 3, NULL)[3]", null);
        assertFunction("MAP(1, 2.0, 3, 4.0)[1]", 2.0);
        assertFunction("MAP(1.0, ARRAY[1, 2], 2.0, ARRAY[3])[1.0]", ImmutableList.of(1L, 2L));
        assertFunction("MAP('puppies', 'kittens')['puppies']", "kittens");
        assertFunction("MAP(TRUE, 2, FALSE, 4)[TRUE]", 2L);
        assertFunction("MAP('1', from_unixtime(1), '100', from_unixtime(100))['1']", new SqlTimestamp(1000, TEST_SESSION.getTimeZoneKey()));
        assertFunction("MAP(from_unixtime(1), 1.0, from_unixtime(100), 100.0)[from_unixtime(1)]", 1.0);
    }
}

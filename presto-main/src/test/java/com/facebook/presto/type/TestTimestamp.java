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

import org.testng.annotations.Test;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;

public class TestTimestamp
        extends TestTimestampBase
{
    public TestTimestamp()
    {
        super(false);
    }

    @Test
    public void testCastFromSliceFailsOnTimestampWithTimeZone()
    {
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 03:04:05.321 +07:09' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 03:04:05.321 +07:09");
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 03:04:05 +07:09' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 03:04:05 +07:09");
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 03:04 +07:09' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 03:04 +07:09");
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 +07:09' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 +07:09");

        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 03:04:05.321 Asia/Oral' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 03:04:05.321 Asia/Oral");
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 03:04:05 Asia/Oral' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 03:04:05 Asia/Oral");
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 03:04 Asia/Oral' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 03:04 Asia/Oral");
        functionAssertions.assertFunctionThrowsPrestoException(
                "cast('2001-1-22 Asia/Oral' as timestamp)",
                INVALID_CAST_ARGUMENT,
                "Value cannot be cast to timestamp as it contains explicitly defined TIME ZONE: 2001-1-22 Asia/Oral");
    }
}

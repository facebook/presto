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

import com.facebook.presto.spi.type.SqlTimestamp;
import org.testng.annotations.Test;

import java.time.LocalDateTime;
import java.time.ZonedDateTime;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;

public class TestTimestampLegacy
        extends TestTimestampBase
{
    public TestTimestampLegacy()
    {
        super(true);
    }

    @Test
    public void testCastFromSliceWithZone()
    {
        assertFunction("cast('2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000, WEIRD_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
        assertFunction("cast('2001-1-22 03:04:05 +07:09' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 0, WEIRD_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
        assertFunction("cast('2001-1-22 03:04 +07:09' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 0, 0, WEIRD_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
        assertFunction("cast('2001-1-22 +07:09' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 0, 0, 0, 0, WEIRD_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));

        assertFunction("cast('2001-1-22 03:04:05.321 Asia/Oral' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000, ORAL_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
        assertFunction("cast('2001-1-22 03:04:05 Asia/Oral' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 0, ORAL_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
        assertFunction("cast('2001-1-22 03:04 Asia/Oral' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 0, 0, ORAL_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
        assertFunction("cast('2001-1-22 Asia/Oral' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 0, 0, 0, 0, ORAL_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
    }

    @Test
    public void testCastFromTimeWithTimeZone()
    {
        assertFunction("cast(TIME '03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(1970, 1, 1, 3, 4, 5, 321_000_000, WEIRD_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
    }

    @Test
    public void testCastFromTimestampWithTimeZone()
    {
        assertFunction("cast(TIMESTAMP '2001-1-22 03:04:05.321 +07:09' as timestamp)",
                TIMESTAMP,
                toTimestamp(ZonedDateTime.of(2001, 1, 22, 3, 4, 5, 321_000_000, WEIRD_ZONE.toTimeZone().toZoneId()).withZoneSameInstant(DATE_TIME_ZONE.toTimeZone().toZoneId()).toLocalDateTime()));
    }

    @Override
    protected SqlTimestamp toTimestamp(LocalDateTime localDateTime)
    {
        ZonedDateTime zonedDateTime = localDateTime.atZone(DATE_TIME_ZONE.toTimeZone().toZoneId());
        return new SqlTimestamp(zonedDateTime.toEpochSecond() * 1000 + zonedDateTime.getNano() / 1_000_000, session.getTimeZoneKey());
    }
}

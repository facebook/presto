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
package com.facebook.presto.spi.type;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Objects;
import java.util.TimeZone;

import static com.facebook.presto.spi.type.TimeZoneIndex.getTimeZoneForKey;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackZoneKey;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKey;

public final class TimestampWithTimeZone
{
    private final long millisUtc;
    private final TimeZoneKey timeZoneKey;

    public TimestampWithTimeZone(long timestampWithTimeZone)
    {
        millisUtc = unpackMillisUtc(timestampWithTimeZone);
        timeZoneKey = unpackZoneKey(timestampWithTimeZone);
    }

    public TimestampWithTimeZone(long millisUtc, TimeZoneKey timeZoneKey)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = timeZoneKey;
    }

    public TimestampWithTimeZone(long millisUtc, TimeZone timeZone)
    {
        this.millisUtc = millisUtc;
        this.timeZoneKey = getTimeZoneKey(timeZone.getID());
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(millisUtc, timeZoneKey);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        TimestampWithTimeZone other = (TimestampWithTimeZone) obj;
        return Objects.equals(this.millisUtc, other.millisUtc) &&
                Objects.equals(this.timeZoneKey, other.timeZoneKey);
    }

    @Override
    public String toString()
    {
        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        format.setTimeZone(getTimeZoneForKey(timeZoneKey));
        return format.format(new Date(millisUtc)) + " " + timeZoneKey.getTimeZoneId();
    }
}

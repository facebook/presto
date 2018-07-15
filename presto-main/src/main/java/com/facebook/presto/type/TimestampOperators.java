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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.function.BlockIndex;
import com.facebook.presto.spi.function.BlockPosition;
import com.facebook.presto.spi.function.IsNull;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarOperator;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.StandardTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.XxHash64;
import org.joda.time.chrono.ISOChronology;

import java.util.concurrent.TimeUnit;

import static com.facebook.presto.operator.scalar.DateTimeFunctions.offsetMinutes;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.BETWEEN;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.spi.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.HASH_CODE;
import static com.facebook.presto.spi.function.OperatorType.INDETERMINATE;
import static com.facebook.presto.spi.function.OperatorType.IS_DISTINCT_FROM;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN;
import static com.facebook.presto.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.NOT_EQUAL;
import static com.facebook.presto.spi.function.OperatorType.SUBTRACT;
import static com.facebook.presto.spi.function.OperatorType.XX_HASH_64;
import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.type.DateTimeOperators.modulo24Hour;
import static com.facebook.presto.util.DateTimeUtils.parseTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeUtils.printTimestampWithoutTimeZone;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static io.airlift.slice.SliceUtf8.trim;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.concurrent.TimeUnit.MINUTES;

public final class TimestampOperators
{
    private TimestampOperators()
    {
    }

    @ScalarOperator(SUBTRACT)
    @SqlType(StandardTypes.INTERVAL_DAY_TO_SECOND)
    public static long subtract(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left - right;
    }

    @ScalarOperator(EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean equal(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left == right;
    }

    @ScalarOperator(NOT_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    @SqlNullable
    public static Boolean notEqual(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left != right;
    }

    @ScalarOperator(LESS_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThan(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left < right;
    }

    @ScalarOperator(LESS_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean lessThanOrEqual(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left <= right;
    }

    @ScalarOperator(GREATER_THAN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThan(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left > right;
    }

    @ScalarOperator(GREATER_THAN_OR_EQUAL)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean greaterThanOrEqual(@SqlType(StandardTypes.TIMESTAMP) long left, @SqlType(StandardTypes.TIMESTAMP) long right)
    {
        return left >= right;
    }

    @ScalarOperator(BETWEEN)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean between(@SqlType(StandardTypes.TIMESTAMP) long value, @SqlType(StandardTypes.TIMESTAMP) long min, @SqlType(StandardTypes.TIMESTAMP) long max)
    {
        return min <= value && value <= max;
    }

    @ScalarFunction("date")
    @ScalarOperator(CAST)
    @SqlType(StandardTypes.DATE)
    public static long castToDate(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        ISOChronology chronology;
        if (session.isLegacyTimestamp()) {
            // round down the current timestamp to days
            chronology = getChronology(session.getTimeZoneKey());
            long date = chronology.dayOfYear().roundFloor(value);
            // date is currently midnight in timezone of the session
            // convert to UTC
            long millis = date + chronology.getZone().getOffset(date);
            return TimeUnit.MILLISECONDS.toDays(millis);
        }
        else {
            return TimeUnit.MILLISECONDS.toDays(value);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME)
    public static long castToTime(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        if (session.isLegacyTimestamp()) {
            return modulo24Hour(getChronology(session.getTimeZoneKey()), value);
        }
        else {
            return modulo24Hour(value);
        }
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIME_WITH_TIME_ZONE)
    public static long castToTimeWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        long localTime;
        long offsetMinutes;
        if (session.isLegacyTimestamp()) {
            localTime = chronology.millisOfDay().get(value);
            offsetMinutes = offsetMinutes(chronology.getZone(), value);
        }
        else {
            // TODO do we handle DST correctly?
            localTime = modulo24Hour(value);
            offsetMinutes = offsetMinutes(chronology.getZone(), chronology.getZone().convertLocalToUTC(value, false));
        }

        return packDateTimeWithZone(localTime + MINUTES.toMillis(offsetMinutes), offsetMinutes);
    }

    @ScalarOperator(CAST)
    @SqlType(StandardTypes.TIMESTAMP_WITH_TIME_ZONE)
    public static long castToTimestampWithTimeZone(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        if (session.isLegacyTimestamp()) {
            return packDateTimeWithZone(value, session.getTimeZoneKey());
        }
        else {
            ISOChronology localChronology = getChronology(session.getTimeZoneKey());

            // This cast does treat TIMESTAMP as wall time in session TZ. This means that in order to get
            // its UTC representation we need to shift the value by the offset of TZ.
            return packDateTimeWithZone(localChronology.getZone().convertLocalToUTC(value, false), session.getTimeZoneKey());
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType("varchar(x)")
    public static Slice castToSlice(ConnectorSession session, @SqlType(StandardTypes.TIMESTAMP) long value)
    {
        if (session.isLegacyTimestamp()) {
            return utf8Slice(printTimestampWithoutTimeZone(session.getTimeZoneKey(), value));
        }
        else {
            return utf8Slice(printTimestampWithoutTimeZone(value));
        }
    }

    @ScalarOperator(CAST)
    @LiteralParameters("x")
    @SqlType(StandardTypes.TIMESTAMP)
    public static long castFromSlice(ConnectorSession session, @SqlType("varchar(x)") Slice value)
    {
        // This accepts value with or without time zone
        if (session.isLegacyTimestamp()) {
            try {
                return parseTimestampWithoutTimeZone(session.getTimeZoneKey(), trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
        else {
            try {
                return parseTimestampWithoutTimeZone(trim(value).toStringUtf8());
            }
            catch (IllegalArgumentException e) {
                throw new PrestoException(INVALID_CAST_ARGUMENT, "Value cannot be cast to timestamp: " + value.toStringUtf8(), e);
            }
        }
    }

    @ScalarOperator(HASH_CODE)
    @SqlType(StandardTypes.BIGINT)
    public static long hashCode(@SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return AbstractLongType.hash(value);
    }

    @ScalarOperator(IS_DISTINCT_FROM)
    public static class TimestampDistinctFromOperator
    {
        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @SqlType(StandardTypes.TIMESTAMP) long left,
                @IsNull boolean leftNull,
                @SqlType(StandardTypes.TIMESTAMP) long right,
                @IsNull boolean rightNull)
        {
            if (leftNull != rightNull) {
                return true;
            }
            if (leftNull) {
                return false;
            }
            return notEqual(left, right);
        }

        @SqlType(StandardTypes.BOOLEAN)
        public static boolean isDistinctFrom(
                @BlockPosition @SqlType(value = StandardTypes.TIMESTAMP, nativeContainerType = long.class) Block left,
                @BlockIndex int leftPosition,
                @BlockPosition @SqlType(value = StandardTypes.TIMESTAMP, nativeContainerType = long.class) Block right,
                @BlockIndex int rightPosition)
        {
            if (left.isNull(leftPosition) != right.isNull(rightPosition)) {
                return true;
            }
            if (left.isNull(leftPosition)) {
                return false;
            }
            return notEqual(TIMESTAMP.getLong(left, leftPosition), TIMESTAMP.getLong(right, rightPosition));
        }
    }

    @ScalarOperator(INDETERMINATE)
    @SqlType(StandardTypes.BOOLEAN)
    public static boolean indeterminate(@SqlType(StandardTypes.TIMESTAMP) long value, @IsNull boolean isNull)
    {
        return isNull;
    }

    @ScalarOperator(XX_HASH_64)
    @SqlType(StandardTypes.BIGINT)
    public static long xxHash64(@SqlType(StandardTypes.TIMESTAMP) long value)
    {
        return XxHash64.hash(value);
    }
}

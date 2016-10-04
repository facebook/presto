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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.operator.Description;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.IntervalDayTimeType;
import com.facebook.presto.spi.type.IntervalYearMonthType;
import com.facebook.presto.spi.type.TimeType;
import com.facebook.presto.spi.type.TimeWithTimeZoneType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TimestampWithTimeZoneType;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.util.DateTimeZoneIndex;
import com.facebook.presto.util.ThreadLocalCache;
import com.google.common.base.Charsets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.DateTimeField;
import org.joda.time.DateTimeZone;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import static com.facebook.presto.spi.type.DateTimeEncoding.packDateTimeWithZone;
import static com.facebook.presto.spi.type.DateTimeEncoding.unpackMillisUtc;
import static com.facebook.presto.spi.type.DateTimeEncoding.updateMillisUtc;
import static com.facebook.presto.spi.type.TimeZoneKey.getTimeZoneKeyForOffset;
import static com.facebook.presto.util.DateTimeZoneIndex.extractZoneOffsetMinutes;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.facebook.presto.util.DateTimeZoneIndex.packDateTimeWithZone;

public final class DateTimeFunctions
{
    private static final ThreadLocalCache<Slice, DateTimeFormatter> DATETIME_FORMATTER_CACHE = new ThreadLocalCache<Slice, DateTimeFormatter>(100)
    {
        @Override
        protected DateTimeFormatter load(Slice format)
        {
            return createDateTimeFormatter(format);
        }
    };

    private static final ISOChronology UTC_CHRONOLOGY = ISOChronology.getInstance(DateTimeZone.UTC);
    private static final DateTimeField MILLIS_OF_DAY = UTC_CHRONOLOGY.millisOfDay();
    private static final DateTimeField SECOND_OF_MINUTE = UTC_CHRONOLOGY.secondOfMinute();
    private static final DateTimeField MINUTE_OF_HOUR = UTC_CHRONOLOGY.minuteOfHour();
    private static final DateTimeField HOUR_OF_DAY = UTC_CHRONOLOGY.hourOfDay();
    private static final DateTimeField DAY_OF_WEEK = UTC_CHRONOLOGY.dayOfWeek();
    private static final DateTimeField DAY_OF_MONTH = UTC_CHRONOLOGY.dayOfMonth();
    private static final DateTimeField DAY_OF_YEAR = UTC_CHRONOLOGY.dayOfYear();
    private static final DateTimeField WEEK_OF_YEAR = UTC_CHRONOLOGY.weekOfWeekyear();
    private static final DateTimeField MONTH_OF_YEAR = UTC_CHRONOLOGY.monthOfYear();
    private static final DateTimeField YEAR = UTC_CHRONOLOGY.year();
    private static final DateTimeField CENTURY = UTC_CHRONOLOGY.centuryOfEra();
    private static final int MILLISECONDS_IN_SECOND = 1000;
    private static final int MILLISECONDS_IN_MINUTE = 60 * MILLISECONDS_IN_SECOND;
    private static final int MILLISECONDS_IN_HOUR = 60 * MILLISECONDS_IN_MINUTE;
    private static final int MILLISECONDS_IN_DAY = 24 * MILLISECONDS_IN_HOUR;
    private static final int MAX_TIME_ZONE_OFFSET = 13 * 60;
    private static final int MIN_TIME_ZONE_OFFSET = -(12 * 60 + 59);
    private static final int MONTHS_IN_QUARTER = 3;

    private DateTimeFunctions() {}

    @ScalarFunction("__to_time__")
    @SqlType(TimeType.class)
    public static long toTime(long time)
    {
        return time;
    }

    @ScalarFunction("__to_time_with_time_zone__")
    @SqlType(TimeWithTimeZoneType.class)
    public static long toTimeWithTimeZone(long timeWithTimeZone)
    {
        return timeWithTimeZone;
    }

    @ScalarFunction("__to_timestamp__")
    @SqlType(TimestampType.class)
    public static long toTimestamp(long timestamp)
    {
        return timestamp;
    }

    @ScalarFunction("__to_timestamp_with_time_zone__")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long toTimestampWithTimeZone(long timestampWithTimeZone)
    {
        return timestampWithTimeZone;
    }

    @ScalarFunction("__to_interval_day_time__")
    @SqlType(IntervalDayTimeType.class)
    public static long toIntervalDayTime(long intervalDayTime)
    {
        return intervalDayTime;
    }

    @ScalarFunction("__to_interval_year_month__")
    @SqlType(IntervalYearMonthType.class)
    public static long toIntervalYearMonth(long intervalYearMonth)
    {
        return intervalYearMonth;
    }

    @Description("current date")
    @ScalarFunction
    @SqlType(DateType.class)
    public static long currentDate(Session session)
    {
        // Stack value is the millisecond at midnight on the date in UTC so
        // no we do not want to correct to the local client time zone.  Client
        // time zone corrections are not needed for date.
        return UTC_CHRONOLOGY.dayOfMonth().roundFloor(session.getStartTime());
    }

    @Description("current time with time zone")
    @ScalarFunction
    @SqlType(TimeWithTimeZoneType.class)
    public static long currentTime(Session session)
    {
        // Stack value is number of milliseconds from start of the current day,
        // but the start of the day is relative to the current time zone.
        long millis = getChronology(session.getTimeZoneKey()).millisOfDay().get(session.getStartTime());
        return packDateTimeWithZone(millis, session.getTimeZoneKey());
    }

    @Description("current time without time zone")
    @ScalarFunction("localtime")
    @SqlType(TimeType.class)
    public static long localTime(Session session)
    {
        // Stack value is number of milliseconds from start of the current day,
        // but the start of the day is relative to the current time zone.
        return getChronology(session.getTimeZoneKey()).millisOfDay().get(session.getStartTime());
    }

    @Description("current timestamp with time zone")
    @ScalarFunction("now")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long currentTimestamp(Session session)
    {
        return packDateTimeWithZone(session.getStartTime(), session.getTimeZoneKey());
    }

    @Description("current timestamp without time zone")
    @ScalarFunction("localtimestamp")
    @SqlType(TimestampType.class)
    public static long localTimestamp(Session session)
    {
        return session.getStartTime();
    }

    @ScalarFunction
    @SqlType(TimeType.class)
    public static long millisToTime(Session session, long millis)
    {
        ISOChronology chronology = getChronology(session.getTimeZoneKey());
        int timeMillis = chronology.millisOfDay().get(millis) - chronology.getZone().getOffset(millis);
        return timeMillis;
    }

    @ScalarFunction
    @SqlType(TimeWithTimeZoneType.class)
    public static long millisToTime(long millis, long hoursOffset, long minutesOffset)
    {
        ISOChronology chronology = ISOChronology.getInstance(DateTimeZone.forOffsetHoursMinutes((int) hoursOffset, (int) minutesOffset));
        int timeMillis = chronology.millisOfDay().get(millis) - chronology.getZone().getOffset(millis);
        return packDateTimeWithZone((long) timeMillis, (int) (hoursOffset * 60 + minutesOffset));
    }

    @ScalarFunction("from_unixtime")
    @SqlType(TimestampType.class)
    public static long fromUnixTime(double unixTime)
    {
        return Math.round(unixTime * 1000);
    }

    @ScalarFunction("from_unixtime")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long fromUnixTime(double unixTime, long hoursOffset, long minutesOffset)
    {
        return packDateTimeWithZone(Math.round(unixTime * 1000), (int) (hoursOffset * 60 + minutesOffset));
    }

    @ScalarFunction("to_unixtime")
    public static double toUnixTime(@SqlType(TimestampType.class) long timestamp)
    {
        return timestamp / 1000.0;
    }

    @ScalarFunction("to_unixtime")
    public static double toUnixTimeFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return unpackMillisUtc(timestampWithTimeZone) / 1000.0;
    }

    @ScalarFunction("at_time_zone")
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeAtTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone, Slice zoneId)
    {
        return packDateTimeWithZone(unpackMillisUtc(timeWithTimeZone), zoneId.toStringUtf8());
    }

    @ScalarFunction("at_time_zone")
    @SqlType(TimeWithTimeZoneType.class)
    public static long timeAtTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone, @SqlType(IntervalDayTimeType.class) long zoneOffset)
    {
        if (zoneOffset % 60_000 != 0) {
            throw new IllegalArgumentException("Invalid time zone offset interval: interval contains seconds");
        }

        int zoneOffsetMinutes = (int) (zoneOffset / 60_000);
        return packDateTimeWithZone(unpackMillisUtc(timeWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @ScalarFunction("at_time_zone")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampAtTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone, Slice zoneId)
    {
        return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), zoneId.toStringUtf8());
    }

    @ScalarFunction("at_time_zone")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long timestampAtTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone, @SqlType(IntervalDayTimeType.class) long zoneOffset)
    {
        if (zoneOffset % 60_000 != 0) {
            throw new IllegalArgumentException("Invalid time zone offset interval: interval contains seconds");
        }

        int zoneOffsetMinutes = (int) (zoneOffset / 60_000);
        return packDateTimeWithZone(unpackMillisUtc(timestampWithTimeZone), getTimeZoneKeyForOffset(zoneOffsetMinutes));
    }

    @Description("add the specified amount of time to the given timestamp")
    @ScalarFunction
    @SqlType(TimestampType.class)
    public static long dateAdd(Session session, Slice unit, long value, @SqlType(TimestampType.class) long timestamp)
    {
        return dateAdd(getChronology(session.getTimeZoneKey()), unit, value, timestamp);
    }

    @Description("add the specified amount of time to the given timestamp")
    @ScalarFunction("date_add")
    @SqlType(TimestampWithTimeZoneType.class)
    public static long dateAddWithTimeZone(Slice unit, long value, @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        long millis = dateAdd(DateTimeZoneIndex.unpackChronology(timestampWithTimeZone), unit, value, unpackMillisUtc(timestampWithTimeZone));
        return updateMillisUtc(millis, timestampWithTimeZone);
    }

    private static long dateAdd(ISOChronology chronology, Slice unit, long value, long milliseconds)
    {
        String unitString = unit.toString(Charsets.US_ASCII).toLowerCase();
        int intValue = Ints.checkedCast(value);
        switch (unitString) {
            case "second":
                return chronology.secondOfMinute().add(milliseconds, intValue);
            case "minute":
                return chronology.minuteOfHour().add(milliseconds, intValue);
            case "hour":
                return chronology.hourOfDay().add(milliseconds, intValue);
            case "day":
                return chronology.dayOfMonth().add(milliseconds, intValue);
            case "week":
                return chronology.weekOfWeekyear().add(milliseconds, intValue);
            case "month":
                return chronology.monthOfYear().add(milliseconds, intValue);
            case "quarter":
                return chronology.monthOfYear().add(milliseconds, intValue * MONTHS_IN_QUARTER);
            case "year":
                return chronology.year().add(milliseconds, intValue);
            case "century":
                return chronology.centuryOfEra().add(milliseconds, intValue);
            default:
                throw new IllegalArgumentException("Unsupported unit " + unitString);
        }
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction
    public static long dateDiff(Session session, Slice unit, @SqlType(TimestampType.class) long timestamp1, @SqlType(TimestampType.class) long timestamp2)
    {
        return dateDiff(getChronology(session.getTimeZoneKey()), unit, timestamp1, timestamp2);
    }

    @Description("difference of the given times in the given unit")
    @ScalarFunction("date_diff")
    public static long dateDiffWithTimeZone(
            Slice unit,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone1,
            @SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone2)
    {
        return dateDiff(DateTimeZoneIndex.unpackChronology(timestampWithTimeZone1), unit, unpackMillisUtc(timestampWithTimeZone1), unpackMillisUtc(timestampWithTimeZone2));
    }

    public static long dateDiff(ISOChronology chronology, Slice unit, long timestamp1, long timestamp2)
    {
        String unitString = unit.toString(Charsets.US_ASCII).toLowerCase();

        switch (unitString) {
            case "second":
                return chronology.secondOfMinute().getDifference(timestamp2, timestamp1);
            case "minute":
                return chronology.minuteOfHour().getDifference(timestamp2, timestamp1);
            case "hour":
                return chronology.hourOfDay().getDifference(timestamp2, timestamp1);
            case "day":
                return chronology.dayOfMonth().getDifference(timestamp2, timestamp1);
            case "week":
                return chronology.weekOfWeekyear().getDifference(timestamp2, timestamp1);
            case "month":
                return chronology.monthOfYear().getDifference(timestamp2, timestamp1);
            case "quarter":
                // quarters are base 1 so we need to add 1
                return chronology.monthOfYear().getDifference(timestamp2, timestamp1) / 4 + 1;
            case "year":
                return chronology.year().getDifference(timestamp2, timestamp1);
            case "century":
                return chronology.centuryOfEra().getDifference(timestamp2, timestamp1);
            default:
                throw new IllegalArgumentException("Unsupported unit " + unitString);
        }
    }

    @Description("parses the specified date/time by the given format")
    @ScalarFunction
    @SqlType(TimestampWithTimeZoneType.class)
    public static long parseDatetime(Session session, Slice datetime, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withChronology(getChronology(session.getTimeZoneKey())).withOffsetParsed();

        String datetimeString = datetime.toString(Charsets.UTF_8);
        DateTime dateTime = formatter.parseDateTime(datetimeString);
        return packDateTimeWithZone(dateTime);
    }

    @Description("formats the given time by the given format")
    @ScalarFunction
    public static Slice formatDatetime(Session session, @SqlType(TimestampType.class) long timestamp, Slice formatString)
    {
        return formatDatetime(getChronology(session.getTimeZoneKey()), timestamp, formatString);
    }

    @Description("formats the given time by the given format")
    @ScalarFunction("format_datetime")
    public static Slice formatDatetimeWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone, Slice formatString)
    {
        return formatDatetime(DateTimeZoneIndex.unpackChronology(timestampWithTimeZone), unpackMillisUtc(timestampWithTimeZone), formatString);
    }

    private static Slice formatDatetime(ISOChronology chronology, long timestamp, Slice formatString)
    {
        String pattern = formatString.toString(Charsets.UTF_8);
        DateTimeFormatter formatter = DateTimeFormat.forPattern(pattern).withChronology(chronology);

        String datetimeString = formatter.print(timestamp);
        return Slices.wrappedBuffer(datetimeString.getBytes(Charsets.UTF_8));
    }

    @ScalarFunction
    public static Slice dateFormat(Session session, @SqlType(TimestampType.class) long timestamp, Slice formatString)
    {
        return dateFormat(getChronology(session.getTimeZoneKey()), timestamp, formatString);
    }

    @ScalarFunction("date_format")
    public static Slice dateFormatWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone, Slice formatString)
    {
        return dateFormat(DateTimeZoneIndex.unpackChronology(timestampWithTimeZone), unpackMillisUtc(timestampWithTimeZone), formatString);
    }

    private static Slice dateFormat(ISOChronology chronology, long timestamp, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString).withChronology(chronology);
        return Slices.copiedBuffer(formatter.print(timestamp), Charsets.UTF_8);
    }

    @ScalarFunction
    @SqlType(TimestampType.class)
    public static long dateParse(Session session, Slice dateTime, Slice formatString)
    {
        DateTimeFormatter formatter = DATETIME_FORMATTER_CACHE.get(formatString).withChronology(getChronology(session.getTimeZoneKey()));
        return formatter.parseMillis(dateTime.toString(Charsets.UTF_8));
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    public static long secondFromTimestamp(@SqlType(TimestampType.class) long timestamp)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(timestamp);
    }

    @Description("second of the minute of the given timestamp")
    @ScalarFunction("second")
    public static long secondFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    public static long secondFromTime(@SqlType(TimeType.class) long time)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(time);
    }

    @Description("second of the minute of the given time")
    @ScalarFunction("second")
    public static long secondFromTimeWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long time)
    {
        // Time is effectively UTC so no need for a custom chronology
        return SECOND_OF_MINUTE.get(unpackMillisUtc(time));
    }

    @Description("second of the minute of the given interval")
    @ScalarFunction("second")
    public static long secondFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_MINUTE) / MILLISECONDS_IN_SECOND;
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    public static long minuteFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(timestamp);
    }

    @Description("minute of the hour of the given timestamp")
    @ScalarFunction("minute")
    public static long minuteFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).minuteOfHour().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction("minute")
    public static long minuteFromTime(Session session, @SqlType(TimeType.class) long time)
    {
        return getChronology(session.getTimeZoneKey()).minuteOfHour().get(time);
    }

    @Description("minute of the hour of the given time")
    @ScalarFunction("minute")
    public static long minuteFromTimeWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timeWithTimeZone).minuteOfHour().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("minute of the hour of the given interval")
    @ScalarFunction("minute")
    public static long minuteFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_HOUR) / MILLISECONDS_IN_MINUTE;
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    public static long hourFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(timestamp);
    }

    @Description("hour of the day of the given timestamp")
    @ScalarFunction("hour")
    public static long hourFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).hourOfDay().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("hour of the day of the given time")
    @ScalarFunction("hour")
    public static long hourFromTime(Session session, @SqlType(TimeType.class) long time)
    {
        return getChronology(session.getTimeZoneKey()).hourOfDay().get(time);
    }

    @Description("hour of the day of the given time")
    @ScalarFunction("hour")
    public static long hourFromTimeWithTimeZone(@SqlType(TimeWithTimeZoneType.class) long timeWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timeWithTimeZone).hourOfDay().get(unpackMillisUtc(timeWithTimeZone));
    }

    @Description("hour of the day of the given interval")
    @ScalarFunction("hour")
    public static long hourFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return (milliseconds % MILLISECONDS_IN_DAY) / MILLISECONDS_IN_HOUR;
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    public static long dayOfWeekFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfWeek().get(timestamp);
    }

    @Description("day of the week of the given timestamp")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    public static long dayOfWeekFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).dayOfWeek().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the week of the given date")
    @ScalarFunction(value = "day_of_week", alias = "dow")
    public static long dayOfWeekFromDate(@SqlType(DateType.class) long date)
    {
        return DAY_OF_WEEK.get(date);
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    public static long dayFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfMonth().get(timestamp);
    }

    @Description("day of the month of the given timestamp")
    @ScalarFunction(value = "day", alias = "day_of_month")
    public static long dayFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).dayOfMonth().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the month of the given date")
    @ScalarFunction(value = "day", alias = "day_of_month")
    public static long dayFromDate(@SqlType(DateType.class) long date)
    {
        return DAY_OF_MONTH.get(date);
    }

    @Description("day of the month of the given interval")
    @ScalarFunction(value = "day", alias = "day_of_month")
    public static long dayFromInterval(@SqlType(IntervalDayTimeType.class) long milliseconds)
    {
        return milliseconds / MILLISECONDS_IN_DAY;
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    public static long dayOfYearFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).dayOfYear().get(timestamp);
    }

    @Description("day of the year of the given timestamp")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    public static long dayOfYearFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).dayOfYear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("day of the year of the given date")
    @ScalarFunction(value = "day_of_year", alias = "doy")
    public static long dayOfYearFromDate(@SqlType(DateType.class) long date)
    {
        return DAY_OF_YEAR.get(date);
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    public static long weekFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).weekOfWeekyear().get(timestamp);
    }

    @Description("week of the year of the given timestamp")
    @ScalarFunction(value = "week", alias = "week_of_year")
    public static long weekFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).weekOfWeekyear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("week of the year of the given date")
    @ScalarFunction(value = "week", alias = "week_of_year")
    public static long weekFromDate(@SqlType(DateType.class) long date)
    {
        return WEEK_OF_YEAR.get(date);
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    public static long monthFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).monthOfYear().get(timestamp);
    }

    @Description("month of the year of the given timestamp")
    @ScalarFunction("month")
    public static long monthFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).monthOfYear().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("month of the year of the given date")
    @ScalarFunction("month")
    public static long monthFromDate(@SqlType(DateType.class) long date)
    {
        return MONTH_OF_YEAR.get(date);
    }

    @Description("month of the year of the given interval")
    @ScalarFunction("month")
    public static long monthFromInterval(@SqlType(IntervalYearMonthType.class) long months)
    {
        return months % 12;
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    public static long quarterFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return (getChronology(session.getTimeZoneKey()).monthOfYear().get(timestamp) / 4) + 1;
    }

    @Description("quarter of the year of the given timestamp")
    @ScalarFunction("quarter")
    public static long quarterFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return (DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).monthOfYear().get(unpackMillisUtc(timestampWithTimeZone)) / 4) + 1;
    }

    @Description("quarter of the year of the given date")
    @ScalarFunction("quarter")
    public static long quarterFromDate(@SqlType(DateType.class) long date)
    {
        return (MONTH_OF_YEAR.get(date) / 4) + 1;
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    public static long yearFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).year().get(timestamp);
    }

    @Description("year of the given timestamp")
    @ScalarFunction("year")
    public static long yearFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).year().get(unpackMillisUtc(timestampWithTimeZone));
    }

    @Description("year of the given date")
    @ScalarFunction("year")
    public static long yearFromDate(@SqlType(DateType.class) long date)
    {
        return YEAR.get(date);
    }

    @Description("year of the given interval")
    @ScalarFunction("year")
    public static long yearFromInterval(@SqlType(IntervalYearMonthType.class) long months)
    {
        return months / 12;
    }

    @Description("century of the given timestamp")
    @ScalarFunction("century")
    public static long centuryFromTimestamp(Session session, @SqlType(TimestampType.class) long timestamp)
    {
        return getChronology(session.getTimeZoneKey()).centuryOfEra().get(timestamp) + 1;
    }

    @Description("century of the given timestamp")
    @ScalarFunction("century")
    public static long centuryFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return DateTimeZoneIndex.unpackChronology(timestampWithTimeZone).centuryOfEra().get(unpackMillisUtc(timestampWithTimeZone)) + 1;
    }

    @Description("century of the given date")
    @ScalarFunction("century")
    public static long centuryFromDate(@SqlType(DateType.class) long date)
    {
        return CENTURY.get(date) + 1;
    }

    @Description("time zone minute of the given timestamp")
    @ScalarFunction("timezone_minute")
    public static long timeZoneMinuteFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) % 60;
    }

    @Description("time zone hour of the given timestamp")
    @ScalarFunction("timezone_hour")
    public static long timeZoneHourFromTimestampWithTimeZone(@SqlType(TimestampWithTimeZoneType.class) long timestampWithTimeZone)
    {
        return extractZoneOffsetMinutes(timestampWithTimeZone) / 60;
    }

    @SuppressWarnings("fallthrough")
    public static DateTimeFormatter createDateTimeFormatter(Slice format)
    {
        DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder();

        String formatString = format.toString(Charsets.UTF_8);
        boolean escaped = false;
        for (int i = 0; i < format.length(); i++) {
            char character = formatString.charAt(i);

            if (escaped) {
                switch (character) {
                    case 'a': // %a Abbreviated weekday name (Sun..Sat)
                        builder.appendDayOfWeekShortText();
                        break;
                    case 'b': // %b Abbreviated month name (Jan..Dec)
                        builder.appendMonthOfYearShortText();
                        break;
                    case 'c': // %c Month, numeric (0..12)
                        builder.appendMonthOfYear(1);
                        break;
                    case 'd': // %d Day of the month, numeric (00..31)
                        builder.appendDayOfMonth(2);
                        break;
                    case 'e': // %e Day of the month, numeric (0..31)
                        builder.appendDayOfMonth(1);
                        break;
                    case 'f': // %f Microseconds (000000..999999)
                        builder.appendMillisOfSecond(6);
                        break;
                    case 'H': // %H Hour (00..23)
                        builder.appendHourOfDay(2);
                        break;
                    case 'h': // %h Hour (01..12)
                    case 'I': // %I Hour (01..12)
                        builder.appendClockhourOfHalfday(2);
                        break;
                    case 'i': // %i Minutes, numeric (00..59)
                        builder.appendMinuteOfHour(2);
                        break;
                    case 'j': // %j Day of year (001..366)
                        builder.appendDayOfYear(3);
                        break;
                    case 'k': // %k Hour (0..23)
                        builder.appendClockhourOfDay(1);
                        break;
                    case 'l': // %l Hour (1..12)
                        builder.appendClockhourOfHalfday(1);
                        break;
                    case 'M': // %M Month name (January..December)
                        builder.appendMonthOfYearText();
                        break;
                    case 'm': // %m Month, numeric (00..12)
                        builder.appendMonthOfYear(2);
                        break;
                    case 'p': // %p AM or PM
                        builder.appendHalfdayOfDayText();
                        break;
                    case 'r': // %r Time, 12-hour (hh:mm:ss followed by AM or PM)
                        builder.appendClockhourOfHalfday(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2)
                                .appendLiteral(' ')
                                .appendHalfdayOfDayText();
                        break;
                    case 'S': // %S Seconds (00..59)
                    case 's': // %s Seconds (00..59)
                        builder.appendSecondOfMinute(2);
                        break;
                    case 'T': // %T Time, 24-hour (hh:mm:ss)
                        builder.appendHourOfDay(2)
                                .appendLiteral(':')
                                .appendMinuteOfHour(2)
                                .appendLiteral(':')
                                .appendSecondOfMinute(2);
                        break;
                    case 'v': // %v Week (01..53), where Monday is the first day of the week; used with %x
                        builder.appendWeekOfWeekyear(2);
                        break;
                    case 'W': // %W Weekday name (Sunday..Saturday)
                        builder.appendDayOfWeekText();
                        break;
                    case 'w': // %w Day of the week (0=Sunday..6=Saturday)
                        builder.appendDayOfWeek(1);
                        break;
                    case 'Y': // %Y Year, numeric, four digits
                        builder.appendYear(4, 4);
                        break;
                    case 'y': // %y Year, numeric (two digits)
                        builder.appendYearOfCentury(2, 2);
                        break;
                    case 'U': // %U Week (00..53), where Sunday is the first day of the week
                    case 'u': // %u Week (00..53), where Monday is the first day of the week
                    case 'V': // %V Week (01..53), where Sunday is the first day of the week; used with %X
                    case 'X': // %X Year for the week where Sunday is the first day of the week, numeric, four digits; used with %V
                    case 'x': // %x Year for the week, where Monday is the first day of the week, numeric, four digits; used with %v
                    case 'D': // %D Day of the month with English suffix (0th, 1st, 2nd, 3rd, …)
                        throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT.toErrorCode(), String.format("%%%s not supported in date format string", character));
                    case '%': // %% A literal “%” character
                        builder.appendLiteral('%');
                        break;
                    default: // %<x> The literal character represented by <x>
                        builder.appendLiteral(character);
                        break;
                }
                escaped = false;
            }
            else if (character == '%') {
                escaped = true;
            }
            else {
                builder.appendLiteral(character);
            }
        }

        try {
            return builder.toFormatter();
        }
        catch (UnsupportedOperationException e) {
            throw new PrestoException(StandardErrorCode.INVALID_FUNCTION_ARGUMENT.toErrorCode(), e);
        }
    }
}

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2003 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package net.sf.farrago.type.runtime;

import java.sql.*;

import java.text.*;

import java.util.Calendar;
import java.util.TimeZone;

import net.sf.farrago.resource.*;

import org.eigenbase.resource.*;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.util.Util;
import org.eigenbase.util14.*;


/**
 * Runtime type for basic date/time/timestamp values without time zone
 * information. All of these types are represented by subclasses of
 * {@link ZonelessDatetime} and have a similar internal representation.
 * This class interoperates with java.sql (Jdbc) types since they are
 * commonly used for external data.
 *
 * TODO: we can probably be smarter about how we allocate Java objects
 * TODO: precision and milliseconds for TIME and TIMESTAMP
 *
 * @author lee
 * @version $Id$
 * @since May 5, 2004
 */
public abstract class SqlDateTimeWithoutTZ
    implements AssignableValue, SpecialDataValue
{

    // ~ Static fields --------------------------------------------------------
    // Use same format as supported by parser (should be ISO format)
    public static final String DateFormatStr = DateTimeUtil.DateFormatStr;
    public static final String TimeFormatStr = DateTimeUtil.TimeFormatStr;
    public static final String TimestampFormatStr =
        DateTimeUtil.TimestampFormatStr;
    private static final TimeZone gmtZone = DateTimeUtil.gmtZone;
    private static final TimeZone defaultZone = DateTimeUtil.defaultZone;

    public static final String INTERNAL_TIME_FIELD_NAME = "internalTime";

    //~ Instance fields --------------------------------------------------------

    /**
     * Calendar, which holds the client time zone. It defaults to null,
     * which implies that no explicit time zone has been set.
     */
    private Calendar cal;

    /**
     *  The calendar to use as a temporary variable. This calendar's time
     *  zone is set to the value time zone.
     */
    private Calendar tempCal;

    /**
     * The raw value of this SqlDateTimeWithoutTZ
     */
    public ZonelessDatetime value;

    /**
     * Whether this value is null.
     */
    public boolean isNull;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a runtime object
     */
    public SqlDateTimeWithoutTZ()
    {
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return long.class
     */
    public static Class getPrimitiveClass()
    {
        return long.class;
    }

    /**
     * Returns the Date/Time format to use
     *
     * @return String representing the default Date/Time format to use
     */
    // REVIEW jpham 2006-09-27: since this is protected, I'm assuming noone
    // needs it
    // protected abstract void parse(String date, String format, TimeZone tz);

    /**
     * Gets data, casted as a Jdbc value.
     */
    protected abstract Object getJdbcValue();

    /**
     * (Optionally) implements NullableValue
     */
    public void setNull(boolean b)
    {
        isNull = b;
    }

    /**
     * Per the {@link NullableValue} contract, returns either null or
     * the value of this object as a Jdbc compatible value. The Jdbc value
     * is constructed relative to the server default time zone.
     */
    public Object getNullableData()
    {
        if (isNull()) {
            return null;
        }
        return getJdbcValue();
    }

    /**
     * Return data to result sets as ZonelessDatetime so that it may be
     * properly localized by a Jdbc driver or client application.
     */
    public Object getSpecialData()
    {
        if (isNull()) {
            return null;
        }
        return value;
    }

    /**
     * @return whether the value has been set to null
     */
    public boolean isNull()
    {
        return isNull;
    }

    /**
     * Assigns a value from another object.
     *
     * <p>The Object may be a {@link Long} or <code>long</code> if it is being
     * intialized from a
     * constant, or being translated from a Fennel value. If so, then the
     * Fennel type must match the Farrago type. It is legal to assign a
     * {@link ZonelessDate} to a {@link ZonelessDate} and a
     * {@link ZonelessTime} to a {@link ZonelessTime], but it is not valid
     * to assign a Timestamp to either, or vice versa.
     *
     * @param date value to assign, or null to set null
     */
    public void assignFrom(Object date)
    {
        if (date == null) {
            setNull(true);
            return;
        }
        setNull(false);
        if (date instanceof Long) {
            assignFrom(((Long) date).longValue());
        } else if (date instanceof ZonelessDatetime) {
            value.setZonelessTime(((ZonelessDatetime) date).getTime());
        } else if (date instanceof java.util.Date) {
            value.setZonedTime(
                ((java.util.Date) date).getTime(), defaultZone);
        } else if (date instanceof SqlDateTimeWithoutTZ) {
            SqlDateTimeWithoutTZ sqlDate = (SqlDateTimeWithoutTZ) date;
            isNull = sqlDate.isNull;
            if (isNull) {
                return;
            }
            value.setZonelessTime(sqlDate.value.getTime());
            // assuming we preserve Calendar of this object
        } else if (date instanceof String) {
            attemptParse((String) date);
        } else {
            // REVIEW jvs 27-Aug-2004:  this is dangerous; should probably
            // require a specific interface instead
            String s = date.toString();
            if (s == null) {
                isNull = true;
                return;
            }
            attemptParse(s);
            return;
        }
    }

    /**
     * Assigns a value from another object.
     *
     * @see #assignFrom(Object)
     */
    public void assignFrom(long l)
    {
        setNull(false);
        value.setZonelessTime(l);
        // cal is synchronized during getCal and setCal
    }

    /**
     * Attempts to parse the string, throwing an understandable exception
     * if an error was detected.
     */
    private void attemptParse(String s) {
        try {
            assignFromString(s.trim());
        } catch (IllegalArgumentException ex) {
            String reason =
                EigenbaseResource.instance().BadFormat.str(getFormat());

            throw FarragoResource.instance().AssignFromFailed.ex(
                s,
                getTypeName(),
                reason);
        }
    }

    /**
     * Assigns the value from a string.
     */
    protected abstract void assignFromString(String s);

    /**
     * Gets a calendar with the time and time zone of this value.
     * The calendar returned is not the internal calendar of this
     * SqlDateTimeWithoutTZ.
     *
     * TODO: does anyone use this? Currently it returns a copy of a
     *   Calendar with the value time zone and the milliseconds.
     *
     * @deprecated please review this code
     */
    public Calendar getCal()
    {
        Calendar ret = Calendar.getInstance(getValueTimeZone());
        ret.setTimeInMillis(value.getTime());
        return ret;
    }

    /**
     * Assigns the time and time zone from a Calendar value.
     *
     * TODO: Does anyone use this? If so, we want to be careful about the
     *   meaning of this.cal. Elsewhere we use it for "client time zone"
     *   but here we seem to be using it to mean "value time zone". Or if we
     *   indeed mean "client time zone", then we should not be setting the
     *   milliseconds time value.
     *
     * @param cal calendar value to assign from
     *
     * @deprecated please review this code
     */
    public void setCal(Calendar cal)
    {
        if (cal == null) {
            setNull(true);
            return;
        }
        setNull(false);
        this.cal = cal;
        value.setZonelessTime(cal.getTimeInMillis());
    }

    // TODO jvs 26-July-2004:  In order to support fractional seconds,
    // need to remember precision and use it in formatting.  Unfortunately,
    // SimpleDateFormat doesn't handle this, so we'll need to use
    // DecimalFormat for the fractional part.

    /**
     * Returns a string in the specified datetime format
     * TODO: does anyone use this?
     *
     * @deprecated please review this code
     */
    public String toString(String format)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        dateFormat.setTimeZone(getValueTimeZone());
        return dateFormat.format(new java.util.Date(value.getTime()));
    }

    /**
     * Returns the client time zone, as indicated by Calendar
     */
    protected TimeZone getClientTimeZone()
    {
        if (cal == null) {
            return defaultZone;
        }
        return cal.getTimeZone();
    }

    /**
     * Returns the time zone used to store the value
     */
    protected TimeZone getValueTimeZone()
    {
        return gmtZone;
    }

    /**
     * Returns a string in default format representing the datetime
     */
    public String toString()
    {
        return value.toString();
    }

    /**
     * Returns the format string for this type
     */
    protected abstract String getFormat();

    /**
     * Returns the name of this type, DATE, TIME, or TIMESTAMP
     */
    protected abstract String getTypeName();

    public void floor(int timeUnitOrdinal)
    {
        Calendar cal = getTempCal();
        switch (timeUnitOrdinal) {
        // Fall through
        case SqlIntervalQualifier.TimeUnit.Year_ordinal:
            cal.set(Calendar.MONTH, 0);
        case SqlIntervalQualifier.TimeUnit.Month_ordinal:
            cal.set(Calendar.DAY_OF_MONTH, 1);
        case SqlIntervalQualifier.TimeUnit.Day_ordinal:
            cal.set(Calendar.HOUR_OF_DAY, 0);
        case SqlIntervalQualifier.TimeUnit.Hour_ordinal:
            cal.set(Calendar.MINUTE, 0);
        case SqlIntervalQualifier.TimeUnit.Minute_ordinal:
            cal.set(Calendar.SECOND, 0);
        case SqlIntervalQualifier.TimeUnit.Second_ordinal:
            cal.set(Calendar.MILLISECOND, 0);
            break;
        default:
            throw Util.newInternal("Invalid timeunit " + timeUnitOrdinal);
        }
        value.setZonelessTime(cal.getTimeInMillis());
    }

    public void ceil(int timeUnitOrdinal)
    {
        Calendar cal = getTempCal();
        boolean incNeeded = false;
        switch (timeUnitOrdinal) {
        // Fall through
        case SqlIntervalQualifier.TimeUnit.Year_ordinal:
            if (cal.get(Calendar.MONTH) >= 0) {
                cal.set(Calendar.MONTH, 0);
                incNeeded = true;
            }
        case SqlIntervalQualifier.TimeUnit.Month_ordinal:
            if (cal.get(Calendar.DAY_OF_MONTH) > 0) {
                cal.set(Calendar.DAY_OF_MONTH, 1);
                incNeeded = true;
            }
        case SqlIntervalQualifier.TimeUnit.Day_ordinal:
            if (cal.get(Calendar.HOUR_OF_DAY) > 0) {
                cal.set(Calendar.HOUR_OF_DAY, 0);
                incNeeded = true;
            }
        case SqlIntervalQualifier.TimeUnit.Hour_ordinal:
            if (cal.get(Calendar.MINUTE) > 0) {
                cal.set(Calendar.MINUTE, 0);
                incNeeded = true;
            }
        case SqlIntervalQualifier.TimeUnit.Minute_ordinal:
            if (cal.get(Calendar.SECOND) > 0) {
                cal.set(Calendar.SECOND, 0);
                incNeeded = true;
            }
        case SqlIntervalQualifier.TimeUnit.Second_ordinal:
            if (cal.get(Calendar.MILLISECOND) > 0) {
                cal.set(Calendar.MILLISECOND, 0);
                incNeeded = true;
            }
            break;
        default:
            throw Util.newInternal("Invalid timeunit " + timeUnitOrdinal);
        }

        if (incNeeded) {
            switch (timeUnitOrdinal) {
            case SqlIntervalQualifier.TimeUnit.Year_ordinal:
                cal.add(Calendar.YEAR, 1);
                break;
            case SqlIntervalQualifier.TimeUnit.Month_ordinal:
                cal.add(Calendar.MONTH, 1);
                break;
            case SqlIntervalQualifier.TimeUnit.Day_ordinal:
                cal.add(Calendar.DAY_OF_MONTH, 1);
                break;
            case SqlIntervalQualifier.TimeUnit.Hour_ordinal:
                cal.add(Calendar.HOUR_OF_DAY, 1);
                break;
            case SqlIntervalQualifier.TimeUnit.Minute_ordinal:
                cal.add(Calendar.MINUTE, 1);
                break;
            case SqlIntervalQualifier.TimeUnit.Second_ordinal:
                cal.add(Calendar.SECOND, 1);
                break;
            default:
                throw Util.newInternal("Invalid timeunit " + timeUnitOrdinal);
            }
            value.setZonelessTime(cal.getTimeInMillis());
        }
    }

    /**
     * Gets a temporary calendar object, initialized with this object's time
     * zone and milliseconds value.
     */
    protected Calendar getTempCal()
    {
        if (tempCal == null) {
            tempCal = Calendar.getInstance(getValueTimeZone());
        }
        tempCal.setTimeInMillis(value.getTime());
        return tempCal;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * SQL date value. The value field of this object represents milliseconds
     * of a FarragoDate.
     */
    public static class SqlDate
        extends SqlDateTimeWithoutTZ
    {
        /**
         * Constructs a new SqlDate
         */
        public SqlDate()
        {
            value = new ZonelessDate();
        }

        // implement SqlDateTimeWithoutTZ
        protected Object getJdbcValue()
        {
            return new Date(value.getJdbcDate(defaultZone));
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromString(String s)
        {
            ZonelessDate date = ZonelessDate.parse(s);
            if (date == null) {
                throw new IllegalArgumentException();
            }
            value = date;
        }

        // implement SqlDateTimeWithoutTZ
        protected String getFormat()
        {
            return DateFormatStr;
        }

        // implement SqlDateTimeWithoutTZ
        protected String getTypeName()
        {
            return "DATE";
        }
    }

    /**
     * SQL time value.
     */
    public static class SqlTime
        extends SqlDateTimeWithoutTZ
    {
        /**
         * Constructs a new SqlDate
         */
        public SqlTime()
        {
            value = new ZonelessTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected Object getJdbcValue()
        {
            return new Time(value.getJdbcTime(defaultZone));
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromString(String s)
        {
            ZonelessTime time = ZonelessTime.parse(s);
            if (time == null) {
                throw new IllegalArgumentException();
            }
            value = time;
        }

        // implement SqlDateTimeWithoutTZ
        protected String getFormat()
        {
            return TimeFormatStr;
        }

        // implement SqlDateTimeWithoutTZ
        protected String getTypeName()
        {
            return "TIME";
        }
    }

    /**
     * SQL timestamp value.
     */
    public static class SqlTimestamp
        extends SqlDateTimeWithoutTZ
    {
        /**
         * The current date as returned by the current_date context variable
         */
        protected ZonelessDate currentDate;

        /**
         * Constructs a SqlTimestamp.
         */
        public SqlTimestamp()
        {
            value = new ZonelessTimestamp();
        }

        /**
         * Sets the current date for use by time to timestamp conversion
         *
         * @param date the value of the current_date context variable
         */
        public void setCurrentDate(SqlDateTimeWithoutTZ date)
        {
            if (currentDate != null
                && date.value.getTime() == currentDate.getTime())
            {
                return;
            }
            currentDate = new ZonelessDate();
            currentDate.setZonelessTime(date.value.getTime());
        }

        // override SqlDateTimeWithoutTZ
        public void assignFrom(Object o)
        {
            if (o instanceof SqlTime) {
                assert (currentDate != null);
                SqlTime time = (SqlTime) o;
                if (time.isNull()) {
                    setNull(true);
                } else {
                    setNull(false);
                    value.setZonelessTime(
                        currentDate.getTime() + time.value.getTime());
                }
            } else {
                super.assignFrom(o);
            }
        }

        // implement SqlDateTimeWithoutTZ
        protected Object getJdbcValue()
        {
            return new Timestamp(value.getJdbcTimestamp(defaultZone));
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromString(String s)
        {
            ZonelessTimestamp parsedDate = ZonelessTimestamp.parse(s);
            if (parsedDate == null) {
                throw new IllegalArgumentException();
            }
            value = parsedDate;
        }

        // implement SqlDateTimeWithoutTZ
        protected String getFormat()
        {
            return TimestampFormatStr;
        }

        // implement SqlDateTimeWithoutTZ
        protected String getTypeName()
        {
            return "TIMESTAMP";
        }
    }
}

// End SqlDateTimeWithoutTZ.java

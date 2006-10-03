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
import org.eigenbase.sql.parser.*;
import org.eigenbase.sql.SqlIntervalQualifier;
import org.eigenbase.util.Util;
import org.eigenbase.util14.*;


/**
 * Runtime type for date/time/timestamp values. We represent all types in 
 * a canonical way (as milliseconds since the epoch in GMT). For timestamps 
 * this is identical to the java.sql convention, so we can use java.sql 
 * timestamps directly. Unfortunately, for dates and times, java.sql does 
 * not have a canonical convention (dates and times are returned relative to 
 * the default time zone). So, for dates and times, Farrago follows its own 
 * convention, and represents dates and times relative to GMT.
 *
 * <p>
 * 
 * Although all types are represented in GMT time, it is a bit tricky, 
 * because timestamps are generally interpreted with repect to the 
 * default time zone, while we have to interpret our dates and times with 
 * respect to the GMT time zone. (All java.sql types are usually interpreted 
 * with respect to the default time zone.)
 * 
 * <p>
 * 
 * To make this work, we avoid assigning timestamps to dates/times without 
 * conversion, and vice versa. We keep track of whether we are dealing with 
 * a Farrago type or a java.sql type. (It would seem safer to avoid java.sql 
 * types, but since most external data comes as java.sql data, we also want 
 * to support it here.)
 *
 * TODO: we can probably be smarter about how we allocate Java objects
 * TODO: precision and milliseconds for TIME and TIMESTAMP
 *
 * @author lee
 * @version $Id$
 * @since May 5, 2004
 */
public abstract class SqlDateTimeWithoutTZ
    implements AssignableValue
{

    // ~ Static fields --------------------------------------------------------
    // Use same format as supported by parser (should be ISO format)
    public static final String DateFormatStr = SqlParserUtil.DateFormatStr;
    public static final String TimeFormatStr = SqlParserUtil.TimeFormatStr;
    public static final String TimestampFormatStr =
        SqlParserUtil.TimestampFormatStr;
    private static final TimeZone gmtZone = TimeZone.getTimeZone("GMT+0");

    /**
     * The default timezone for this Java VM.
     */
    private static final TimeZone defaultZone =
        Calendar.getInstance().getTimeZone();

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
     * Time as milliseconds since epoch (1 Jan 1970 GMT). Date and Time is 
     * valid with respect to the GMT time zone only.
     */
    public long value;

    /**
     * Whether this value is null.
     */
    public boolean isNull;

    /**
     * The current date as returned by the current_date context variable
     */
    protected Date currentDate;
    // For caching
    private long lastCurrentDate;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a runtime object with timezone offset set from localtime.
     *
     * <p>FIXME - we need the session tz, not the server tz.
     */
    public SqlDateTimeWithoutTZ()
    {
        value = 0;
        isNull = false;
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
    public abstract Object getData(long millisecond);

    /**
     * (Optionally) implements NullableValue
     */
    public void setNull(boolean b)
    {
        isNull = b;
    }

    /**
     * Per the {@link NullableValue} contract, returns either null or the
     * embedded value object. The value object is a {@link java.sql.Time JDBC
     * Time/Date/Timestamp} in local time.
     */
    public Object getNullableData()
    {
        if (isNull()) {
            return null;
        }

        return this.getData(value);
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
     * <p>The Object may be a Long or long if it is being intialized from a 
     * constant, or being translated from a Fennel value. If so, then the 
     * Fennel type must match the Farrago type. It is legal to assign a 
     * GmtDate to a GmtDate and a GmtTime to a GmtTime, but it is not valid 
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
        } else if (date instanceof GmtDate) {
            assignFromDate((GmtDate) date);
        } else if (date instanceof GmtTime) {
            assignFromTime((GmtTime) date);
        } else if (date instanceof java.util.Date) {
            assignFromJdbc((java.util.Date) date);
        } else if (date instanceof SqlDateTimeWithoutTZ) {
            SqlDateTimeWithoutTZ sqlDate = (SqlDateTimeWithoutTZ) date;
            isNull = sqlDate.isNull;
            if (isNull) {
                return;
            }
            assignFromRuntime(sqlDate);
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
     * @see assignFrom(Object)
     */
    public void assignFrom(long l)
    {
        setNull(false);
        value = l;
        // cal is synchronized during getCal and setCal
    }

    protected abstract void assignFromDate(GmtDate date);
    protected abstract void assignFromTime(GmtTime time);
    protected abstract void assignFromJdbc(java.util.Date date);
    protected abstract void assignFromRuntime(SqlDateTimeWithoutTZ date);

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
        ret.setTimeInMillis(value);
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
        value = cal.getTimeInMillis();
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
        return dateFormat.format(new java.util.Date(value));
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
    public abstract String toString();

    /**
     * Returns the format string for this type
     */
    protected abstract String getFormat();

    /**
     * Returns the name of this type, DATE, TIME, or TIMESTAMP
     */
    protected abstract String getTypeName();

    /**
     * Sets the current date for use by time to timestamp conversion
     * 
     * @param date the value of the current_date context variable
     */
    public void setCurrentDate(SqlDateTimeWithoutTZ date)
    {
        if (date.value == lastCurrentDate) {
            return;
        }
        GmtDate fd = new GmtDate(date.value);
        currentDate = 
            ConversionUtil.gmtToJdbcDate(fd, getClientTimeZone());
        lastCurrentDate = date.value;
    }

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
                Util.permAssert(false, "Invalid timeunit " + timeUnitOrdinal);
        }
        value = cal.getTimeInMillis();
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
                Util.permAssert(false, "Invalid timeunit " + timeUnitOrdinal);
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
                    Util.permAssert(false, "Invalid timeunit " + timeUnitOrdinal);
            }
            value = cal.getTimeInMillis();
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
        tempCal.setTimeInMillis(value);
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
        // override SqlDateTimeWithoutTZ
        public void assignFrom(Object o)
        {
            super.assignFrom(o);
            // clear the time component
            floor(SqlIntervalQualifier.TimeUnit.Day_ordinal);
        }

        // override SqlDateTimeWithoutTZ
        public void assignFrom(long l)
        {
            super.assignFrom(l);
            // clear the time component
            floor(SqlIntervalQualifier.TimeUnit.Day_ordinal);
        }

        // implement SqlDateTimeWithoutTZ
        public Object getData(long millisecond)
        {
            GmtDate date = new GmtDate(millisecond);
            TimeZone zone = getClientTimeZone();
            return ConversionUtil.gmtToJdbcDate(date, zone);
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromDate(GmtDate date)
        {
            value = date.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromTime(GmtTime time)
        {
            Util.permAssert(
                false, "SqlDateTimeWithoutTZ: cannot assign date from time");
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromJdbc(java.util.Date date)
        {
            TimeZone zone = getClientTimeZone();
            GmtDate fd = ConversionUtil.jdbcToGmtDate(date, zone);
            value = fd.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromRuntime(SqlDateTimeWithoutTZ date)
        {
            if (date instanceof SqlTimestamp) {
                assignFromJdbc(new Date(date.value));
            } else {
                value = date.value;
            }
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromString(String s)
        {
            GmtDate date = GmtDate.parseGmt(s);
            if (date == null) {
                throw new IllegalArgumentException();
            }
            value = date.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        public String toString()
        {
            GmtDate date = new GmtDate(value);
            return date.toString();
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
        // override SqlDateTimeWithoutTZ
        public void assignFrom(Object o)
        {
            super.assignFrom(o);
            clearDayComponent();
        }

        // override SqlDateTimeWithoutTZ
        public void assignFrom(long l)
        {
            super.assignFrom(l);
            clearDayComponent();
        }

        /**
         * Clears the day component of the current time value
         */
        private void clearDayComponent()
        {
            Calendar cal = getTempCal();
            cal.set(Calendar.YEAR, 1970);
            cal.set(Calendar.DAY_OF_YEAR, 1);
            value = cal.getTimeInMillis();
        }

        // implement SqlDateTimeWithoutTZ
        public Object getData(long millisecond)
        {
            GmtTime time = new GmtTime(millisecond);
            TimeZone zone = getClientTimeZone();
            return ConversionUtil.gmtToJdbcTime(time, zone);
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromDate(GmtDate date)
        {
            Util.permAssert(
                false, "SqlDateTimeWithoutTZ: cannot assign time from date");
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromTime(GmtTime time)
        {
            value = time.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromJdbc(java.util.Date date)
        {
            TimeZone zone = getClientTimeZone();
            GmtTime fd = ConversionUtil.jdbcToGmtTime(date, zone);
            value = fd.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromRuntime(SqlDateTimeWithoutTZ date)
        {
            if (date instanceof SqlTimestamp) {
                assignFromJdbc(new Date(date.value));
            } else {
                value = date.value;
            }
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromString(String s)
        {
            GmtTime time = GmtTime.parseGmt(s);
            if (time == null) {
                throw new IllegalArgumentException();
            }
            value = time.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        public String toString()
        {
            GmtTime time = new GmtTime(value);
            return time.toString();
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
        // override SqlDateTimeWithoutTZ
        protected TimeZone getValueTimeZone()
        {
            return getClientTimeZone();
        }

        // implement SqlDateTimeWithoutTZ
        public Object getData(long millisecond)
        {
            return new Timestamp(millisecond);
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromDate(GmtDate date)
        {
            TimeZone zone = getClientTimeZone();
            Date jdbcDate = ConversionUtil.gmtToJdbcDate(date, zone);
            value = jdbcDate.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromTime(GmtTime time)
        {
            assert (currentDate != null);
            value = currentDate.getTime() + time.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromJdbc(java.util.Date date)
        {
            value = date.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromRuntime(SqlDateTimeWithoutTZ date)
        {
            if (date instanceof SqlTimestamp) {
                value = date.value;
            } else if (date instanceof SqlDate) {
                assignFromDate(new GmtDate(date.value));
            } else {
                assignFromTime(new GmtTime(date.value));
            }
        }

        // implement SqlDateTimeWithoutTZ
        protected void assignFromString(String s)
        {
            TimeZone zone = getClientTimeZone();
            Timestamp parsedDate = DateTimeUtil.parseTimestamp(s, zone);
            if (parsedDate == null) {
                throw new IllegalArgumentException();
            }
            value = parsedDate.getTime();
        }

        // implement SqlDateTimeWithoutTZ
        public String toString()
        {
            SimpleDateFormat sdf = new SimpleDateFormat(getFormat());
            sdf.setTimeZone(getClientTimeZone());
            return sdf.format(new java.util.Date(value));
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

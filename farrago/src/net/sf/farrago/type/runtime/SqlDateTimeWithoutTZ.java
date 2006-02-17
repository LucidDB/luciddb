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

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

import net.sf.farrago.resource.FarragoResource;

import org.eigenbase.resource.*;
import org.eigenbase.sql.parser.SqlParserUtil;


/**
 * Runtime type for date/time/timestamp values.
 *
 * <p>This is a bit ugly, due to the unfortunate nature of sql 99 time without
 * Timezone.
 *
 * <p>The localtime is always the same hours/minutes in the current timezone,
 * so GMT has to be offset accordingly.
 *
 * @author lee
 * @since May 5, 2004
 * @version $Id$
 **/
public abstract class SqlDateTimeWithoutTZ implements AssignableValue
{
    //~ Static fields/initializers --------------------------------------------

    // ~ Static fields --------------------------------------------------------
    // Use same format as supported by parser (should be ISO format)
    public static final String DateFormatStr = SqlParserUtil.DateFormatStr;
    public static final String TimeFormatStr = SqlParserUtil.TimeFormatStr;
    public static final String TimestampFormatStr =
        SqlParserUtil.TimestampFormatStr;
    private static final TimeZone gmtZone = TimeZone.getTimeZone("GMT+0");

    /** The default timezone for this Java VM. */
    private static final TimeZone defaultZone =
        Calendar.getInstance().getTimeZone();

    //~ Instance fields -------------------------------------------------------

    /**
     * Calendar, which holds this object's time value. Its timezone is GMT,
     * unless you call {@link #setCal}.
     */
    private Calendar cal = Calendar.getInstance();

    /**
     * Time as milliseconds since epoch (1 Jan 1970 GMT).
     */
    public long value = 0;
    public int timeZoneOffset = 0; // timezone offset in Millis.

    /** Whether this value is null. */
    public boolean isNull;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a runtime object with timezone offset set from localtime.
     *
     * <p>FIXME    - we need the session tz, not the server tz.
     */
    public SqlDateTimeWithoutTZ()
    {
        timeZoneOffset = defaultZone.getRawOffset();
        cal.setTimeZone(gmtZone);
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * @return long.class
     */
    public static Class getPrimitiveClass()
    {
        return long.class;
    }

    /**
     * Returns the Date/Time format to use
      * @return String representing the default Date/Time format to use
     */
    protected abstract String getFormat();
    protected abstract void parse(String date, String format, TimeZone tz);

    public abstract Object getData(long millisecond);

    // (optionally) implement NullableValue
    public void setNull(boolean b)
    {
        isNull = b;
    }

    /**
     * Per the {@link NullableValue} contract, returns either null or the
     * embedded value object. The value object is a {@link java.sql.Time
     * JDBC Time/Date/Timestamp} in local time.
     */
    public Object getNullableData()
    {
        if (isNull() || (getCal() == null)) {
            return null;
        }

        // subtract timeZoneOffset to get localtime == GMT time.
        long millis = cal.getTimeInMillis();
        timeZoneOffset = defaultZone.getOffset(millis);
        return this.getData(millis - timeZoneOffset);
    }

    /**
     * @return whether the value has been set to null
     */
    public boolean isNull()
    {
        return isNull;
    }

    /**
     * Assigns value from an Object.
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
            value = ((Long) date).longValue();
            return;
        } else if (date instanceof java.util.Date) {
            value = ((java.util.Date) date).getTime(); // + timeZoneOffset; // set tzOffset?
            return;
        } else if (date instanceof SqlDateTimeWithoutTZ) {
            SqlDateTimeWithoutTZ sqlDate = (SqlDateTimeWithoutTZ) date;
            this.timeZoneOffset = sqlDate.timeZoneOffset;
            this.value = sqlDate.value;
            this.isNull = sqlDate.isNull;
            return;
        } else if (date instanceof String) {
            String dateStr = (String) date;
            String format = getFormat();
            parse(dateStr, format, null);
        } else {
            // REVIEW jvs 27-Aug-2004:  this is dangerous; should probably
            // require a specific interface instead
            String s = date.toString();
            assignFrom(s);
            return;
        }
    }

    public void assignFrom(long l)
    {
        setNull(false);
        value = l;
        cal.setTimeInMillis(value);
        timeZoneOffset = defaultZone.getOffset(l);
    }

    // implement NullablePrimitive
    protected void setNumber(Number num)
    {
        assignFrom(num);
    }

    public Calendar getCal()
    {
        if (cal == null) {
            cal = Calendar.getInstance();
            cal.setTimeZone(gmtZone);
        }
        cal.setTimeInMillis(value);
        return cal;
    }

    public void setCal(Calendar cal)
    {
        if (cal == null) {
            setNull(true);
            return;
        }
        setNull(false);
        this.cal = cal;
        value = cal.getTimeInMillis();
        timeZoneOffset = cal.getTimeZone().getOffset(value);
    }

    // TODO jvs 26-July-2004:  In order to support fractional seconds,
    // need to remember precision and use it in formatting.  Unfortunately,
    // SimpleDateFormat doesn't handle this, so we'll need to use
    // DecimalFormat for the fractional part.

    /**
     * Returns a string in the specified datetime format
     */
    public String toString(String format)
    {
        SimpleDateFormat dateFormat = new SimpleDateFormat(format);
        return dateFormat.format(new java.util.Date(value - timeZoneOffset));
    }

    /**
     * Returns a string in default format representing the datetime
     */
    public String toString()
    {
        return toString(getFormat());
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * SQL date value.
     */
    public static class SqlDate extends SqlDateTimeWithoutTZ
    {
        public String getFormat()
        {
            return DateFormatStr;
        }

        /**
         * Parses date from a String
         */
        public void parse(String date, String format, TimeZone tz)
        {
            if (format == null) {
                format = getFormat();
            }
            Calendar cal = SqlParserUtil.parseDateFormat(date, format, tz);
            if (cal != null) {
                java.util.Date parsedDate = cal.getTime();
                assignFrom(parsedDate);
            } else {
                String reason =
                    EigenbaseResource.instance().BadFormat.str(format);

                throw FarragoResource.instance().AssignFromFailed.ex(date,
                    "DATE", reason);
            }

        }

        /**
         * Use java.sql objects for JDBC's benefit.
         *
         * @return an Object representation of this value's data, or null if this
         *         value is null
         */
        public Object getData(long millisecond)
        {
            return new java.sql.Date(millisecond);
        }
    }

    /**
     * SQL time value.
     */
    public static class SqlTime extends SqlDateTimeWithoutTZ
    {
        protected String getFormat()
        {
            return TimeFormatStr;
        }

        /**
         * Parses time from a String
         */
        public void parse(String date, String format, TimeZone tz)
        {
            if (format == null) {
                format = getFormat();
            }
            SqlParserUtil.PrecisionTime pt =
                SqlParserUtil.parsePrecisionDateTimeLiteral(date, format, tz);
            if (pt != null) {
                java.util.Date parsedDate = pt.getCalendar().getTime();
                assignFrom(parsedDate);
            } else {
                String reason =
                    EigenbaseResource.instance().BadFormat.str(format);

                throw FarragoResource.instance().AssignFromFailed.ex(date,
                    "TIME", reason);
            }
        }

        /**
         * Use java.sql objects for JDBC's benefit.
         *
         * @return an Object representation of this value's data, or null if this
         *         value is null
         */
        public Object getData(long millisecond)
        {
            /* int hour = cal.get(Calendar.HOUR_OF_DAY);
            int min = cal.get(Calendar.MINUTE);
            int sec = cal.get(Calendar.SECOND); */
            return new java.sql.Time(millisecond);
        }
    }

    /**
     * SQL timestamp value.
     */
    public static class SqlTimestamp extends SqlDateTimeWithoutTZ
    {

        protected String getFormat()
        {
            return TimestampFormatStr;
        }

        /**
         * Parses timestamp from a String
         */
        public void parse(String date, String format, TimeZone tz)
        {
            if (format == null) {
                format = getFormat();
            }
            SqlParserUtil.PrecisionTime pt =
                SqlParserUtil.parsePrecisionDateTimeLiteral(date, format, tz);
            if (pt != null) {
                java.util.Date parsedDate = pt.getCalendar().getTime();
                assignFrom(parsedDate);
            } else {
                String reason =
                    EigenbaseResource.instance().BadFormat.str(format);

                throw FarragoResource.instance().AssignFromFailed.ex(date,
                    "TIMESTAMP", reason);
            }
        }

        /**
         * Use java.sql objects for JDBC's benefit.
         *
         * @return an Object representation of this value's data, or null if this
         *         value is null
         */
        public Object getData(long millisecond)
        {
            return new java.sql.Timestamp(millisecond);
        }
    }
}


// End SqlDateTimeWithoutTZ.java

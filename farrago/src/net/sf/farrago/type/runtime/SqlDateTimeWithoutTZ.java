/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2003-2003 Disruptive Technologies, Inc.
// You must accept the terms in LICENSE.html to use this software.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/
package net.sf.farrago.type.runtime;

import net.sf.farrago.resource.FarragoResource;
import net.sf.saffron.sql.parser.ParserUtil;
import net.sf.saffron.resource.SaffronResource;

import java.util.Calendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;


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
    // ~ Instance fields ------------------------------------------------------
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

    // ~ Static fields --------------------------------------------------------

    // Use same format as supported by parser (should be ISO format)
    public static final String DateFormatStr = ParserUtil.DateFormatStr;
    public static final String TimeFormatStr = ParserUtil.TimeFormatStr;
    public static final String TimestampFormatStr = ParserUtil.TimestampFormatStr;

    private static final TimeZone gmtZone = TimeZone.getTimeZone("GMT+0");

    /** The default timezone for this Java VM. */
    private static final TimeZone defaultZone =
            Calendar.getInstance().getTimeZone();

    /**
     * Create a runtime object with timezone offset set from localtime.
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
     *
     * @return long.class
     */
    public static Class getPrimitiveClass() {
        return long.class;
    }

    public abstract Object getData(long millisecond);

    /**
     * Set whether or not the value is null.  Note that once a value has been
     * set to null, its data should not be updated until the null state has
     * been cleared with a call to setNull(false).
     *
     * @param b true to set a null value; false to indicate a non-null
     *        value
     */
    public void setNull(boolean b) {
        isNull = b;
    }

    /**
     * Per the {@link NullableValue} contract, returns either null or the
     * embedded value object. The value object is a {@link java.sql.Time
     * JDBC Time/Date/Timestamp} in local time.
     */
    public Object getNullableData() {
        if (isNull() || getCal() == null) {
            return null;
        }
        // subtract timeZoneOffset to get localtime == GMT time.
        long millis = cal.getTimeInMillis();
        timeZoneOffset = defaultZone.getOffset(millis);
        return this.getData(millis - timeZoneOffset);
    }

    /**
     *
     * @return whether the value has been set to null
     */
    public boolean isNull() {
        return isNull;
    }

    /**
     * Assign value from an Object.
     *
     * @param date value to assign, or null to set null
     */
    public void assignFrom(Object date) {
        assert !isNull :
                "attempt to assign to null object in SqlDateTimeWithoutTZ";
        if (null == date) {
            setNull(true);
            return;
        } else if (date instanceof Long) {
            value = ((Long)date).longValue();
            return;
        } else if (date instanceof java.util.Date) {
            value = ((java.util.Date) date).getTime();// + timeZoneOffset; // set tzOffset?
            return;
        } else if (date instanceof SqlDateTimeWithoutTZ) {
            SqlDateTimeWithoutTZ  sqlDate = (SqlDateTimeWithoutTZ) date;
            this.timeZoneOffset = sqlDate.timeZoneOffset;
            this.value = sqlDate.value;
            this.isNull = sqlDate.isNull;
            return;
        } else {
            // REVIEW jvs 27-Aug-2004:  this is dangerous; should probably
            // require a specific interface instead
            String s = date.toString();
            assignFrom(s);
            return;
        }
    }

    public void assignFrom(long l) {
        assert (!isNull) : "attempt to assign to null object in SqlDateTimeWithoutTZ";
        value = l;
        cal.setTimeInMillis(value);
        timeZoneOffset = defaultZone.getOffset(l);
    }

    // implement NullablePrimitive
    protected void setNumber(Number num) {
        assignFrom(num);
    }

    public Calendar getCal() {
        if (cal == null) {
            cal = Calendar.getInstance();
            cal.setTimeZone(gmtZone);
        }
        cal.setTimeInMillis(value);
        return cal;
    }

    public void setCal(Calendar cal) {
        assert !isNull : "attempt to assign to null object in SqlDateTimeWithoutTZ";
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
     * Pad or truncate this value according to the given precision.
     *
     * @param precision desired precision
     *
     * @param needPad true if short values should be padded
     *
     * @param padByte byte to pad with
     */
    public void enforceBytePrecision(int precision,boolean needPad,byte padByte)
    {
        // Function stolen from BytePointer.java, currently does nothing
        // TODO: Properly implement for timestamps
    }


    /**
     * sql date
     */
    public static class SqlDate extends SqlDateTimeWithoutTZ {


        public SqlDate() {
            super();
        }

        /**
         * Assigns date from a String
         */
        public void assignFrom(Object obj) {
            if (!(obj instanceof String)) {
                super.assignFrom(obj);
                return;
            }
            String date = (String) obj;

            Calendar cal = ParserUtil.parseDateFormat(date, DateFormatStr);
            if (cal != null) {
                java.util.Date parsedDate = cal.getTime();
                assignFrom(parsedDate);
            }
            else {
                String reason = SaffronResource.instance().
                    getBadFormat(DateFormatStr);

                throw FarragoResource.instance().
                    newAssignFromFailed(date, "DATE", reason);
            }
        }

        /**
         * Returns a string in default format representing the date
         */
        public String toString()
        {
            return toString(DateFormatStr);
        }

        /**
         * use java.sql objects for jdbcs benefit.
         *
         * @return an Object representation of this value's data, or null if this
         *         value is null
         */
        public Object getData(long millisecond) {
            return new java.sql.Date(millisecond);
        }


    }

    /**
     * sql time
     */
    public static class SqlTime extends SqlDateTimeWithoutTZ {
        /**
         * Assigns time from a String
         */
        public void assignFrom(Object obj) {
            if (!(obj instanceof String)) {
                super.assignFrom(obj);
                return;
            }
            String date = (String) obj;

            ParserUtil.PrecisionTime pt =
                ParserUtil.parsePrecisionDateTimeLiteral(date, TimeFormatStr);
            if (pt != null) {
                java.util.Date parsedDate = pt.cal.getTime();
                assignFrom(parsedDate);
            }
            else {
                String reason = SaffronResource.instance().
                    getBadFormat(TimeFormatStr);

                throw FarragoResource.instance().
                    newAssignFromFailed(date, "TIME", reason);
            }
        }

        /**
         * Returns a string in default format representing the time
         */
        public String toString()
        {
            return toString(TimeFormatStr);
        }

        /**
         * use java.sql objects for jdbcs benefit.
         *
         * @return an Object representation of this value's data, or null if this
         *         value is null
         */
        public Object getData(long millisecond) {
            /* int hour = cal.get(Calendar.HOUR_OF_DAY);
            int min = cal.get(Calendar.MINUTE);
            int sec = cal.get(Calendar.SECOND); */
            return new java.sql.Time(millisecond);
        }
    }

    /**
     * sql timestamp.
     */
    public static class SqlTimestamp extends SqlDateTimeWithoutTZ {

        /**
         * Assigns timestamp from a String
         */
        public void assignFrom(Object obj) {
            if (!(obj instanceof String)) {
                super.assignFrom(obj);
                return;
            }
            String date = (String) obj;

            ParserUtil.PrecisionTime pt =
                ParserUtil.parsePrecisionDateTimeLiteral(date, TimestampFormatStr);
            if (pt != null) {
                java.util.Date parsedDate = pt.cal.getTime();
                assignFrom(parsedDate);
            }
            else {
                String reason = SaffronResource.instance().
                    getBadFormat(TimestampFormatStr);

                throw FarragoResource.instance().
                    newAssignFromFailed(date, "TIMESTAMP", reason);
            }
        }

        /**
         * Returns a string in default format representing the timestamp
         */
        public String toString()
        {
            return toString(TimestampFormatStr);
        }

        /**
         * use java.sql objects for jdbc's benefit.
         *
         * @return an Object representation of this value's data, or null if this
         *         value is null
         */
        public Object getData(long millisecond) {
            return new java.sql.Timestamp(millisecond);
        }

    }
}

// End SqlDateTimeWithoutTZ.java

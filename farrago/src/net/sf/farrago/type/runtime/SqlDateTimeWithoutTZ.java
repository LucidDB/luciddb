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

import net.sf.farrago.util.FarragoException;

import java.util.Calendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import java.text.ParseException;


/**
 * runtime type for date/time values.
 * This is a bit ugly, due to the unfortunate nature of sql 99 time w/o Timezone.
 *
 * The localtime is always the same hours/minutes in the current timezone, so GMT
 * has to be offset accordingly.
 *
 * @author lee
 * @since May 5, 2004
 * @version $Id$
 **/
public abstract class SqlDateTimeWithoutTZ extends NullablePrimitive
    implements NullableValue, AssignableValue
{
    public static final String ISODateFormat = "yyyy-MM-dd";
    public static final String ISOTimeFormat = "HH:mm:ss";
    public static final String ISOTimestampFormat = ISODateFormat + " " + ISOTimeFormat;

    private Calendar cal =  Calendar.getInstance();
    private static final TimeZone gmtZone = TimeZone.getTimeZone("GMT+0");

    public long value = 0; // time as GMT since epoch.
    public int timeZoneOffset = 0; // timezone offset in Millis.

    /**
     * Create a runtime object with timezone offset set from localtime.
     * FIXME    - we need the session tz, not the
     * server tz.
     */
    public SqlDateTimeWithoutTZ()
    {
        timeZoneOffset = Calendar.getInstance().getTimeZone().getRawOffset();
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

    public Object getNullableData() {
        if (isNull() || getCal() == null) {
            return null;
        }
        // subtract timeZoneOffset to get localtime == GMT time.
        long millis = cal.getTimeInMillis();
        timeZoneOffset = Calendar.getInstance().getTimeZone().getOffset(millis);
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
        assert ( !isNull ) : "attempt to assign to null object in SqlDateTimeWithoutTZ";
        if (null == date) {
            setNull(true);
            return;
        } else if (date instanceof Long) {
            value = ((Long)date).longValue()  ;
            return;
        } else if (date instanceof java.util.Date) {
            value = ((java.util.Date) date).getTime() + timeZoneOffset; // set tzOffset?
            return;
        } else if (date instanceof SqlDateTimeWithoutTZ) {
            SqlDateTimeWithoutTZ  sqlDate = (SqlDateTimeWithoutTZ) date;
            this.timeZoneOffset = sqlDate.timeZoneOffset;
            this.value = sqlDate.value;
            this.isNull = sqlDate.isNull;
            return;
        }
        assert false : "Unsupported object " + date;
    }

    public void assignFrom(long l) {
        assert (!isNull) : "attempt to assign to null object in SqlDateTimeWithoutTZ";
        value = l;
        cal.setTimeInMillis(value);
        timeZoneOffset = Calendar.getInstance().getTimeZone().getOffset(l);
    }

    // implement NullablePrimitive
    protected void setNumber(Number num) {
        assignFrom(num);
    }

    public Calendar getCal() {
        if (cal == null) {
            cal = Calendar.getInstance();
            cal.setTimeZone((gmtZone));
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
        public void assignFrom(String date) {
            if (date == null) {
                setNull(true);
                return;
            }

            // Only support ISO format for now
            SimpleDateFormat dateFormat = new SimpleDateFormat(ISODateFormat);
            try {
                java.util.Date parsedDate = dateFormat.parse(date);
                assignFrom(parsedDate);
            }
            catch (ParseException exp) {
                throw new FarragoException("Error converting String to Date", exp);
            }
        }

        /**
         * Returns a string in ISO format representing the date
         */
        public String toString()
        {
            return toString(ISODateFormat);
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
        public void assignFrom(String date) {
            if (date == null) {
                setNull(true);
                return;
            }

            // Only support ISO format for now
            SimpleDateFormat dateFormat = new SimpleDateFormat(ISOTimeFormat);
            try {
                java.util.Date parsedDate = dateFormat.parse(date);
                assignFrom(parsedDate);
            }
            catch (ParseException exp) {
                throw new FarragoException("Error converting String to Time", exp);
            }
        }

        /**
         * Returns a string in ISO format representing the time
         */
        public String toString()
        {
            return toString(ISOTimeFormat);
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
        public void assignFrom(String date) {
            if (date == null) {
                setNull(true);
                return;
            }

            // Only support ISO format for now
            SimpleDateFormat dateFormat = new SimpleDateFormat(ISOTimestampFormat);
            try {
                java.util.Date parsedDate = dateFormat.parse(date);
                assignFrom(parsedDate);
            }
            catch (ParseException exp) {
                throw new FarragoException("Error converting String to Timestamp", exp);
            }
        }

        /**
         * Returns a string in ISO format representing the timestamp
         */
        public String toString()
        {
            return toString(ISOTimestampFormat);
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

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

import java.util.Calendar;
import java.util.TimeZone;


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
        /*} else if (date instanceof java.util.Date) {
            value = ((java.util.Date)date).getTime() + timeZoneOffset; // set tzOffset?
            return; */
        } else if (date instanceof SqlDateTimeWithoutTZ) {
            SqlDateTimeWithoutTZ  sqlDate = (SqlDateTimeWithoutTZ) date;
            this.timeZoneOffset = sqlDate.timeZoneOffset;
            this.value = sqlDate.value;
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

    /**
     * sql date
     */
    public static class SqlDate extends SqlDateTimeWithoutTZ {

        public SqlDate() {
            super();
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

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2002-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package org.eigenbase.util14;

import java.sql.*;
import java.text.*;
import java.util.Calendar;
import java.util.TimeZone;

import org.eigenbase.resource.*;


/**
 * Utility functions for converting from one type to another
 *
 * @author angel
 * @version $Id$
 * @since Jan 22, 2006
 */
public class ConversionUtil
{

    //~ Methods ----------------------------------------------------------------

    /**
     * Converts a byte array into a bit string or a hex string.
     *
     * <p>For example, <code>toStringFromByteArray(new byte[] {0xAB, 0xCD},
     * 16)</code> returns <code>ABCD</code>.
     */
    public static String toStringFromByteArray(
        byte [] value,
        int radix)
    {
        assert (2 == radix) || (16 == radix) : "Make sure that the algorithm below works for your radix";
        if (0 == value.length) {
            return "";
        }

        int trick = radix * radix;
        StringBuffer ret = new StringBuffer();
        for (int i = 0; i < value.length; i++) {
            ret.append(
                Integer.toString(trick | (0x0ff & value[i]), radix).substring(
                    1));
        }

        return ret.toString().toUpperCase();
    }

    /**
     * Converts a string into a byte array. The inverse of {@link
     * #toStringFromByteArray(byte[], int)}.
     */
    public static byte [] toByteArrayFromString(
        String value,
        int radix)
    {
        assert (16 == radix) : "Specified string to byte array conversion not supported yet";
        assert ((value.length() % 2) == 0) : "Hex binary string must contain even number of characters";

        byte [] ret = new byte[value.length() / 2];
        for (int i = 0; i < ret.length; i++) {
            int digit1 = Character.digit(
                    value.charAt(i * 2),
                    radix);
            int digit2 = Character.digit(
                    value.charAt((i * 2) + 1),
                    radix);
            assert ((digit1 != -1) && (digit2 != -1)) : "String could not be converted to byte array";
            ret[i] = (byte) ((digit1 * radix) + digit2);
        }
        return ret;
    }

    /**
     * Converts an approximate value into a string, following the SQL 2003
     * standard.
     */
    public static String toStringFromApprox(double d, boolean isFloat)
    {
        NumberFormat nf = NumberUtil.getApproxFormatter(isFloat);
        return nf.format(d);
    }

    /**
     * Converts a string into a boolean
     */
    public static Boolean toBoolean(String str)
    {
        if (str == null) {
            return null;
        }
        str = str.trim();
        if (str.equalsIgnoreCase("TRUE")) {
            return Boolean.TRUE;
        } else if (str.equalsIgnoreCase("FALSE")) {
            return Boolean.FALSE;
        } else if (str.equalsIgnoreCase("UNKNOWN")) {
            return null;
        } else {
            throw EigenbaseResource.instance().InvalidBoolean.ex(str);
        }
    }

    /**
     * Converts a GmtDate to a java.sql.Date. A GmtDate has valid date 
     * components in GMT time, while a java.sql.Date is valid for the 
     * client time zone.
     * 
     * @param gmtDate a date relative to the GMT time zone
     * @param zone the client time zone
     * @return a Date with Jdbc semantics
     */
    public static Date gmtToJdbcDate(GmtDate gmtDate, TimeZone zone)
    {
        Calendar cal = Calendar.getInstance(DateTimeUtil.gmtZone);
        cal.setTime(gmtDate);

        int year = cal.get(Calendar.YEAR);
        int doy = cal.get(Calendar.DAY_OF_YEAR);

        cal.clear();
        cal.setTimeZone(zone);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.DAY_OF_YEAR, doy);
        return new Date(cal.getTimeInMillis());
    }

    /**
     * Converts a GmtTime to a java.sql.Time. A GmtTime has valid time 
     * components in GMT time, while a java.sql.Time is valid for the 
     * client time zone.
     * 
     * @param gmtTime a time relative to the GMT time zone
     * @param zone the client time zone
     * @return a Time with Jdbc semantics
     */
    public static Time gmtToJdbcTime(GmtTime gmtTime, TimeZone zone)
    {
        Calendar cal = Calendar.getInstance(DateTimeUtil.gmtZone);
        cal.setTime(gmtTime);
        
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);

        cal.clear();
        cal.setTimeZone(zone);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millis);
        return new Time(cal.getTimeInMillis());
    }

    /**
     * Converts a java.util.Date to a GmtDate. A java.util.Date has valid 
     * date components for the client time zone, while a GmtDate is valid 
     * for the GMT time zone.
     * 
     * @param jdbcDate a date with Jdbc semantics
     * @param zone the client time zone
     * @return a Date with Farrago semanctics.
     */
    public static GmtDate jdbcToGmtDate(
        java.util.Date jdbcDate, TimeZone zone)
    {
        long millis = jdbcDate.getTime();
        return new GmtDate(millis + zone.getOffset(millis));
    }

    /**
     * Converts a java.util.Time to a GmtTime. A java.util.Time has valid 
     * time components for the client time zone, while a GmtTime is valid 
     * for the GMT time zone.
     * 
     * @param jdbcTime a Time with Jdbc semantics
     * @param zone the client time zone
     * @return a Time with Farrago semanctics.
     */
    public static GmtTime jdbcToGmtTime(
        java.util.Date jdbcTime, TimeZone zone)
    {
        long millis = jdbcTime.getTime();
        return new GmtTime(millis + zone.getOffset(millis));
    }

    /**
     * Converts a Timestamp into a Date by setting time 
     * components to zero.
     * 
     * @param ts the timestamp to be converted
     * @param zone the time zone in which to interpret the timestamp
     * @return a date with zeroed time components
     */
    public static Date timestampToDate(
        Timestamp ts, TimeZone zone)
    {
        Calendar cal = Calendar.getInstance(zone);
        cal.setTime(ts);
        cal.set(Calendar.HOUR, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        return new Date(cal.getTimeInMillis());
    }
}

// End ConversionUtil.java

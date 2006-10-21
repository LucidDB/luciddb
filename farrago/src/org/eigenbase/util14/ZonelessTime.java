/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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
package org.eigenbase.util14;

import java.sql.Time;
import java.text.*;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * ZonelessTime is a time value without a time zone
 *
 * @author John Pham
 * @version $Id$
 */
public class ZonelessTime extends ZonelessDatetime
{

    //~ Instance fields --------------------------------------------------------

    protected int precision;
    protected Time tempTime;

    //~ Methods ----------------------------------------------------------------

    /**
     * Constructs a ZonelessTime
     */
    public ZonelessTime()
    {
    }

    // override ZonelessDatetime
    public void setZonelessTime(long value)
    {
        super.setZonelessTime(value);
        clearDate();
    }

    // override ZonelessDatetime
    public void setZonedTime(long value, TimeZone zone)
    {
        super.setZonedTime(value, zone);
        clearDate();
    }

    // implement ZonelessDatetime
    public Object toJdbcObject()
    {
        return getTempTime(getJdbcTime(DateTimeUtil.defaultZone));
    }

    /**
     * Override ZonelessDatetime
     * 
     * NOTE: the returned timestamp is based on the current date of the 
     * specified time zone, rather than the context variable for current_date,
     * as specified by the SQL standard.
     */
    public long getJdbcTimestamp(TimeZone zone)
    {
        Calendar cal = getCalendar(DateTimeUtil.gmtZone);
        cal.setTimeInMillis(getTime());
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);
        
        cal.setTimeZone(zone);
        cal.setTimeInMillis(System.currentTimeMillis());
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millis);
        return cal.getTimeInMillis();
    }

    /**
     * Converts this ZonelessTime to a java.sql.Time and formats it via the 
     * {@link java.sql.Time#toString() toString()} method of that class.
     * 
     * @return the formatted time string
     */
    public String toString()
    {
        Time jdbcTime = getTempTime(getJdbcTime(DateTimeUtil.defaultZone));
        return jdbcTime.toString();
    }

    /**
     * Formats this ZonelessTime via a SimpleDateFormat
     * 
     * @param format format string, as required by SimpleDateFormat
     * @return the formatted time string
     */
    public String toString(String format)
    {
        DateFormat formatter = getFormatter(format);
        Time jdbcTime = getTempTime(getTime());
        return formatter.format(jdbcTime);
    }

    /**
     * Parses a string as a ZonelessTime.
     * 
     * @param s a string representing a time in ISO format, i.e. according 
     *   to the SimpleDateFormat string "HH:mm:ss"
     * @return the parsed time, or null if parsing failed
     */
    public static ZonelessTime parse(String s)
    {
        DateTimeUtil.PrecisionTime pt =
            DateTimeUtil.parsePrecisionDateTimeLiteral(
                s,
                DateTimeUtil.TimeFormatStr,
                DateTimeUtil.gmtZone);
        if (pt == null) {
            return null;
        }
        ZonelessTime zt = new ZonelessTime();
        zt.setZonelessTime(pt.getCalendar().getTime().getTime());
        zt.precision = pt.getPrecision();
        return zt;
    }

    /**
     * Gets a temporary Time object. The same object is returned every time.
     */
    protected Time getTempTime(long value)
    {
        if (tempTime == null) {
            tempTime = new Time(value);
        } else {
            tempTime.setTime(value);
        }
        return tempTime;
    }
}

// End GmtTime.java

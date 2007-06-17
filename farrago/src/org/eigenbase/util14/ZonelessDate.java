/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

import java.sql.Date;

import java.text.*;

import java.util.Calendar;
import java.util.TimeZone;


/**
 * ZonelessDate is a date value without a time zone.
 *
 * @author John Pham
 * @version $Id$
 */
public class ZonelessDate
    extends ZonelessDatetime
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -6385775986251759394L;

    //~ Instance fields --------------------------------------------------------

    protected transient Date tempDate;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a ZonelessDate.
     */
    public ZonelessDate()
    {
    }

    //~ Methods ----------------------------------------------------------------

    // override ZonelessDatetime
    public void setZonelessTime(long value)
    {
        super.setZonelessTime(value);
        clearTime();
    }

    // override ZonelessDatetime
    public void setZonedTime(long value, TimeZone zone)
    {
        super.setZonedTime(value, zone);
        clearTime();
    }

    // implement ZonelessDatetime
    public Object toJdbcObject()
    {
        return getTempDate(getJdbcDate(DateTimeUtil.defaultZone));
    }

    /**
     * Converts this ZonelessDate to a java.sql.Date and formats it via the
     * {@link java.sql.Date#toString() toString()} method of that class.
     *
     * @return the formatted date string
     */
    public String toString()
    {
        Date jdbcDate = getTempDate(getJdbcDate(DateTimeUtil.defaultZone));
        return jdbcDate.toString();
    }

    /**
     * Formats this ZonelessDate via a SimpleDateFormat
     *
     * @param format format string, as required by {@link SimpleDateFormat}
     *
     * @return the formatted date string
     */
    public String toString(String format)
    {
        DateFormat formatter = getFormatter(format);
        Date jdbcDate = getTempDate(getTime());
        return formatter.format(jdbcDate);
    }

    /**
     * Parses a string as a ZonelessDate.
     *
     * @param s a string representing a date in ISO format, i.e. according to
     * the SimpleDateFormat string "yyyy-MM-dd"
     *
     * @return the parsed date, or null if parsing failed
     */
    public static ZonelessDate parse(String s)
    {
        return parse(s, DateTimeUtil.DateFormatStr);
    }

    /**
     * Parses a string as a ZonelessDate with a given format string.
     *
     * @param s a string representing a date in ISO format, i.e. according to
     * the SimpleDateFormat string "yyyy-MM-dd"
     * @param format format string as per {@link SimpleDateFormat}
     *
     * @return the parsed date, or null if parsing failed
     */
    public static ZonelessDate parse(String s, String format)
    {
        Calendar cal =
            DateTimeUtil.parseDateFormat(
                s,
                format,
                DateTimeUtil.gmtZone);
        if (cal == null) {
            return null;
        }
        ZonelessDate zd = new ZonelessDate();
        zd.setZonelessTime(cal.getTimeInMillis());
        return zd;
    }

    /**
     * Gets a temporary Date object. The same object is returned every time.
     */
    protected Date getTempDate(long value)
    {
        if (tempDate == null) {
            tempDate = new Date(value);
        } else {
            tempDate.setTime(value);
        }
        return tempDate;
    }
}

// End ZonelessDate.java

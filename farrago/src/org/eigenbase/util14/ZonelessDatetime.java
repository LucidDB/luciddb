/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

import java.io.*;

import java.text.*;

import java.util.*;


/**
 * ZonelessDatetime is an abstract class for dates, times, or timestamps that
 * contain a zoneless time value.
 *
 * @author John Pham
 * @version $Id$
 */
public abstract class ZonelessDatetime
    implements BasicDatetime,
        Serializable
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -1274713852537224763L;

    //~ Instance fields --------------------------------------------------------

    /**
     * Treat this as a protected field. It is only made public to simplify Java
     * code generation.
     */
    public long internalTime;

    // The following fields are workspace and are not serialized.

    protected transient Calendar tempCal;
    protected transient DateFormat tempFormatter;
    protected transient String lastFormat;

    //~ Methods ----------------------------------------------------------------

    // implement BasicDatetime
    public long getTime()
    {
        return internalTime;
    }

    // implement BasicDatetime
    public void setZonelessTime(long value)
    {
        this.internalTime = value;
    }

    // implement BasicDatetime
    public void setZonedTime(long value, TimeZone zone)
    {
        this.internalTime = value + zone.getOffset(value);
    }

    /**
     * Gets the time portion of this zoneless datetime.
     */
    public long getTimeValue()
    {
        // Value must be non-negative, even for negative timestamps, and
        // unfortunately the '%' operator returns a negative value if its LHS
        // is negative.
        long timePart = internalTime % DateTimeUtil.MILLIS_PER_DAY;
        if (timePart < 0) {
            timePart += DateTimeUtil.MILLIS_PER_DAY;
        }
        return timePart;
    }

    /**
     * Gets the date portion of this zoneless datetime.
     */
    public long getDateValue()
    {
        return internalTime - getTimeValue();
    }

    /**
     * Clears the date component of this datetime
     */
    public void clearDate()
    {
        internalTime = getTimeValue();
    }

    /**
     * Clears the time component of this datetime
     */
    public void clearTime()
    {
        internalTime = getDateValue();
    }

    /**
     * Gets the value of this datetime as a milliseconds value for {@link
     * java.sql.Time}.
     *
     * @param zone time zone in which to generate a time value for
     */
    public long getJdbcTime(TimeZone zone)
    {
        long timeValue = getTimeValue();
        return timeValue - zone.getOffset(timeValue);
    }

    /**
     * Gets the value of this datetime as a milliseconds value for {@link
     * java.sql.Date}.
     *
     * @param zone time zone in which to generate a time value for
     */
    public long getJdbcDate(TimeZone zone)
    {
        Calendar cal = getCalendar(DateTimeUtil.gmtZone);
        cal.setTimeInMillis(getDateValue());

        int year = cal.get(Calendar.YEAR);
        int doy = cal.get(Calendar.DAY_OF_YEAR);

        cal.clear();
        cal.setTimeZone(zone);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.DAY_OF_YEAR, doy);
        return cal.getTimeInMillis();
    }

    /**
     * Gets the value of this datetime as a milliseconds value for {@link
     * java.sql.Timestamp}.
     *
     * @param zone time zone in which to generate a time value for
     */
    public long getJdbcTimestamp(TimeZone zone)
    {
        Calendar cal = getCalendar(DateTimeUtil.gmtZone);
        cal.setTimeInMillis(internalTime);

        int year = cal.get(Calendar.YEAR);
        int doy = cal.get(Calendar.DAY_OF_YEAR);
        int hour = cal.get(Calendar.HOUR_OF_DAY);
        int minute = cal.get(Calendar.MINUTE);
        int second = cal.get(Calendar.SECOND);
        int millis = cal.get(Calendar.MILLISECOND);

        cal.clear();
        cal.setTimeZone(zone);
        cal.set(Calendar.YEAR, year);
        cal.set(Calendar.DAY_OF_YEAR, doy);
        cal.set(Calendar.HOUR_OF_DAY, hour);
        cal.set(Calendar.MINUTE, minute);
        cal.set(Calendar.SECOND, second);
        cal.set(Calendar.MILLISECOND, millis);
        return cal.getTimeInMillis();
    }

    /**
     * Returns this datetime as a Jdbc object
     */
    public abstract Object toJdbcObject();

    /**
     * Gets a temporary Calendar set to the specified time zone. The same
     * Calendar is returned on subsequent calls.
     */
    protected Calendar getCalendar(TimeZone zone)
    {
        if (tempCal == null) {
            tempCal = Calendar.getInstance(zone);
        } else {
            tempCal.setTimeZone(zone);
        }
        return tempCal;
    }

    /**
     * Gets a temporary formatter for a zoneless date time. The same formatter
     * is returned on subsequent calls.
     *
     * @param format a {@link java.text.SimpleDateFormat} format string
     */
    protected DateFormat getFormatter(String format)
    {
        if ((tempFormatter != null) && lastFormat.equals(format)) {
            return tempFormatter;
        }
        tempFormatter = DateTimeUtil.newDateFormat(format);
        tempFormatter.setTimeZone(DateTimeUtil.gmtZone);
        lastFormat = format;
        return tempFormatter;
    }
}

// End ZonelessDatetime.java

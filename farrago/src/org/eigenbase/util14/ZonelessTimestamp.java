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

import java.sql.Timestamp;
import java.text.*;

/**
 * ZonelessTimestamp is a timestamp value without a time zone.
 *
 * @author John Pham
 * @version $Id$
 */
public class ZonelessTimestamp extends ZonelessDatetime
{

    //~ Instance fields --------------------------------------------------------

    protected int precision;
    protected Timestamp tempTimestamp;

    //~ Methods ----------------------------------------------------------------

    /**
     * Constructs a ZonelessTimestamp.
     */
    public ZonelessTimestamp()
    {
    }

    // implement ZonelessDatetime
    public Object toJdbcObject()
    {
        return getTempTimestamp(getJdbcTimestamp(DateTimeUtil.defaultZone));
    }

    /**
     * Converts this ZonelessTimestamp to a java.sql.Timestamp and formats 
     * it via the {@link java.sql.Timestamp#toString() toString()} method of 
     * that class.
     * 
     * <p>
     * 
     * Note: Jdbc formatting always includes a decimal point and at least 
     * one digit of milliseconds precision. Trailing zeros, except for the 
     * first one after the decimal point, do not appear in the output.
     * 
     * @return the formatted time string
     */
    public String toString()
    {
        Timestamp ts = 
            getTempTimestamp(getJdbcTimestamp(DateTimeUtil.defaultZone));
        return ts.toString();
    }

    /**
     * Formats this ZonelessTimestamp via a SimpleDateFormat. This method 
     * does not display milliseconds precision.
     * 
     * @param format format string, as required by SimpleDateFormat
     * @return the formatted timestamp string
     */
    public String toString(String format)
    {
        DateFormat formatter = getFormatter(format);
        Timestamp ts = getTempTimestamp(getTime());
        return formatter.format(ts);
    }

    /**
     * Parses a string as a ZonelessTimestamp. This method's parsing is strict 
     * and may parse fractional seconds (as opposed to just milliseconds.)
     * 
     * @param s a string representing a time in ISO format, i.e. according 
     *   to the SimpleDateFormat string "yyyy-MM-dd HH:mm:ss"
     * @return the parsed time, or null if parsing failed
     */
    public static ZonelessTimestamp parse(String s)
    {
        DateTimeUtil.PrecisionTime pt =
            DateTimeUtil.parsePrecisionDateTimeLiteral(
                s,
                DateTimeUtil.TimestampFormatStr,
                DateTimeUtil.gmtZone);
        if (pt == null) {
            return null;
        }
        ZonelessTimestamp zt = new ZonelessTimestamp();
        zt.setZonelessTime(pt.getCalendar().getTime().getTime());
        zt.precision = pt.getPrecision();
        return zt;
    }

    /**
     * Gets a temporary Timestamp object. The same object is returned 
     * every time.
     */
    protected Timestamp getTempTimestamp(long value)
    {
        if (tempTimestamp == null) {
            tempTimestamp = new Timestamp(value);
        } else {
            tempTimestamp.setTime(value);
        }
        return tempTimestamp;
    }
}

// End GmtTime.java

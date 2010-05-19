/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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


/**
 * ZonelessTimestamp is a timestamp value without a time zone.
 *
 * @author John Pham
 * @version $Id$
 */
public class ZonelessTimestamp
    extends ZonelessDatetime
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * SerialVersionUID created with JDK 1.5 serialver tool.
     */
    private static final long serialVersionUID = -6829714640541648394L;

    //~ Instance fields --------------------------------------------------------

    protected final int precision;

    protected transient Timestamp tempTimestamp;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs a ZonelessTimestamp.
     */
    public ZonelessTimestamp()
    {
        this.precision = 0;
    }

    /**
     * Constructs a ZonelessTimestamp with precision.
     *
     * <p>The precision is the number of digits to the right of the decimal
     * point in the seconds value. For example, a <code>TIMESTAMP(3)</code> has
     * a precision to milliseconds.
     *
     * @param precision Number of digits of precision
     */
    public ZonelessTimestamp(int precision)
    {
        this.precision = precision;
    }

    //~ Methods ----------------------------------------------------------------

    // implement ZonelessDatetime
    public Object toJdbcObject()
    {
        return new Timestamp(getJdbcTimestamp(DateTimeUtil.defaultZone));
    }

    /**
     * Converts this ZonelessTimestamp to a java.sql.Timestamp and formats it
     * via the {@link java.sql.Timestamp#toString() toString()} method of that
     * class.
     *
     * <p>Note: Jdbc formatting always includes a decimal point and at least one
     * digit of milliseconds precision. Trailing zeros, except for the first one
     * after the decimal point, do not appear in the output.
     *
     * @return the formatted time string
     */
    public String toString()
    {
        Timestamp ts =
            getTempTimestamp(getJdbcTimestamp(DateTimeUtil.defaultZone));

        // Remove trailing '.0' so that format is consistent with SQL spec for
        // CAST(TIMESTAMP(0) TO VARCHAR). E.g. "1969-12-31 16:00:00.0"
        // becomes "1969-12-31 16:00:00"
        return ts.toString().substring(0, 19);
    }

    /**
     * Formats this ZonelessTimestamp via a SimpleDateFormat. This method does
     * not display milliseconds precision.
     *
     * @param format format string, as required by SimpleDateFormat
     *
     * @return the formatted timestamp string
     */
    public String toString(String format)
    {
        DateFormat formatter = getFormatter(format);
        Timestamp ts = getTempTimestamp(getTime());
        return formatter.format(ts);
    }

    /**
     * Parses a string as a ZonelessTimestamp.
     *
     * <p>This method's parsing is strict and may parse fractional seconds (as
     * opposed to just milliseconds.)
     *
     * @param s a string representing a time in ISO format, i.e. according to
     * the SimpleDateFormat string "yyyy-MM-dd HH:mm:ss"
     *
     * @return the parsed time, or null if parsing failed
     */
    public static ZonelessTimestamp parse(String s)
    {
        return parse(s, DateTimeUtil.TimestampFormatStr);
    }

    /**
     * Parses a string as a ZonelessTimestamp using a given format string.
     *
     * <p>This method's parsing is strict and may parse fractional seconds (as
     * opposed to just milliseconds.)
     *
     * @param s a string representing a time in ISO format, i.e. according to
     * the SimpleDateFormat string "yyyy-MM-dd HH:mm:ss"
     * @param format format string as per {@link SimpleDateFormat}
     *
     * @return the parsed timestamp, or null if parsing failed
     */
    public static ZonelessTimestamp parse(String s, String format)
    {
        DateTimeUtil.PrecisionTime pt =
            DateTimeUtil.parsePrecisionDateTimeLiteral(
                s,
                format,
                DateTimeUtil.gmtZone);
        if (pt == null) {
            return null;
        }
        ZonelessTimestamp zt = new ZonelessTimestamp(pt.getPrecision());
        zt.setZonelessTime(pt.getCalendar().getTime().getTime());
        return zt;
    }

    /**
     * Gets a temporary Timestamp object. The same object is returned every
     * time.
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

// End ZonelessTimestamp.java

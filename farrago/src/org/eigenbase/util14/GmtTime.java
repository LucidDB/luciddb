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
import java.util.TimeZone;

/**
 * GmtTime represents an instant in time whose time components are 
 * valid with respect to the GMT time zone. It differs from 
 * {@link java.sql.Time} whose time components are valid with respect 
 * to the default time zone. Unlike java.sql.Time, GmtTime is a 
 * canonical representation and does not vary depending on time zone
 * settings.
 *
 * @author John Pham
 * @version $Id$
 */
public class GmtTime extends java.util.Date
{

    //~ Static fields/initializers ---------------------------------------------

    private static final String format = DateTimeUtil.TimeFormatStr;
    private static final TimeZone gmtZone = DateTimeUtil.gmtZone;
    private static final TimeZone defaultZone = DateTimeUtil.defaultZone;

    //~ Methods ----------------------------------------------------------------

    /**
     * Constructs a GmtTime initialized with milliseconds since the epoch.
     * This method does not perform any checks for validity.
     */
    public GmtTime(long millis)
    {
        super(millis);
    }

    /**
     * Converts this GmtTime to a java.sql.Time and formats it via the 
     * {@link java.sql.Time#toString() toString()} method of that class.
     * 
     * @return the formatted time string
     */
   public String toString()
    {
        Time jdbcTime = ConversionUtil.gmtToJdbcTime(this, defaultZone);
        return jdbcTime.toString();
    }

    /**
     * Formats this GmtTime via a SimpleDateFormat
     * 
     * @param format format string, as required by SimpleDateFormat
     * @return the formatted time string
     */
    public String toString(String format)
    {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(gmtZone);
        return sdf.format(this);
    }

    /**
     * Parses a string as a GmtTime. The time will be valid for  
     * the GMT time zone.
     * 
     * @param s a string representing a time in ISO format, i.e. according 
     *   to the SimpleDateFormat string "HH:mm:ss"
     * @return the parsed time, or null if parsing failed
     */
    public static GmtTime parseGmt(String s)
    {
        DateTimeUtil.PrecisionTime pt =
            DateTimeUtil.parsePrecisionDateTimeLiteral(
                s,
                format,
                gmtZone);
        if (pt == null) {
            return null;
        }
        return new GmtTime(pt.getCalendar().getTime().getTime());
    }
}

// End GmtTime.java

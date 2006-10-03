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

import java.sql.Date;
import java.text.*;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * GmtDate represents an instant in time whose date components are 
 * valid with respect to the GMT time zone. It differs from 
 * {@link java.sql.Date} whose date components are valid with respect 
 * to the default time zone. Unlike java.sql.Date, GmtDate is a 
 * canonical representation and does not vary depending on time zone
 * settings.
 *
 * @author John Pham
 * @version $Id$
 */
public class GmtDate extends java.util.Date
{

    //~ Static fields/initializers ---------------------------------------------

    private static final String format = DateTimeUtil.DateFormatStr;
    private static final TimeZone gmtZone = DateTimeUtil.gmtZone;
    private static final TimeZone defaultZone = DateTimeUtil.defaultZone;

    //~ Methods ----------------------------------------------------------------

    /**
     * Constructs a GmtDate initialized with milliseconds since the epoch.
     * This method does not perform any checks for validity.
     */
    public GmtDate(long millis)
    {
        super(millis);
    }

    /**
     * Converts this GmtDate to a java.sql.Date and formats it via the 
     * {@link java.sql.Date#toString() toString()} method of that class.
     * 
     * @return the formatted date string
     */
    public String toString()
    {
        Date jdbcDate = ConversionUtil.gmtToJdbcDate(this, defaultZone);
        return jdbcDate.toString();
    }

    /**
     * Formats this GmtDate via a SimpleDateFormat
     * 
     * @param format format string, as required by {@link SimpleDateFormat}
     * @return the formatted date string
     */
    public String toString(String format)
    {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        sdf.setTimeZone(gmtZone);
        return sdf.format(this);
    }

    /**
     * Parses a string as a GmtDate. The date will be valid for  
     * the GMT time zone.
     * 
     * @param s a string representing a date in ISO format, i.e. according 
     *   to the SimpleDateFormat string "yyyy-MM-dd"
     * @return the parsed date, or null if parsing failed
     */
    public static GmtDate parseGmt(String s)
    {
        Calendar cal = DateTimeUtil.parseDateFormat(s, format, gmtZone);
        if (cal == null) {
            return null;
        }
        return new GmtDate(cal.getTimeInMillis());
    }
}

// End GmtDate.java

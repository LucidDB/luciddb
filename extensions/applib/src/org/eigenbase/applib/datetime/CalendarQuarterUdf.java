/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2006 SQLstream, Inc.
// Copyright (C) 2006 DynamoBI Corporation
//
// This library is free software; you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation; either version 2.1 of the License, or (at
// your option) any later version.
//
// This library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this library; if not, write to the Free Software
// Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
*/
package org.eigenbase.applib.datetime;

import java.sql.*;

import org.eigenbase.applib.resource.*;


/**
 * Convert a date to a calendar quarter, with Q1 beginning in January. Ported
 * from //bb/bb713/server/SQL/toCYQuarter.java
 */
public class CalendarQuarterUdf
{
    // NOTE: Both java.sql.Date and java.sql.Timestamp extend java.util.Date.
    // Problem is that the getXXX methods have been deprecated in
    // java.util.Date, so both methods execute the copies of the same code
    // rather than upcasting and executing generic code.

    //~ Methods ----------------------------------------------------------------

    /**
     * @param in Timestamp to convert
     *
     * @return Quarter and calendar year represented by date (e.g., Q3CY96)
     */
    public static String execute(Timestamp in)
    {
        int year = in.getYear();
        int month = in.getMonth() + 1;
        int quarter = ((month - 1) / 3) + 1;
        return (getCalendarQuarter(quarter, year));
    }

    /**
     * @param in Date to convert
     *
     * @return Quarter and calendar year represented by date (e.g., Q3CY96)
     */
    public static String execute(Date in)
    {
        int year = in.getYear();
        int month = in.getMonth() + 1;
        int quarter = ((month - 1) / 3) + 1;
        return (getCalendarQuarter(quarter, year));
    }

    /**
     * Get the string representation of the calendar quarter for a given quarter
     * and year. Ported from
     * //bb/bb713/server/Java/Broadbase/TimeDimensionInternal.java
     *
     * @param quarterIn quarter for string representation of calendar
     * @param yearIn year for string representation of caledar
     */
    public static String getCalendarQuarter(int quarterIn, int yearIn)
    {
        int year =
            ((yearIn % 100) >= 0) ? (yearIn % 100) : (100 + (yearIn % 100));
        String strYear =
            (year < 10) ? ("0" + Integer.toString(year))
            : Integer.toString(year);
        String ret =
            ApplibResource.instance().CalendarQuarter.str(
                Integer.toString(quarterIn),
                strYear);

        return ret;
    }
}

// End CalendarQuarterUdf.java

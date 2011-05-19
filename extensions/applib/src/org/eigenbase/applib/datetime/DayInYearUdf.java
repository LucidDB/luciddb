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


/**
 * Convert a date to the day in the year. Ported from
 * //bb/bb713/server/SQL/toDayInYear.java
 */
public class DayInYearUdf
{
    //~ Static fields/initializers ---------------------------------------------

    // 0-based, for day-in-year
    private static final int [] numDays = {
        0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
    };

    // 0-based, for day-in-year
    private static final int [] leapNumDays = {
        0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335
    };

    //~ Methods ----------------------------------------------------------------

    /**
     * @param in Date to convert
     *
     * @return Day in year
     */
    public static int execute(Date dt)
    {
        return execute(dt.getYear() + 1900, dt.getMonth() + 1, dt.getDate());
    }

    /**
     * @param in Timestamp to convert
     *
     * @return Day in year
     */
    public static int execute(Timestamp ts)
    {
        return execute(ts.getYear() + 1900, ts.getMonth() + 1, ts.getDate());
    }

    /**
     * @param year Year of date to convert
     * @param month Month of date to convert
     * @param date Day in month of date to convert
     *
     * @return Day in year
     */
    public static int execute(int year, int month, int date)
    {
        // check if leap year
        if (((year % 4) == 0) && (((year % 100) != 0) || ((year % 400) == 0))) {
            return (leapNumDays[month - 1] + date);
        } else {
            return (numDays[month - 1] + date);
        }
    }
}

// End DayInYearUdf.java

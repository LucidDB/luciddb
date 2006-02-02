/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.luciddb.applib;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Types;

/**
 * Convert a date to the day in the year.
 *
 * Ported from //bb/bb713/server/SQL/toDayInYear.java
 */
public class DayInYear
{
    private static final int numDays[]
        = {0,31,59,90,120,151,181,212,243,273,304,334}; // 0-based, for day-in-year
    private static final int leapNumDays[]
        = {0,31,60,91,121,152,182,213,244,274,305,335}; // 0-based, for day-in-year

    /**
     * @param in		Date to convert
     * @return			Day in year
     */
    public static int FunctionExecute( Date dt )
    {
        return FunctionExecute( dt.getYear()+1900, dt.getMonth()+1, dt.getDate() );
    }

    /**
     * @param in		Timestamp to convert
     * @return			Day in year
     */
    public static int FunctionExecute( Timestamp ts )
    {
        return FunctionExecute( ts.getYear()+1900, ts.getMonth()+1, ts.getDate() );
    }

    /**
     * @param year		Year of date to convert
     * @param month		Month of date to convert
     * @param date		Day in month of date to convert
     * @return			Day in year
     */
    public static int FunctionExecute( int year, int month, int date )
    {
        // check if leap year
        if( ( year%4 == 0 ) && ( ( year%100 != 0 ) || ( year%400 == 0 ) ) )
            return( leapNumDays[ month-1 ] + date );
        else
            return( numDays[ month-1 ] + date );
    }

}

// End DayInYear.java

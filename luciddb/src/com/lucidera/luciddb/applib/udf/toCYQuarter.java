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

import java.sql.Types;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Convert a date to a calendar quarter, with Q1 beginning in January.
 *
 * Ported from //bb/bb713/server/SQL/toCYQuarter.java
 */
public class toCYQuarter
{
    // NOTE: Both java.sql.Date and java.sql.Timestamp extend java.util.Date.
    // Problem is that the getXXX methods have been deprecated in
    // java.util.Date, so both methods execute the copies of the same code
    // rather than upcasting and executing generic code.

    /**
     * @param in Timestamp to convert
     * @return Quarter and calendar year represented by date (e.g., Q3CY96)
     * @exception SQLException thrown when the mask is invalid, or the string
     *     is invalid for the specified mask.
     */
    public static String FunctionExecute( Timestamp in )
    {
        int year = in.getYear();
        int month = in.getMonth() + 1;
        int quarter = ( month - 1 ) / 3 + 1;
        return( getCalendarQuarter( quarter, year ) );
    }

    /**
     * @param in Date to convert
     * @return Quarter and calendar year represented by date (e.g., Q3CY96)
     * @exception SQLException thrown when the mask is invalid, or the string
     *     is invalid for the specified mask.
     */
    public static String FunctionExecute( Date in )
    {
        int year = in.getYear();
        int month = in.getMonth() + 1;
        int quarter = ( month - 1 ) / 3 + 1;
        return( getCalendarQuarter( quarter, year ) );
    }

    /**
     * Get the string representation of the calendar
     * quarter for a given quarter and year.  
     *
     * Ported from //bb/bb713/server/Java/Broadbase/TimeDimensionInternal.java
     */
    public static String getCalendarQuarter( int quarterIn, int yearIn )
    {
        int year = ((yearIn % 100) >= 0)
            ? (yearIn % 100 ) : (100 + (yearIn % 100));
        String strYear = ( year < 10 )
            ? "0" + Integer.toString(year) : Integer.toString(year);
        String ret = ApplibResourceObject.get().CalendarQuarter.str(
            Integer.toString(quarterIn), strYear);

        return ret;
    }

}

// End toCYQuarter.java

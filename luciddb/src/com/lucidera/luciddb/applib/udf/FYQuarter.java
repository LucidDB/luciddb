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
 * Convert a date to a fiscal quarter, with Q1 beginning in the month
 * specified.
 *
 * Ported from //bb/bb713/server/SQL/toFYQuarter.java
 */
public class FYQuarter
{
    public static String FunctionExecute( int year, int month, int firstMonth )
    {
        if( ( firstMonth < 1 ) || ( firstMonth > 12 ) ) {
            throw new IllegalArgumentException(
                ApplibResourceObject.get().InvalidFirstMonth.ex());
        }

        if( month >= firstMonth ) {
            month = month - firstMonth;
            if( firstMonth > 1 )
                year++;
        } else {
            month = month - firstMonth + 12;
        }

        int quarter = (month / 3) + 1;
        int intYear = ((year % 100) >= 0)
            ? (year % 100) : (100 + (year % 100));
 
        String strYear = (intYear < 10)
            ? "0" + Integer.toString(intYear) : Integer.toString(intYear);

        String ret = ApplibResourceObject.get().FiscalYearQuarter.str(
            Integer.toString(quarter), strYear);
        return ret;
    }

    /**
     * @param in Date to convert
     * @param firstMonth First month of fiscal year (January = 1) 
     * @return Quarter and fiscal year represented by date (e.g., Q1FY97) 
     * @exception SQLException thrown when the mask is invalid, or the string
     *     is invalid for the specified mask.
     */
    public static String FunctionExecute( Date in, int firstMonth )
    {
        if( ( firstMonth < 1 ) || ( firstMonth > 12 ) ) {
            throw new IllegalArgumentException(
                ApplibResourceObject.get().InvalidFirstMonth.ex());
        }

        int year = in.getYear();
        int month = in.getMonth() + 1;
        return FunctionExecute( year, month, firstMonth );
    }

    /*
     * @param in Timestamp to convert
     * @param firstMonth First month of fiscal year (January = 1) 
     * @return Quarter and fiscal year represented by date (e.g., Q1FY97) 
     * @exception SQLException thrown when the mask is invalid, or the string
     *     is invalid for the specified mask.
     */
    public static String FunctionExecute( Timestamp in, int firstMonth )
    {
        if( ( firstMonth < 1 ) || ( firstMonth > 12 ) ) {
            throw new IllegalArgumentException(
                ApplibResourceObject.get().InvalidFirstMonth.ex());
        }

        int year = in.getYear();
        int month = in.getMonth() + 1;
        return FunctionExecute( year, month, firstMonth );
    }

}

// End FYQuarter.java

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
package com.lucidera.luciddb.applib.datetime;

import java.sql.Types;
import java.sql.Date;
import java.sql.Timestamp;
import com.lucidera.luciddb.applib.resource.*;

/**
 * Convert a date to a fiscal year, with Q1 beginning in the month specified.
 *
 * Ported from //bb/bb713/server/SQL/toFYYear.java
 */
public class FiscalYearUdf
{
    private static int calculate( int year, int month, int firstMonth )
        throws ApplibException
    {
        if( ( firstMonth < 1 ) || ( firstMonth > 12 ) ) {
            throw ApplibResourceObject.get().InvalidFirstMonth.ex();
        }

        if( ( firstMonth != 1 ) && ( month >= firstMonth ) ) {
            year++;
        }

        return year + 1900;
    }

    /**
     * @param in Date to convert
     * @param firstMonth First month of fiscal year (January = 1)
     * @return Fiscal year represented by date
     * @exception ApplibException
     */
    public static int execute( Date in, int firstMonth ) throws ApplibException
    {
        return calculate( in.getYear(), in.getMonth() + 1, firstMonth );
    }

    /**
     * @param in Timestamp to convert
     * @param firstMonth First month of fiscal year (January = 1)
     * @return Fiscal year represented by date
     * @exception ApplibException
     */
    public static int execute( Timestamp in, int firstMonth ) 
        throws ApplibException
    {
        return calculate( in.getYear(), in.getMonth() + 1, firstMonth );
    }

}

// End FiscalYearUdf.java

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
 * Convert a date to a fiscal year, with Q1 beginning in the month specified.
 *
 * Ported from //bb/bb713/server/SQL/toFYYear.java
 */
public class FYYear
{
    private static int calculate( int year, int month, int firstMonth )
    {
        if( ( firstMonth < 1 ) || ( firstMonth > 12 ) )
            throw new IllegalArgumentException("invalid first month");

        if( ( firstMonth != 1 ) && ( month >= firstMonth ) )
            year++;

        return year + 1900;
    }

    /**
     * @param in Date to convert
     * @param firstMonth First month of fiscal year (January = 1)
     * @return Fiscal year represented by date
     */
    public static int FunctionExecute( Date in, int firstMonth )
    {
        return calculate( in.getYear(), in.getMonth() + 1, firstMonth );
    }

    /**
     * @param in Timestamp to convert
     * @param firstMonth First month of fiscal year (January = 1)
     * @return Fiscal year represented by date
     */
    public static int FunctionExecute( Timestamp in, int firstMonth )
    {
        return calculate( in.getYear(), in.getMonth() + 1, firstMonth );
    }

}

// End FYYear.java

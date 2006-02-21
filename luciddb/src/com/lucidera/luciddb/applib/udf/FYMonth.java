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
 * Convert a date to a fiscal month, with month 1 beginning in the month
 * specified.
 *
 * Ported from //BB/bb713/server/SQL/toFYMonth.java
 */
public class FYMonth
{
    private static int calculate(int month, int firstMonth)
    {
        if((firstMonth < 1) || (firstMonth > 12)) {
            throw new IllegalArgumentException(
                ApplibResourceObject.get().InvalidFirstMonth.ex());
        }

        if(month >= firstMonth) {
            month = month - firstMonth;
        } else {
            month = month - firstMonth + 12;
        }

        return month + 1;
    }

    /**
     * @param in Date to convert
     * @param firstMonth First month of fiscal year
     * @return	Fiscal month for this date and year begin month
     * @exception SQLException
     */
    public static int FunctionExecute(Date in, int firstMonth)
    {
        return calculate(in.getMonth() + 1, firstMonth);
    }

    /**
     * @param in Timestamp to convert
     * @param firstMonth First month of fiscal year
     * @return 	Fiscal month for this date and year begin month
     * @exception SQLException
     */
    public static int FunctionExecute(Timestamp in, int firstMonth)
    {
        return calculate(in.getMonth() + 1, firstMonth);
    }
        
}

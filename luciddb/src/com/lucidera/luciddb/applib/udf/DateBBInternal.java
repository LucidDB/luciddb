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

import java.util.Date;

/**
 * Convert a Broadbase internal date to a date string
 *
 * Ported from //bb/bb713/server/SQL/toDateBBInternal.java
 */
public class DateBBInternal
{
    /**
     * @param in Internal Broadbase date (millisec since 1900) 
     * @return String representing local time
     * @exception SQLException thrown when the date passed in cannot be 
     *     converted to a local time string
     */
    public static String FunctionExecute( long in )
    {
        if( in == -1 )
            return null;

        java.util.Date d;

        // Use conversions as in bbdates.h.
        // This gives the delta between Broadbase time 0 and UTC time 0
        long delta = 365 * 1969 + ( 1969 / 4 ) + ( 1969 / 400 ) - ( 1969 / 100 ) + 1;
        delta = delta * 1000 * 60 * 60 * 24;

        // Apply delta to get UTC time from time passed in
        in -= delta;

        d = new java.util.Date( in );
        return d.toString();
    }
}

// End DateBBInternal.java

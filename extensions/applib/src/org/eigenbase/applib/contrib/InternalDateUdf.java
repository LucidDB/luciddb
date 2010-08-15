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
package org.eigenbase.applib.contrib;

import net.sf.farrago.runtime.*;


/**
 * Convert a Broadbase internal date to a date string Ported from
 * //bb/bb713/server/SQL/toDateBBInternal.java
 */
public class InternalDateUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @param in Internal date representation (millisec since 1900)
     *
     * @return String representing local time
     */
    public static String execute(long in)
    {
        if (in == -1) {
            return null;
        }

        java.util.Date d = (java.util.Date) FarragoUdrRuntime.getContext();

        // Use conversions as in bbdates.h.
        // This gives the delta between Broadbase time 0 and UTC time 0
        long delta =
            (365 * 1969) + (1969 / 4) + (1969 / 400)
            - (1969 / 100) + 1;
        delta = delta * 1000 * 60 * 60 * 24;

        // Apply delta to get UTC time from time passed in
        in -= delta;

        if (d == null) {
            d = new java.util.Date(in);
            FarragoUdrRuntime.setContext(d);
            return d.toString();
        }

        d.setTime(in);
        return d.toString();
    }
}

// End InternalDateUdf.java

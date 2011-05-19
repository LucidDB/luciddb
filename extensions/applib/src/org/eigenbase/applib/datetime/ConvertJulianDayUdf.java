/*
// $Id$
// Applib is a library of SQL-invocable routines for Eigenbase applications.
// Copyright (C) 2007 The Eigenbase Project
// Copyright (C) 2007 SQLstream, Inc.
// Copyright (C) 2007 DynamoBI Corporation
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

import java.util.*;

import net.sf.farrago.runtime.*;


/**
 * Julian day conversions, uses simplified conversion formula for backward
 * compatibility
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class ConvertJulianDayUdf
{
    //~ Static fields/initializers ---------------------------------------------

    private static final long MILLISECONDS_PER_DAY = 1000 * 60 * 60 * 24;

    // number of days from the julian calendar start date, Jan 1st -4712
    // (4713 BC) to the epoch, Jan 1st 1970
    private static final long DAYS_FROM_JULIAN_START_TO_EPOCH = 2440588;

    //~ Methods ----------------------------------------------------------------

    private static Calendar julianDayToCal(int jd)
    {
        long ms = (jd - DAYS_FROM_JULIAN_START_TO_EPOCH) * MILLISECONDS_PER_DAY;
        Calendar cal = (Calendar) FarragoUdrRuntime.getContext();
        if (cal == null) {
            cal = Calendar.getInstance();
            FarragoUdrRuntime.setContext(cal);
        }
        cal.setTimeInMillis(ms);

        // clear out time portion
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);

        // adjustment so 2440587 and 2440588 don't both return 1969-12-31
        cal.add(Calendar.DAY_OF_MONTH, 1);
        return cal;
    }

    public static int dateToJulianDay(java.sql.Date dt)
    {
        long jd = dt.getTime();

        // for Dec 31, 1969 and earlier
        if (jd < 0) {
            jd -= MILLISECONDS_PER_DAY;
        }
        return (int) ((jd / MILLISECONDS_PER_DAY)
            + DAYS_FROM_JULIAN_START_TO_EPOCH);
    }

    public static java.sql.Date julianDayToDate(int jd)
    {
        return new java.sql.Date(julianDayToCal(jd).getTimeInMillis());
    }

    public static Timestamp julianDayToTimestamp(int jd)
    {
        return new Timestamp(julianDayToCal(jd).getTimeInMillis());
    }
}

// End ConvertJulianDayUdf.java

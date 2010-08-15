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

import java.text.*;

import java.util.*;

import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * Convert a date to an absolute day number. Useful for having integer date key
 * that will work with the TimeDimension Java table. Ported from
 * //bb/bb713/server/SQL/toDayNumberOverall.java
 */
public class DayNumberOverallUdf
{
    //~ Methods ----------------------------------------------------------------

    // Convert date, of local time zone, to equivalent date in GMT time
    private static java.util.Date convertToGmt(java.util.Date localDate)
        throws ApplibException
    {
        ApplibResource res = ApplibResource.instance();

        DateFormat localFormatter = (DateFormat) FarragoUdrRuntime.getContext();
        DateFormat utcFormatter = (DateFormat) FarragoUdrRuntime.getContext();
        if (localFormatter == null) {
            localFormatter = new SimpleDateFormat(res.LocalDateFormat.str());
            FarragoUdrRuntime.setContext(localFormatter);
            utcFormatter = new SimpleDateFormat(res.UtcDateFormat.str());
            utcFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
            FarragoUdrRuntime.setContext(utcFormatter);
        }

        String dateString = localFormatter.format(localDate);
        java.util.Date gmtDate;
        try {
            gmtDate = utcFormatter.parse(dateString);
        } catch (ParseException e) {
            throw res.DayNumOverallParseError.ex();
        }
        return gmtDate;
    }

    /**
     * @param dtIn Date to convert
     *
     * @return Absolute day number
     *
     * @exception ApplibException
     */
    public static Integer execute(java.sql.Date dtIn)
        throws ApplibException
    {
        if (dtIn == null) {
            return null;
        }
        java.util.Date dt = convertToGmt((java.util.Date) dtIn);
        return (new Integer((int) (dt.getTime() / 1000 / 60 / 60 / 24)));
    }

    /**
     * @param tsIn Timestamp to convert
     *
     * @return Absolute day number
     *
     * @exception ApplibException
     */
    public static Integer execute(Timestamp tsIn)
        throws ApplibException
    {
        if (tsIn == null) {
            return null;
        }
        java.util.Date ts = convertToGmt(tsIn);
        return (new Integer((int) (ts.getTime() / 1000 / 60 / 60 / 24)));
    }
}

// End DayNumberOverallUdf.java

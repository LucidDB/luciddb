/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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

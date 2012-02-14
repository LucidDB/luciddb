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

import org.eigenbase.applib.resource.*;


/**
 * Convert a date to a calendar quarter, with Q1 beginning in January. Ported
 * from //bb/bb713/server/SQL/toCYQuarter.java
 */
public class CalendarQuarterUdf
{
    // NOTE: Both java.sql.Date and java.sql.Timestamp extend java.util.Date.
    // Problem is that the getXXX methods have been deprecated in
    // java.util.Date, so both methods execute the copies of the same code
    // rather than upcasting and executing generic code.

    //~ Methods ----------------------------------------------------------------

    /**
     * @param in Timestamp to convert
     *
     * @return Quarter and calendar year represented by date (e.g., Q3CY96)
     */
    public static String execute(Timestamp in)
    {
        int year = in.getYear();
        int month = in.getMonth() + 1;
        int quarter = ((month - 1) / 3) + 1;
        return (getCalendarQuarter(quarter, year));
    }

    /**
     * @param in Date to convert
     *
     * @return Quarter and calendar year represented by date (e.g., Q3CY96)
     */
    public static String execute(Date in)
    {
        int year = in.getYear();
        int month = in.getMonth() + 1;
        int quarter = ((month - 1) / 3) + 1;
        return (getCalendarQuarter(quarter, year));
    }

    /**
     * Get the string representation of the calendar quarter for a given quarter
     * and year. Ported from
     * //bb/bb713/server/Java/Broadbase/TimeDimensionInternal.java
     *
     * @param quarterIn quarter for string representation of calendar
     * @param yearIn year for string representation of caledar
     */
    public static String getCalendarQuarter(int quarterIn, int yearIn)
    {
        int year =
            ((yearIn % 100) >= 0) ? (yearIn % 100) : (100 + (yearIn % 100));
        String strYear =
            (year < 10) ? ("0" + Integer.toString(year))
            : Integer.toString(year);
        String ret =
            ApplibResource.instance().CalendarQuarter.str(
                Integer.toString(quarterIn),
                strYear);

        return ret;
    }
}

// End CalendarQuarterUdf.java

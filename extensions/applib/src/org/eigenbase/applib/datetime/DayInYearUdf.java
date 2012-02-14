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


/**
 * Convert a date to the day in the year. Ported from
 * //bb/bb713/server/SQL/toDayInYear.java
 */
public class DayInYearUdf
{
    //~ Static fields/initializers ---------------------------------------------

    // 0-based, for day-in-year
    private static final int [] numDays = {
        0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334
    };

    // 0-based, for day-in-year
    private static final int [] leapNumDays = {
        0, 31, 60, 91, 121, 152, 182, 213, 244, 274, 305, 335
    };

    //~ Methods ----------------------------------------------------------------

    /**
     * @param in Date to convert
     *
     * @return Day in year
     */
    public static int execute(Date dt)
    {
        return execute(dt.getYear() + 1900, dt.getMonth() + 1, dt.getDate());
    }

    /**
     * @param in Timestamp to convert
     *
     * @return Day in year
     */
    public static int execute(Timestamp ts)
    {
        return execute(ts.getYear() + 1900, ts.getMonth() + 1, ts.getDate());
    }

    /**
     * @param year Year of date to convert
     * @param month Month of date to convert
     * @param date Day in month of date to convert
     *
     * @return Day in year
     */
    public static int execute(int year, int month, int date)
    {
        // check if leap year
        if (((year % 4) == 0) && (((year % 100) != 0) || ((year % 400) == 0))) {
            return (leapNumDays[month - 1] + date);
        } else {
            return (numDays[month - 1] + date);
        }
    }
}

// End DayInYearUdf.java

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
 * Convert a date to a fiscal quarter, with Q1 beginning in the month specified.
 * Ported from //bb/bb713/server/SQL/toFYQuarter.java
 */
public class FiscalQuarterUdf
{
    //~ Methods ----------------------------------------------------------------

    public static String execute(int year, int month, int firstMonth)
        throws ApplibException
    {
        if ((firstMonth < 1) || (firstMonth > 12)) {
            throw ApplibResource.instance().InvalidFirstMonth.ex();
        }

        if (month >= firstMonth) {
            month = month - firstMonth;
            if (firstMonth > 1) {
                year++;
            }
        } else {
            month = month - firstMonth + 12;
        }

        int quarter = (month / 3) + 1;
        int intYear = ((year % 100) >= 0) ? (year % 100) : (100 + (year % 100));

        String strYear =
            (intYear < 10) ? ("0" + Integer.toString(intYear))
            : Integer.toString(intYear);

        String ret =
            ApplibResource.instance().FiscalYearQuarter.str(
                Integer.toString(quarter),
                strYear);
        return ret;
    }

    /**
     * @param in Date to convert
     * @param firstMonth First month of fiscal year (January = 1)
     *
     * @return Quarter and fiscal year represented by date (e.g., Q1FY97)
     *
     * @exception ApplibException thrown when the mask is invalid, or the string
     * is invalid for the specified mask.
     */
    public static String execute(Date in, int firstMonth)
        throws ApplibException
    {
        if ((firstMonth < 1) || (firstMonth > 12)) {
            throw ApplibResource.instance().InvalidFirstMonth.ex();
        }

        int year = in.getYear();
        int month = in.getMonth() + 1;
        return execute(year, month, firstMonth);
    }

    /*
     * @param in Timestamp to convert
     * @param firstMonth First month of fiscal year (January = 1)
     * @return Quarter and fiscal year represented by date (e.g., Q1FY97)
     * @exception ApplibException thrown when the mask is invalid, or the
     *      string is invalid for the specified mask.
     */
    public static String execute(Timestamp in, int firstMonth)
        throws ApplibException
    {
        if ((firstMonth < 1) || (firstMonth > 12)) {
            throw ApplibResource.instance().InvalidFirstMonth.ex();
        }

        int year = in.getYear();
        int month = in.getMonth() + 1;
        return execute(year, month, firstMonth);
    }
}

// End FiscalQuarterUdf.java

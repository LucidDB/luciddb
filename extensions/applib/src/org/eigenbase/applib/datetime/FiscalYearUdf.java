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
 * Convert a date to a fiscal year, with Q1 beginning in the month specified.
 * Ported from //bb/bb713/server/SQL/toFYYear.java
 */
public class FiscalYearUdf
{
    //~ Methods ----------------------------------------------------------------

    private static int calculate(int year, int month, int firstMonth)
        throws ApplibException
    {
        if ((firstMonth < 1) || (firstMonth > 12)) {
            throw ApplibResource.instance().InvalidFirstMonth.ex();
        }

        if ((firstMonth != 1) && (month >= firstMonth)) {
            year++;
        }

        return year + 1900;
    }

    /**
     * @param in Date to convert
     * @param firstMonth First month of fiscal year (January = 1)
     *
     * @return Fiscal year represented by date
     *
     * @exception ApplibException
     */
    public static int execute(Date in, int firstMonth)
        throws ApplibException
    {
        return calculate(in.getYear(), in.getMonth() + 1, firstMonth);
    }

    /**
     * @param in Timestamp to convert
     * @param firstMonth First month of fiscal year (January = 1)
     *
     * @return Fiscal year represented by date
     *
     * @exception ApplibException
     */
    public static int execute(Timestamp in, int firstMonth)
        throws ApplibException
    {
        return calculate(in.getYear(), in.getMonth() + 1, firstMonth);
    }
}

// End FiscalYearUdf.java

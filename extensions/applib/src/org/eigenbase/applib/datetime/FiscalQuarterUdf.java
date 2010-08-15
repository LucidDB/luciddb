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

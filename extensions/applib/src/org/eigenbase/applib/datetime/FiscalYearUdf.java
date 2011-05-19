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

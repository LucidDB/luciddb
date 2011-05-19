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
package org.eigenbase.applib.test;

import org.eigenbase.applib.resource.*;


/**
 * ApplibResource tests
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class ApplibResourceTest
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Test function to try ApplibResource messages
     */
    public static int tryApplibResourceStr()
    {
        ApplibResource res = ApplibResource.instance();

        System.out.println(
            "Fiscal Year Quarter:"
            + res.FiscalYearQuarter.str("6", "11"));
        System.out.println(
            "Exception as message:"
            + res.LenSpecifyNonNegative.str());
        return Integer.parseInt(res.PhoneLocalAreaCode.str());
    }

    /**
     * Test function to try ApplibResource exception
     */
    public static int tryApplibResourceEx(boolean b)
        throws ApplibException
    {
        if (b) {
            throw ApplibResource.instance().InvalidFirstMonth.ex();
        } else {
            return 0;
        }
    }
}

// End ApplibResourceTest.java

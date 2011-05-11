/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  
*/
package com.lucidera.luciddb.applib.test;

import com.lucidera.luciddb.applib.resource.*;

/**
 * ApplibResource tests
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public class ApplibResourceTest
{
    /**
     * Test function to try ApplibResource messages
     */
    public static int tryApplibResourceStr()
    {
        ApplibResource res = ApplibResourceObject.get();

        System.out.println("Fiscal Year Quarter:"
            + res.FiscalYearQuarter.str("6", "11"));
        System.out.println("Exception as message:"
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
            throw ApplibResourceObject.get().InvalidFirstMonth.ex();
        } else {
            return 0;
        }
    }

}

// End ApplibResourceTest.java

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

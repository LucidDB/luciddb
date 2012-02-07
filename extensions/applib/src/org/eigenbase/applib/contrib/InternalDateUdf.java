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
package org.eigenbase.applib.contrib;

import net.sf.farrago.runtime.*;


/**
 * Convert a Broadbase internal date to a date string Ported from
 * //bb/bb713/server/SQL/toDateBBInternal.java
 */
public class InternalDateUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * @param in Internal date representation (millisec since 1900)
     *
     * @return String representing local time
     */
    public static String execute(long in)
    {
        if (in == -1) {
            return null;
        }

        java.util.Date d = (java.util.Date) FarragoUdrRuntime.getContext();

        // Use conversions as in bbdates.h.
        // This gives the delta between Broadbase time 0 and UTC time 0
        long delta =
            (365 * 1969) + (1969 / 4) + (1969 / 400)
            - (1969 / 100) + 1;
        delta = delta * 1000 * 60 * 60 * 24;

        // Apply delta to get UTC time from time passed in
        in -= delta;

        if (d == null) {
            d = new java.util.Date(in);
            FarragoUdrRuntime.setContext(d);
            return d.toString();
        }

        d.setTime(in);
        return d.toString();
    }
}

// End InternalDateUdf.java

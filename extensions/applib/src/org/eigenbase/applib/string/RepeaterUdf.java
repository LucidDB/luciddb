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
package org.eigenbase.applib.string;

import org.eigenbase.applib.resource.*;


/**
 * Repeater returns the input string repeated N times Ported from
 * //bb/bb713/server/SQL/repeater.java
 */
public class RepeaterUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Ported from //bb/bb713/server/SQL/BBString.java
     *
     * @param in String to be repeated
     * @param times Number of times to repeat string
     *
     * @return The repeated string
     *
     * @exception ApplibException
     */
    public static String execute(String in, int times)
    {
        if (times < 0) {
            throw ApplibResource.instance().RepSpecifyNonNegative.ex();
        }

        int len = in.length();

        // clip maximum size of output to 64k
        if ((times * len) > (64 * 1024)) {
            times = (64 * 1024) / len;
        }

        char [] outArray = new char[times * len];
        char [] inArray = in.toCharArray();

        for (int i = 0; i < times; i++) {
            System.arraycopy(inArray, 0, outArray, i * len, len);
        }

        return new String(outArray);
    }
}

// End RepeaterUdf.java

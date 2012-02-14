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
 * InStr UDF function returns the location of a substring in a string. The first
 * position in a string is 1. Returns 0 if subtring is not found.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class InStrUdf
{
    //~ Static fields/initializers ---------------------------------------------

    public static final int MAX_VARCHAR_PRECISION = 65535;

    //~ Methods ----------------------------------------------------------------

    /**
     * @param inStr string to search
     * @param subStr substring to search for in inStr
     * @param startPos the position in inStr to start the search, if negative,
     * function counts back startPos number of characters from the end of inStr
     * and then searches towards the beginning of inStr.
     * @param nthAppearance the number of appearance to search for.
     *
     * @return the position of the nthAppearance occurrence of subStr in inStr
     * starting from startPos, or 0 if not found.
     */
    public static int execute(
        String inStr,
        String subStr,
        int startPos,
        int nthAppearance)
    {
        int position, i;
        int inStrLen = inStr.length();
        int subStrLen = subStr.length();

        if ((startPos == 0)
            || (nthAppearance < 1)
            || (startPos > inStrLen)
            || (inStrLen > MAX_VARCHAR_PRECISION)
            || (subStrLen > MAX_VARCHAR_PRECISION))
        {
            throw ApplibResource.instance().InStrInvalidArgument.ex(
                inStr,
                subStr,
                String.valueOf(startPos),
                String.valueOf(nthAppearance));
        }

        // returns 0 is substring is longer than instring or if substring
        // length is 0
        if ((subStrLen > inStrLen) || (subStrLen == 0)) {
            return 0;
        }

        if (startPos > 0) {
            // startPos is positive
            position = startPos - 1;
            for (i = 0; i < nthAppearance; i++) {
                if ((inStrLen - position) < subStrLen) {
                    return 0;
                }
                position = inStr.indexOf(subStr, position);
                if (position == -1) {
                    return 0;
                }
                position = position + 1;
            }
        } else {
            // startPos is negative search backwards
            position = inStr.length() + startPos;
            for (i = 0; i < nthAppearance; i++) {
                position = inStr.lastIndexOf(subStr, position);
                if (position == -1) {
                    return 0;
                }
                position = position - 1;
            }
            position = position + 2;
        }
        return position;
    }

    /**
     * @param inStr string to search
     * @param subStr substring to search for in inStr
     *
     * @return the 1st occurrence of subStr in inStr starting from the beginning
     * of the string, or 0 if not found.
     */
    public static int execute(String inStr, String subStr)
    {
        return execute(inStr, subStr, 1, 1);
    }
}

// End InStrUdf.java

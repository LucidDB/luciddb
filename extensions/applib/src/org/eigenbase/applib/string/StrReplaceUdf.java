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

/**
 * strReplace replaces all occurences of the old string with the new string
 * Ported from //bb/bb713/server/SQL/strReplace.java
 */
public class StrReplaceUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Ported from //bb/bb713/server/SQL/BBString.java strReplace method
     *
     * @param in Input string
     * @param oldStr String to be replaced in the input string
     * @param newStr String to replace oldStr in the input string
     *
     * @return New String will all occurences of oldStr replaced with newStr
     */
    public static String execute(String in, String oldStr, String newStr)
    {
        if ((in == null) || (oldStr == null)) {
            return in;
        }

        int prevPos = 0;
        int currPos = 0;
        int incrLen = oldStr.length();

        if (incrLen == 0) {
            return in;
        }

        StringBuffer retVal = new StringBuffer();

        try {
            while ((currPos = in.indexOf(oldStr, prevPos)) != -1) {
                retVal.append(in.substring(prevPos, currPos));
                retVal.append(newStr);
                prevPos = currPos + incrLen;
            }
            retVal.append(in.substring(prevPos));
        } catch (StringIndexOutOfBoundsException e) {
        }

        return retVal.toString();
    }
}

// End StrReplaceUdf.java

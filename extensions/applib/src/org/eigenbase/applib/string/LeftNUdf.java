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

import java.nio.*;

import net.sf.farrago.runtime.*;

import org.eigenbase.applib.resource.*;


/**
 * leftN returns the first N characters of the string Ported from
 * //bb/bb713/server/SQL/leftN.java
 */
public class LeftNUdf
{
    //~ Methods ----------------------------------------------------------------

    public static String execute(String in, int len)
    {
        if (len < 0) {
            throw ApplibResource.instance().LenSpecifyNonNegative.ex();
        }

        char [] chars;
        CharBuffer cb = (CharBuffer) FarragoUdrRuntime.getContext();
        if (cb == null) {
            chars = new char[2048];
            cb = CharBuffer.wrap(chars);
            FarragoUdrRuntime.setContext(cb);
        }
        chars = cb.array();

        // we want either the left len characters or all the characters if the
        // total length of the string is already less than len
        int maxLen = Math.min(in.length(), len);

        // make sure we don't overflow our buffer
        // save one space for a null terminator
        if (maxLen >= chars.length) {
            maxLen = chars.length - 1;
        }

        // get the characters
        in.getChars(0, maxLen, chars, 0);

        // null terminate
        chars[maxLen] = 0;

        return new String(chars, 0, maxLen);
    }
}

// End LeftNUdf.java

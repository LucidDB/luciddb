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
 * rightN returns the last N characters of the string Ported from
 * //bb/bb713/server/SQL/rightN.java
 */
public class RightNUdf
{
    //~ Methods ----------------------------------------------------------------

    /**
     * Ported from //bb/bb713/server/SQL/BBString.java
     *
     * @param in Input string
     * @param len N number of characters to return
     *
     * @return New String with last N characters of the input string
     *
     * @exception ApplibException
     */
    public static String execute(String in, int len)
        throws ApplibException
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

        int inlen = in.length();
        int startPos = Math.max(inlen - len, 0);
        in.getChars(startPos, inlen, chars, 0);

        return new String(chars, 0, inlen - startPos);
    }
}

// End RightNUdf.java

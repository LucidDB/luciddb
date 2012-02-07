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
 * CharReplace replaces all occurrences of the old character in a string with
 * the new character. Ported from //BB/bb713/server/SQL/charReplace.java
 */
public abstract class CharReplaceUdf
{
    //~ Methods ----------------------------------------------------------------

    public static String execute(String in, int oldChar, int newChar)
    {
        return in.replace((char) oldChar, (char) newChar);
    }

    public static String execute(
        String in,
        String oldChar,
        String newChar)
    {
        ApplibResource res = ApplibResource.instance();

        if (oldChar.length() != 1) {
            throw res.ReplacedCharSpecifyOneChar.ex();
        }
        if (newChar.length() != 1) {
            throw res.ReplacementCharSpecifyOneChar.ex();
        }

        return in.replace(oldChar.charAt(0), newChar.charAt(0));
    }
}

// End CharReplaceUdf.java

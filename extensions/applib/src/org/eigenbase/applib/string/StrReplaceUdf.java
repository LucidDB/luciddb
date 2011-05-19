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

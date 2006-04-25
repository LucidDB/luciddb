/*
// $Id$
// LucidDB is a DBMS optimized for business intelligence.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/
package com.lucidera.luciddb.applib.string;

import com.lucidera.luciddb.applib.resource.*;

/**
 * InStr UDF function returns the location of a substring in a string. The 
 * first position in a string is 1. Returns 0 if subtring is not found.
 *
 * @author Elizabeth Lin
 * @version $Id$
 */
public abstract class InStrUdf
{

    public static final int MAX_VARCHAR_PRECISION = 65535;
    /**
     * @param inStr string to search
     * @param subStr substring to search for in inStr
     * @param startPos the position in inStr to start the search, if negative,
     *   function counts back startPos number of characters from the end of 
     *   inStr and then searches towards the beginning of inStr.
     * @param nthAppearance the number of appearance to search for.
     * @return the position of the nthAppearance occurrence of subStr in inStr
     *   starting from startPos, or 0 if not found.
     */
    public static int execute(String inStr, String subStr, int startPos, 
        int nthAppearance)
    {
        int position, i;
        int inStrLen = inStr.length();
        int subStrLen = subStr.length();

        if (startPos == 0 || nthAppearance < 1 || startPos > inStrLen || 
            subStrLen == 0 || subStrLen > inStrLen || 
            inStrLen > MAX_VARCHAR_PRECISION || 
            subStrLen > MAX_VARCHAR_PRECISION) 
        {
            throw ApplibResourceObject.get().InStrInvalidArgument.ex(
                inStr, subStr, String.valueOf(startPos), 
                String.valueOf(nthAppearance));
        }

        if (startPos > 0) {
            // startPos is positive
            position = startPos - 1;
            for (i=0; i < nthAppearance; i++) {
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
            for (i=0; i < nthAppearance; i++) {
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
     * @return the 1st occurrence of subStr in inStr starting from the 
     *   beginning of the string, or 0 if not found.
     */
    public static int execute(String inStr, String subStr)
    {
        return execute(inStr, subStr, 1, 1);
    }
}

// End InStrUdf.java

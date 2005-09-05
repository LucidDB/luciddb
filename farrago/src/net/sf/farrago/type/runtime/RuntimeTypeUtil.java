/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2005-2005 Xiaoyang Luo
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
package net.sf.farrago.type.runtime;

import net.sf.farrago.resource.*;

import org.eigenbase.util.*;
import org.eigenbase.sql.fun.SqlTrimFunction;

import java.io.*;
import java.nio.*;


/**
 * Runtime Utility Subroutine.
 *
 * @author Xiaoyang Luo
 * @version $Id$
 */
public class RuntimeTypeUtil
{

    /** 
     * Translate the like pattern to java's regex pattern.
     */

    private static final String javaRegexSpecials = "([{}])$^|?*-+\\";

    public static String SqlToRegexLike(String sqlPattern, String escapeStr)
    { 
        int i;
        char escapeChar = (char) 0;
        if (escapeStr != null) {
            if (escapeStr.length() != 1) {
                throw net.sf.farrago.resource.FarragoResource.instance().newInvalidEscapeCharacter();
            }
            escapeChar = escapeStr.charAt(0);
        }
        int len = sqlPattern.length();
        StringBuffer javaPattern = new StringBuffer(len + len);
        javaPattern.append("\\A");
        for (i = 0; i < len; i++) {
            char c = sqlPattern.charAt(i);
            if (javaRegexSpecials.indexOf(c)>= 0) {
                javaPattern.append('\\');
            }
            if (c == escapeChar) {
                if (i == sqlPattern.length()-1) {
                    throw net.sf.farrago.resource.FarragoResource.instance().newInvalidEscapeSequence();
                }
                char nextChar = sqlPattern.charAt(i+1);
                if (nextChar == '_' || nextChar == '%' || nextChar == escapeChar) {
                    javaPattern.append(nextChar);
                    i++;
                } else {
                    throw net.sf.farrago.resource.FarragoResource.instance().newInvalidEscapeSequence();
                }
            } else if (c == '_') {
                javaPattern.append('.');
            } else if (c == '%') {
                javaPattern.append(".");
                javaPattern.append('*');
            } else {
                javaPattern.append(c);
            }
        }
        javaPattern.append("\\Z");
        return javaPattern.toString();
    }
    public static String SqlToRegexSimilar(String sqlPattern, String escapeStr)
    {
        // not implemented yet.
        assert(false);
        return null;
    }
}


// End RuntimeTypeUtil.java

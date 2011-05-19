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

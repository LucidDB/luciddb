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
package com.lucidera.luciddb.applib;

import java.sql.Types;

/**
 * CharReplace replaces all occurrences of the old character in a string
 * with the new character.
 *
 * Ported from //BB/bb713/server/SQL/charReplace.java
*/
public abstract class CharReplace
{
    public static String FunctionExecute(String in, int oldChar, int newChar)
    {
        return in.replace((char)oldChar, (char)newChar);
    }
    
    public static String FunctionExecute(
        String in, String oldChar, String newChar)
    {
        ApplibResource res = ApplibResourceObject.get();

        if(oldChar.length() != 1) {
            throw new IllegalArgumentException(
                res.ReplacedCharSpecifyOneChar.ex());
        }
        if(newChar.length() != 1) {
            throw new IllegalArgumentException(
                res.ReplacementCharSpecifyOneChar.ex());
        }

        return in.replace(oldChar.charAt(0), newChar.charAt(0));
    }
}

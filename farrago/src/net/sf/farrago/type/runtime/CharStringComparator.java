/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.type.runtime;

import java.util.*;

/**
 * A very slow, generic comparator for two objects with valid toString()
 * methods.  It implements SQL character comparison semantics (rtrim before
 * compare).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CharStringComparator implements Comparator
{
    public static final int compareCharStrings(Object o1,Object o2)
    {
        String s1 = rtrim(o1.toString());
        String s2 = rtrim(o2.toString());
        return s1.compareTo(s2);
    }
    
    public int compare(Object o1,Object o2)
    {
        return compareCharStrings(o1,o2);
    }

    private static String rtrim(String s)
    {
        int n = s.length() - 1;
        if (s.charAt(n) != ' ') {
            return s;
        }
        for (--n; n >= 0; --n) {
            if (s.charAt(n) != ' ') {
                return s.substring(0,n+1);
            }
        }
        return "";
    }
}

// End CharStringComparator.java

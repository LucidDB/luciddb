/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.*;

import org.eigenbase.util.*;

/**
 * A very slow, generic comparator for two objects with valid toString()
 * methods. It implements SQL character comparison semantics (rtrim before
 * compare).
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class CharStringComparator
    implements Comparator
{

    //~ Methods ----------------------------------------------------------------

    public static final int compareCharStrings(
        Object o1,
        Object o2)
    {
        String s1 = Util.rtrim(o1.toString());
        String s2 = Util.rtrim(o2.toString());
        return s1.compareTo(s2);
    }

    public int compare(
        Object o1,
        Object o2)
    {
        return compareCharStrings(o1, o2);
    }
}

// End CharStringComparator.java

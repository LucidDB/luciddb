/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
package org.eigenbase.util;

/**
 * <code>HashableArray</code> provides a <code>Object[]</code> with a {@link
 * #hashCode} and an {@link #equals} function, so it can be used as a key in a
 * {@link java.util.Hashtable}.
 */
public class HashableArray
{
    //~ Instance fields --------------------------------------------------------

    Object [] a;

    //~ Constructors -----------------------------------------------------------

    public HashableArray(Object [] a)
    {
        this.a = a;
    }

    //~ Methods ----------------------------------------------------------------

    // override Object
    public int hashCode()
    {
        return arrayHashCode(a);
    }

    // override Object
    public boolean equals(Object o)
    {
        return (o instanceof HashableArray)
            && arraysAreEqual(this.a, ((HashableArray) o).a);
    }

    public static int arrayHashCode(Object [] a)
    {
        // hash algorithm borrowed from java.lang.String
        int h = 0;
        for (int i = 0; i < a.length; i++) {
            h = (31 * h) + a[i].hashCode();
        }
        return h;
    }

    /**
     * Returns whether two arrays are equal (deep compare).
     */
    public static boolean arraysAreEqual(Object [] a1, Object [] a2)
    {
        if (a1.length != a2.length) {
            return false;
        }
        for (int i = 0; i < a1.length; i++) {
            if (!a1[i].equals(a2[i])) {
                return false;
            }
        }
        return true;
    }
}

// End HashableArray.java

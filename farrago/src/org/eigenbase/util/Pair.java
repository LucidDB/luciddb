/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import java.util.*;


/**
 * Pair of objects.
 *
 * @author jhyde
 * @version $Id$
 * @since Oct 17, 2007
 */
public class Pair<T1, T2>
{
    //~ Instance fields --------------------------------------------------------

    public final T1 left;
    public final T2 right;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a Pair.
     *
     * @param left left value
     * @param right right value
     */
    public Pair(T1 left, T2 right)
    {
        this.left = left;
        this.right = right;
    }

    //~ Methods ----------------------------------------------------------------

    public boolean equals(Object obj)
    {
        return (obj instanceof Pair)
            && Util.equal(this.left, ((Pair) obj).left)
            && Util.equal(this.right, ((Pair) obj).right);
    }

    public int hashCode()
    {
        int h1 = Util.hash(0, left);
        return Util.hash(h1, right);
    }

    /**
     * Converts a collection of Pairs into a Map.
     *
     * <p>This is an obvious thing to do because Pair is similar in structure to
     * {@link java.util.Map.Entry}.
     *
     * <p>The map contains a copy of the collection of Pairs; if you change the
     * collection, the map does not change.
     *
     * @param pairs Collection of Pair objects
     *
     * @return map with the same contents as the collection
     */
    public static <K, V> Map<K, V> toMap(Collection<Pair<K, V>> pairs)
    {
        HashMap<K, V> map = new HashMap<K, V>();
        for (Pair<K, V> pair : pairs) {
            map.put(pair.left, pair.right);
        }
        return map;
    }
}

// End Pair.java

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
 * Extension to {@link ArrayList} to help build an array of <code>int</code>
 * values.
 *
 * @author jhyde
 * @version $Id$
 */
public class IntList
    extends ArrayList<Integer>
{
    //~ Methods ----------------------------------------------------------------

    public int [] toIntArray()
    {
        return toArray(this);
    }

    /**
     * Converts a list of {@link Integer} objects to an array of primitive
     * <code>int</code>s.
     *
     * @param integers List of Integer objects
     *
     * @return Array of primitive <code>int</code>s
     */
    public static int [] toArray(List<Integer> integers)
    {
        final int [] ints = new int[integers.size()];
        for (int i = 0; i < ints.length; i++) {
            ints[i] = integers.get(i);
        }
        return ints;
    }

    /**
     * Returns a list backed by an array of primitive <code>int</code> values.
     *
     * <p>The behavior is analogous to {@link Arrays#asList(Object[])}. Changes
     * to the list are reflected in the array. The list cannot be extended.
     *
     * @param args Array of primitive <code>int</code> values
     *
     * @return List backed by array
     */
    public static List<Integer> asList(final int [] args)
    {
        return new AbstractList<Integer>() {
            public Integer get(int index)
            {
                return args[index];
            }

            public int size()
            {
                return args.length;
            }

            public Integer set(int index, Integer element)
            {
                return args[index] = element;
            }
        };
    }
}

// End IntList.java

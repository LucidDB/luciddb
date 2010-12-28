/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2009 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2009 Dynamo BI Corporation
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
 * Read-only list that is the concatenation of sub-lists.
 *
 * <p>The list is read-only; attempts to call methods such as
 * {@link #add(Object)} or {@link #set(int, Object)} will throw.
 *
 * <p>Changes to the backing lists, including changes in length, will be
 * reflected in this list.
 *
 * <p>This class is not thread-safe. Changes to backing lists will cause
 * unspecified behavior.
 *
 * @param <T> Element type
 *
 * @author jhyde
 * @version $Id$
 * @since 31 October, 2009
 */
public class CompositeList<T> extends AbstractList<T>
{
    private final List<T>[] lists;

    /**
     * Creates a CompoundList.
     *
     * @param lists Constituent lists
     */
    public CompositeList(List<T>... lists)
    {
        this.lists = lists;
    }

    /**
     * Creates a CompoundList.
     *
     * <p>More convenient than {@link #CompositeList(java.util.List[])},
     * because element type is inferred. Use this method as you would
     * {@link java.util.Arrays#asList(Object[])} or
     * {@link java.util.EnumSet#of(Enum, Enum[])}.
     *
     * @param lists Consistituent lists
     * @param <T> Element type
     * @return List consisting of all lists
     */
    public static <T> CompositeList<T> of(List<T>... lists)
    {
        return new CompositeList<T>(lists);
    }

    public T get(int index)
    {
        for (List<T> list : lists) {
            int nextIndex = index - list.size();
            if (nextIndex < 0) {
                return list.get(index);
            }
            index = nextIndex;
        }
        throw new IndexOutOfBoundsException();
    }

    public int size()
    {
        int n = 0;
        for (List<T> list : lists) {
            n += list.size();
        }
        return n;
    }
}

// End CompositeList.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2011 The Eigenbase Project
// Copyright (C) 2011 SQLstream, Inc.
// Copyright (C) 2011 Dynamo BI Corporation
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
* Set based on object identity, like {@link IdentityHashMap}.
 *
 * @version $Id$
 * @author jhyde
*/
public class IdentityHashSet<E> extends AbstractSet<E> implements Set<E>
{
    private final Map<E, Object> map;

    // Dummy value to associate with an Object in the backing Map
    private static final Object PRESENT = new Object();

    /**
     * Creates an empty IdentityHashSet.
     */
    public IdentityHashSet()
    {
        map = new IdentityHashMap<E, Object>();
    }

    /**
     * Creates an IdentityHashSet containing the elements of the specified
     * collection.
     *
     * @param c the collection whose elements are to be placed into this set
     */
    public IdentityHashSet(Collection<? extends E> c)
    {
        map = new IdentityHashMap<E, Object>(Math.max(c.size() * 2 + 1, 16));
        addAll(c);
    }

    public int size()
    {
        return map.size();
    }

    public boolean isEmpty()
    {
        return map.isEmpty();
    }

    public boolean contains(Object o)
    {
        //noinspection SuspiciousMethodCalls
        return map.containsKey(o);
    }

    public Iterator<E> iterator()
    {
        return map.keySet().iterator();
    }

    public Object[] toArray()
    {
        return map.keySet().toArray();
    }

    public <T> T[] toArray(T[] a)
    {
        //noinspection SuspiciousToArrayCall
        return map.keySet().toArray(a);
    }

    public boolean add(E e)
    {
        return map.put(e, PRESENT) != null;
    }

    public boolean remove(Object o)
    {
        return map.remove(o) != null;
    }

    public void clear()
    {
        map.clear();
    }
}

// End IdentityHashSet.java

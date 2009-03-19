/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
 * Converts a list whose members are automatically down-cast to a given type.
 *
 * <p>If a member of the backing list is not an instanceof <code>E</code>, the
 * accessing method (such as {@link List#get}) will throw a {@link
 * ClassCastException}.
 *
 * <p>All modifications are automatically written to the backing list. Not
 * synchronized.
 *
 * @author jhyde
 * @version $Id$
 */
public class CastingList<E>
    extends AbstractList<E>
    implements List<E>
{
    //~ Instance fields --------------------------------------------------------

    private final List<? super E> list;
    private final Class<E> clazz;

    //~ Constructors -----------------------------------------------------------

    protected CastingList(List<? super E> list, Class<E> clazz)
    {
        super();
        this.list = list;
        this.clazz = clazz;
    }

    //~ Methods ----------------------------------------------------------------

    public E get(int index)
    {
        return clazz.cast(list.get(index));
    }

    public int size()
    {
        return list.size();
    }

    public E set(int index, E element)
    {
        final Object o = list.set(index, element);
        return clazz.cast(o);
    }

    public E remove(int index)
    {
        return clazz.cast(list.remove(index));
    }

    public void add(int pos, E o)
    {
        list.add(pos, o);
    }
}

// End CastingList.java

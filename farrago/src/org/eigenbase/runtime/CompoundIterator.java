/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

package org.eigenbase.runtime;

import java.util.Collections;
import java.util.Iterator;


/**
 * <code>CompoundIterator</code> creates an iterator out of several.
 */
public class CompoundIterator implements Iterator
{
    //~ Instance fields -------------------------------------------------------

    private Iterator iterator;
    private Iterator [] iterators;
    private int i;

    //~ Constructors ----------------------------------------------------------

    public CompoundIterator(Iterator [] iterators)
    {
        this.iterators = iterators;
        this.i = 0;
    }

    //~ Methods ---------------------------------------------------------------

    public boolean hasNext()
    {
        if (iterator == null) {
            nextIterator();
        }
        return iterator.hasNext();
    }

    public Object next()
    {
        Object o = iterator.next();
        if (!iterator.hasNext()) {
            nextIterator();
        }
        return o;
    }

    public void remove()
    {
        iterator.remove();
    }

    private void nextIterator()
    {
        while (i < iterators.length) {
            iterator = iterators[i++];
            if (iterator.hasNext()) {
                return;
            }
        }
        iterator = Collections.EMPTY_LIST.iterator();
    }
}


// End CompoundIterator.java

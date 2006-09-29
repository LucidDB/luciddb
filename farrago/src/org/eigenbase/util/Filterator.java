/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2004-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.List;


/**
 * Filtered iterator class:  an iterator that includes only
 * elements that are instanceof a specified class.  Apologies
 * for the dorky name.
 *
 * @see Util#cast(List, Class)
 * @see Util#cast(Iterator, Class)
 *
 * @author jason
 * @since November 9, 2004
 * @version $Id$
 **/
public class Filterator<E> implements Iterator<E>
{
    //~ Instance fields -------------------------------------------------------

    Class<E> includeFilter;
    Iterator<? extends Object> iterator;
    E lookAhead;
    boolean ready;

    //~ Constructors ----------------------------------------------------------

    public Filterator(
        Iterator<?> iterator,
        Class<E> includeFilter)
    {
        this.iterator = iterator;
        this.includeFilter = includeFilter;
    }

    //~ Methods ---------------------------------------------------------------

    /* (non-Javadoc)
     * @see java.util.Iterator#hasNext()
     */
    public boolean hasNext()
    {
        // look ahead to see if there are any additional elements
        try {
            lookAhead = next();
            ready = true;
            return true;
        } catch (NoSuchElementException e) {
            ready = false;
            return false;
        }
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#next()
     */
    public E next()
    {
        if (ready) {
            E o = lookAhead;
            ready = false;
            return o;
        }

        while (iterator.hasNext()) {
            Object o = iterator.next();
            if (includeFilter.isInstance(o)) {
                return includeFilter.cast(o);
            }
        }
        throw new NoSuchElementException();
    }

    /* (non-Javadoc)
     * @see java.util.Iterator#remove()
     */
    public void remove()
    {
        iterator.remove();
    }
}

// End Filterator.java

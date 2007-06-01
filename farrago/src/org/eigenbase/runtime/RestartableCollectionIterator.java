/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
package org.eigenbase.runtime;

import java.util.*;


/**
 * <code>RestartableCollectionIterator</code> implements the {@link
 * RestartableIterator} interface in terms of an underlying {@link Collection}.
 *
 * <p>TODO jvs 21-Mar-2006: This class is no longer used except by Saffron, so
 * we should move it to Saffron.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RestartableCollectionIterator
    implements RestartableIterator
{
    //~ Instance fields --------------------------------------------------------

    private final Collection collection;
    private Iterator iterator;

    //~ Constructors -----------------------------------------------------------

    public RestartableCollectionIterator(Collection collection)
    {
        this.collection = collection;
        iterator = collection.iterator();
    }

    //~ Methods ----------------------------------------------------------------

    // implement Iterator
    public Object next()
    {
        return iterator.next();
    }

    // implement Iterator
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    // implement Iterator
    public void remove()
    {
        iterator.remove();
    }

    // implement RestartableIterator
    public void restart()
    {
        iterator = collection.iterator();
    }
}

// End RestartableCollectionIterator.java

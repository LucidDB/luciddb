/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2004-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2004-2006 John V. Sichi
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
 * <code>RestartableCollectionTupleIter</code> implements the {@link TupleIter}
 * interface in terms of an underlying {@link Collection}. It is used to
 * implement {@link org.eigenbase.oj.rel.IterOneRowRel}.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class RestartableCollectionTupleIter
    implements TupleIter
{

    //~ Instance fields --------------------------------------------------------

    private final Collection collection;
    private Iterator iterator;

    //~ Constructors -----------------------------------------------------------

    // this handles the case where we thought a join was one-to-many
    // but it's actually one-to-one
    public RestartableCollectionTupleIter(Object obj)
    {
        if (obj instanceof Collection) {
            collection = (Collection) obj;
        } else {
            collection = Collections.singleton(obj);
        }
        iterator = collection.iterator();
    }

    public RestartableCollectionTupleIter(Collection collection)
    {
        this.collection = collection;
        iterator = collection.iterator();
    }

    //~ Methods ----------------------------------------------------------------

    // implement TupleIter
    public Object fetchNext()
    {
        if (iterator.hasNext()) {
            return iterator.next();
        }

        return NoDataReason.END_OF_DATA;
    }

    // implement TupleIter
    public void restart()
    {
        iterator = collection.iterator();
    }

    // implement TupleIter
    public void closeAllocation()
    {
        iterator = null;
    }
}

// End RestartableCollectionTupleIter.java

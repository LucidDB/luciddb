/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
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
package org.eigenbase.runtime;

import java.util.*;


/**
 * <code>RestartableIteratorTupleIter</code> implements the {@link TupleIter}
 * interface in terms of an underlying {@link RestartableIterator}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RestartableIteratorTupleIter
    implements TupleIter
{
    //~ Instance fields --------------------------------------------------------

    private final RestartableIterator iterator;

    //~ Constructors -----------------------------------------------------------

    public RestartableIteratorTupleIter(RestartableIterator iterator)
    {
        this.iterator = iterator;
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
        iterator.restart();
    }

    // implement TupleIter
    public void closeAllocation()
    {
    }
}

// End RestartableIteratorTupleIter.java

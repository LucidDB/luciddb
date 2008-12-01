/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2008 The Eigenbase Project
// Copyright (C) 2006-2008 Disruptive Tech
// Copyright (C) 2006-2008 LucidEra, Inc.
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

// REVIEW mberkowitz 1-Nov-2008.
// This adapter is used only to present a FarragoJavaUdxIterator as a TupleIter.
// Redundant since a FarragoJavaUdxIterator can be a TupleIter itself, and provide a
// correct, non-blocking fetchNext(). However some farrago queries depend on
// fetchNext() to block: eg in unitsql/expressions/udfInvocation.sql,
// SELECT * FROM TABLE(RAMP(5)) ORDER BY 1;
//
// Consequently, I've made FarragoJavaUdxIterator implement TupleIter as well as
// RestartableIterator, but as a kludge I've retained this adapter for farrago
// queries.


/**
 * <code>RestartableIteratorTupleIter</code> adapts an underlying
 * {@link RestartableIterator} as a {@link TupleIter}.
 * It is an imperfect adaptor; {@link #fetchNext} blocks when a
 * real TupleIter would return {@link TupleIter.NoDataReason#UNDERFLOW}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class RestartableIteratorTupleIter
    extends AbstractTupleIter
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
            // blocks
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

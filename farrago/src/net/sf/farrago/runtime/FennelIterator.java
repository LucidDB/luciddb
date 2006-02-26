/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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
package net.sf.farrago.runtime;

import net.sf.farrago.fennel.FennelStreamGraph;
import net.sf.farrago.fennel.FennelStreamHandle;

import org.eigenbase.runtime.RestartableIterator;

import java.util.NoSuchElementException;

/**
 * FennelIterator implements the {@link java.util.Iterator} and
 * {@link RestartableIterator} interfaces by reading tuples from a
 * Fennel ExecStream.  It does this by adapting a FennelTupleIter to
 * the {@link RestartableIterator} interface.
 *
 * <p>FennelIterator only deals with raw byte buffers; it delegates to a
 * {@link FennelTupleReader} object the responsibility to unmarshal individual
 * fields.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelIterator extends FennelTupleIter 
    implements RestartableIterator
{
    //~ Instance fields -------------------------------------------------------
    private Object next;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     * @param streamGraph underlying FennelStreamGraph
     * @param streamHandle handle to underlying Fennel TupleStream
     * @param bufferSize number of bytes in buffer used for fetching from
     *     Fennel
     */
    public FennelIterator(
        FennelTupleReader tupleReader,
        FennelStreamGraph streamGraph,
        FennelStreamHandle streamHandle,
        int bufferSize)
    {
        super(tupleReader, streamGraph, streamHandle, bufferSize);
        this.next = null;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Iterator
    // Note that we hold the buffer whenever this returns true.
    public boolean hasNext()
    {
        if (next != null) {
            return true;
        }

        Object fetched = fetchNext();
        if (fetched == NoDataReason.END_OF_DATA) {
            return false;
        }

        // Old-style iterator convention doesn't handle anything but
        // END_OF_DATA
        assert(!(fetched instanceof NoDataReason));

        next = fetched;

        return true;
    }

    // implement Iterator
    public Object next()
    {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }

        Object result = next;
        next = null;
        return result;
    }

    // implement Iterator
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}


// End FennelIterator.java

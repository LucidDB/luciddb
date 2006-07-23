/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
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

import java.util.*;
import java.util.logging.*;

import org.eigenbase.runtime.*;


/**
 * FennelPipeIterator implements the {@link RestartableIterator} interface,
 * receiving data from a producer as {@link ByteBuffer} objects, and
 * unmarshalling them to a consumer. It does this by extending {@link
 * FennelPipeTupleIter} and adapting TupleIter semantics to Iterator semantics.
 *
 * @author Julian Hyde
 * @version $Id$
 */
public class FennelPipeIterator
    extends FennelPipeTupleIter
    implements RestartableIterator
{

    //~ Instance fields --------------------------------------------------------

    private Object next = null; // current row

    //~ Constructors -----------------------------------------------------------

    /**
     * creates a new FennelPipeIterator object.
     *
     * @param tupleReader FennelTupleReader to use to interpret Fennel data
     */
    public FennelPipeIterator(FennelTupleReader tupleReader)
    {
        super(tupleReader);
    }

    //~ Methods ----------------------------------------------------------------

    // implement Iterator
    public boolean hasNext()
    {
        if (next != null) {
            traceHasNext(true);
            return true;
        }

        Object fetched = fetchNext();
        if (fetched == NoDataReason.END_OF_DATA) {
            traceHasNext(false);
            return false;
        }

        // Old-style iterator convention doesn't handle anything but
        // END_OF_DATA
        assert (!(fetched instanceof NoDataReason));

        next = fetched;

        traceHasNext(true);
        return true;
    }

    private void traceHasNext(boolean hasNextResult)
    {
        if (!tracer.isLoggable(Level.FINE)) {
            return;
        }

        if (!hasNextResult || tracer.isLoggable(Level.FINER)) {
            String msg = getStatus(this.toString()) + " => " + hasNextResult;

            if (!hasNextResult) {
                tracer.fine(msg);
            } else {
                tracer.finer(msg);
            }
        }
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

// End FennelPipeIterator.java

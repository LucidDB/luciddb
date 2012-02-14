/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.runtime;

import java.util.*;
import java.util.logging.*;

import org.eigenbase.runtime.*;


/**
 * FennelPipeIterator implements the {@link RestartableIterator} interface,
 * receiving data from a producer as {@link java.nio.ByteBuffer} objects, and
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

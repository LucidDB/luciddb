/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

import java.nio.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;

import org.eigenbase.runtime.*;

import net.sf.farrago.trace.FarragoTrace;

/**
 * FarragoTransformImpl provides a base class for generated implementations of
 * {@link FarragoTransform}.
 *
 * @author Julian Hyde, Stephan Zuercher
 * @version $Id$
 */
public abstract class FarragoTransformImpl
    implements FarragoTransform
{
    //~ Class fields --------------------------------------------------------
    protected static final Logger tracer =
        FarragoTrace.getFarragoTransformTracer();

    //~ Instance fields --------------------------------------------------------

    private TupleIter tupleIter;
    private FennelTupleWriter tupleWriter;
    private Object next;

    //~ Methods ----------------------------------------------------------------

    /**
     * Initialze this FarragoTransformImpl. Generated FarragoTransform
     * implementations should pass their generated FennelTupleWriter and
     * TupleIter implementations here. A subclass (not an anonymous subclass)
     * may pass <code>null</code> for <code>tupleWriter</code> or <code>
     * tupleIter</code> iff it has a different way to read or write its data and
     * iff it overrides {@link #execute} and {@link #restart} as appropriate.
     *
     * <code>tupleWriter</code> or <code>tupleIter</code> iff it has a different
     * way to read or write its data and iff it overrides {@link #execute} and
     * {@link #restart} as appropriate.
     *
     * @param tupleWriter FennelTupleWriter that can marshal this transform's
     * output tuple format.
     * @param tupleIter TupleIter that performs this transform's work
     */
    protected void init(FennelTupleWriter tupleWriter, TupleIter tupleIter)
    {
        this.tupleWriter = tupleWriter;
        this.tupleIter = tupleIter;
        this.next = null;
    }

    /**
     * for named subclasses, not for generated transforms
     */
    protected TupleIter getTupleIter()
    {
        return tupleIter;
    }

    /**
     * exposes {@link org.eigenbase.runtime.TupleIter#setTimeout}, to avoid blocking
     * JavaTransformExecStream
     */
    public void setInputFetchTimeout(long timeout)
    {
        tupleIter.setTimeout(timeout, true);
    }

    /**
     * Execute this transform. Execution continues until the underlying {@link
     * #tupleIter} returns END_OF_DATA or UNDERFLOW or until the underlying
     * {@link #tupleWriter} can no longer marshal tuples into the output buffer.
     *
     * @param outputBuffer output ByteBuffer, written to via {@link
     * #tupleWriter}
     * @param quantum the maximum number of tuples to process before returning
     *
     * @return number of bytes marshaled into outputBuffer; 0 on END_OF_DATA; -1
     * on UNDERFLOW
     */
    public int execute(ByteBuffer outputBuffer, long quantum)
    {
        long tupleCount = 0;

        // If next is not null, then a row was previously fetched but
        // there wasn't room to marshal it.
        if (next == null) {
            Object o = tupleIter.fetchNext();

            if (o == TupleIter.NoDataReason.END_OF_DATA) {
                tracer.finer("end of data");
                return 0;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                tracer.finer("underflow");
                return -1;
            }

            next = o;
        }

        outputBuffer.order(ByteOrder.nativeOrder());
        outputBuffer.clear();

        for (;;) {
            // Before attempting to marshal tuple, record current start
            // position in case a partial marshalling attempt moves it.
            int startPosition = outputBuffer.position();
            if (!tupleWriter.marshalTuple(outputBuffer, next)) {
                if (startPosition == 0) {
                    // We were not able to marshal the entire tuple,
                    // and so far we haven't even marshalled one tuple,
                    // so there's no way we're going to make progress.
                    throw FarragoResource.instance().JavaRowTooLong.ex(
                        outputBuffer.remaining(),
                        next.toString());
                } else {
                    // Not enough room to marshal the latest tuple, but we've
                    // already got some earlier ones marshalled.
                    break;
                }
            }

            // See note re: quantum as unsigned int.
            if (++tupleCount >= quantum) {
                next = null;
                break;
            }

            Object o = tupleIter.fetchNext();
            tracer.log(Level.FINEST, "wrote row {0}", o);
            if (o == TupleIter.NoDataReason.END_OF_DATA) {
                // Will return 0 on next call to this method -- we've already
                // marshaled at least one tuple that we need to return.
                next = null;
                break;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                // We marshaled at least one tuple, so don't return -1.
                next = null;
                break;
            }

            next = o;
        }

        tracer.log(Level.FINER, "wrote {0} rows", tupleCount);
        outputBuffer.flip();
        return outputBuffer.limit();
    }

    /**
     * Restart the underlying {@link #tupleIter}.
     */
    public void restart()
    {
        tupleIter.restart();
    }
}

// End FarragoTransformImpl.java

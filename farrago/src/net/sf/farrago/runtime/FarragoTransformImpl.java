/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import net.sf.farrago.fennel.*;
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
    private FarragoRuntimeContext connection;
    private FennelStreamHandle peerStream;
    private TupleIter tupleIter;
    private FennelTupleWriter tupleWriter;
    private Object next;

    //~ Methods ----------------------------------------------------------------

    /**
     * Initialze this FarragoTransformImpl.
     *
     * Generated FarragoTransform implementations should pass their generated
     * FennelTupleWriter and TupleIter implementations here.
     *
     * A subclass (not an anonymous subclass) may pass <code>null</code>
     * for <code>tupleWriter</code> or <code> tupleIter</code>
     * iff it has a different way to read or write its data and iff it overrides
     *  {@link #execute} and {@link #restart} as appropriate.
     *
     * @param connection  the runtime context
     * @param peerStreamName null if no fennel peer
     * @param tupleWriter FennelTupleWriter that marshals this transform's
     * output tuple format.
     * @param tupleIter TupleIter that performs this transform's work
     */
    protected void init(
        FarragoRuntimeContext connection,
        String peerStreamName,
        FennelTupleWriter tupleWriter,
        TupleIter tupleIter)
    {
        this.connection = connection;
        this.peerStream = connection.getStreamHandle(peerStreamName, false);
        this.tupleWriter = tupleWriter;
        this.tupleIter = tupleIter;
        this.next = null;
        if (tupleIter instanceof FarragoJavaUdxIterator) {
            ((FarragoJavaUdxIterator) tupleIter).setThreadName(peerStreamName);
        }
    }

    /**
     * @return the TupleIter
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

    public void pleaseSignalOnMoreData()
    {
        tracer.fine("pleaseSignalOnMoreData");
        boolean ok = tupleIter.addListener(
            new TupleIter.MoreDataListener() {
                // called when more data after underflow
                public void onMoreData()
                {
                    tracer.fine(
                        "more data after underflow; signal fennel peer");
                    FennelStreamGraph graph = connection.getStreamGraph();
                    graph.setStreamRunnable(peerStream, true);
                }
            });
        if (!ok) {
            tracer.severe(
                "FarragoTramsform failed to add input data listener: " + this);
        }
    }

    /**
     * Execute this transform, fetching rows from the source {@link #tupleIter}.
     * Each row is written to the output buffer by the {@link #tupleWriter}.
     * Produces rows until either the quantum is exceeded, the output buffer is
     * full, or the source returns END_OF_DATA or UNDERFLOW.
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
                traceOutput(Level.FINER, 0, outputBuffer);
                return 0;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                traceOutput(Level.FINER, -1, outputBuffer);
                return -1;
            }
            next = o;
        }

        outputBuffer.order(ByteOrder.nativeOrder());
        outputBuffer.clear();

        for (;;) {
            // write a row
            int startPosition = outputBuffer.position();
            if (!tupleWriter.marshalTuple(outputBuffer, next)) {
                if (startPosition == 0) {
                    // no room even for one row: hopeless
                    throw FarragoResource.instance()
                        .JavaRowTooLong.ex(
                            outputBuffer.remaining(), next.toString());
                } else {
                    tracer.finest("overflow");
                    break;
                }
            }
            tracer.log(Level.FINEST, "wrote row {0}", next);
            next = null;                // was written
            if (++tupleCount >= quantum) {
                tracer.finest("quantum");
                break;
            }

            // fetch a row
            Object o = tupleIter.fetchNext();
            if (o == TupleIter.NoDataReason.END_OF_DATA) {
                // Will return 0 on next call to this method -- we've already
                // marshaled at least one tuple that we need to return.
                tracer.finest("weak end of data");
                break;
            } else if (o == TupleIter.NoDataReason.UNDERFLOW) {
                tracer.finest("weak underflow");
                break;
            } else {
                next = o;
            }
        }

        traceOutput(Level.FINER, tupleCount, outputBuffer);
        outputBuffer.flip();
        return outputBuffer.limit();
    }

    protected void traceOutput(
        Level level, long nrows, ByteBuffer outputBuffer)
    {
        if (!tracer.isLoggable(level)) {
            return;
        }
        StringBuilder buf = new StringBuilder();
        if (nrows == 0) {
            buf.append("end of data");
        } else if (nrows < 0) {
            buf.append("underflow");
        } else {
            buf.append("wrote ").append(nrows).append(" rows");
        }
        // be cautious for the sake of subclasses
        if (tupleIter != null) {
            tupleIter.printStatus(buf.append(", input: "));
        }
        if (outputBuffer != null) {
            buf.append(", output: ").append(outputBuffer);
        }
        tracer.log(level, buf.toString());
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

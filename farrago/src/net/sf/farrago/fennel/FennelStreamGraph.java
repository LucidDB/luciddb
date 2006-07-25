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
package net.sf.farrago.fennel;

import java.sql.*;

import java.util.logging.*;

import net.sf.farrago.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;


/**
 * FennelStreamGraphs are FarragoAllocations for FemStreamGraphHandles.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelStreamGraph
    implements FarragoAllocation
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getFennelStreamGraphTracer();

    //~ Instance fields --------------------------------------------------------

    private final FennelDbHandle fennelDbHandle;
    private long streamGraphHandle;

    //~ Constructors -----------------------------------------------------------

    FennelStreamGraph(
        FennelDbHandle fennelDbHandle,
        FemStreamGraphHandle streamGraphHandle)
    {
        this.fennelDbHandle = fennelDbHandle;
        this.streamGraphHandle = streamGraphHandle.getLongHandle();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Finds a particular stream within the graph.
     *
     * @param metadataFactory factory for creating Fem instances
     * @param streamName name of stream to find
     * @param isInput
     *
     * @return handle to stream
     */
    public FennelStreamHandle findStream(FarragoMetadataFactory metadataFactory,
        String streamName,
        boolean isInput)
    {
        FemCmdCreateStreamHandle cmd =
            metadataFactory.newFemCmdCreateStreamHandle();
        cmd.setStreamGraphHandle(getStreamGraphHandle());
        cmd.setStreamName(streamName);
        cmd.setInput(isInput);
        fennelDbHandle.executeCmd(cmd);
        return new FennelStreamHandle(cmd.getResultHandle().getLongHandle());
    }

    /**
     * @return the underlying FemStreamGraphHandle
     */
    public FemStreamGraphHandle getStreamGraphHandle()
    {
        FemStreamGraphHandle newHandle =
            fennelDbHandle.getMetadataFactory().newFemStreamGraphHandle();
        newHandle.setLongHandle(streamGraphHandle);
        return newHandle;
    }

    /**
     * @return native handle to underlying graph
     */
    public long getLongHandle()
    {
        return streamGraphHandle;
    }

    private void traceGraphHandle(String operation)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                operation + " streamGraphHandle = "
                + Long.toHexString(streamGraphHandle));
        }
    }

    private void traceStreamHandle(
        String operation,
        FennelStreamHandle streamHandle)
    {
        if (tracer.isLoggable(Level.FINER)) {
            tracer.fine(
                operation + " streamHandle = "
                + Long.toHexString(streamHandle.getLongHandle()));
        }
    }

    private void traceStreamHandle(
        String operation,
        FennelStreamHandle streamHandle,
        int execStreamInputOrdinal)
    {
        if (tracer.isLoggable(Level.FINER)) {
            tracer.fine(
                operation
                + " streamHandle = "
                + Long.toHexString(streamHandle.getLongHandle())
                + " input ordinal = " + execStreamInputOrdinal);
        }
    }

    /**
     * Opens a prepared stream graph.
     *
     * @param fennelTxnContext transaction context in which stream graph should
     * participate
     * @param javaStreamMap optional FennelJavaStreamMap
     */
    public void open(
        FennelTxnContext fennelTxnContext,
        FennelJavaStreamMap javaStreamMap)
    {
        traceGraphHandle("open");
        try {
            FennelStorage.tupleStreamGraphOpen(
                streamGraphHandle,
                fennelTxnContext.getTxnHandleLong(),
                javaStreamMap);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Fetches a buffer of rows from a stream. If unpositioned, this fetches the
     * first rows.
     *
     * @param streamHandle handle to stream from which to fetch
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (at least one tuple should always be
     * fetched if any are available, so 0 indicates end of stream)
     */
    public int fetch(
        FennelStreamHandle streamHandle,
        byte [] byteArray)
    {
        traceStreamHandle("fetch", streamHandle);
        try {
            return
                FennelStorage.tupleStreamFetch(
                    streamHandle.getLongHandle(),
                    byteArray);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Fetches a buffer of rows from a stream. If unpositioned, this fetches the
     * first rows.
     *
     * @param streamHandle handle to JavaTransformExecStream from which to fetch
     * @param execStreamInputOrdinal ordinal of the stream's input
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (0 indicates end of stream, less than 0
     * indicates no data currently available)
     */
    public int transformFetch(
        FennelStreamHandle streamHandle,
        int execStreamInputOrdinal,
        byte [] byteArray)
    {
        traceStreamHandle(
            "transformFetch",
            streamHandle,
            execStreamInputOrdinal);
        try {
            return
                FennelStorage.tupleStreamTransformFetch(
                    streamHandle.getLongHandle(),
                    execStreamInputOrdinal,
                    byteArray);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    // NOTE:  close/abort/closeAllocation are synchronized since
    // abort can be asynchronous

    /**
     * Closes the stream graph (but does not deallocate it).
     */
    public synchronized void close()
    {
        traceGraphHandle("close");
        if (streamGraphHandle == 0) {
            return;
        }
        try {
            FennelStorage.tupleStreamGraphClose(
                streamGraphHandle,
                FennelStorage.CLOSE_RESULT);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Aborts the current execution (but does not close the graph).
     */
    public synchronized void abort()
    {
        traceGraphHandle("abort");
        if (streamGraphHandle == 0) {
            return;
        }
        try {
            FennelStorage.tupleStreamGraphClose(
                streamGraphHandle,
                FennelStorage.CLOSE_ABORT);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    // implement FarragoAllocation
    public synchronized void closeAllocation()
    {
        traceGraphHandle("deallocate");
        if (streamGraphHandle == 0) {
            return;
        }
        try {
            FennelStorage.tupleStreamGraphClose(
                streamGraphHandle,
                FennelStorage.CLOSE_DEALLOCATE);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        } finally {
            streamGraphHandle = 0;
        }
    }

    /**
     * Restarts a particular stream in this graph.
     *
     * @param streamHandle handle to stream to restart
     */
    public void restart(FennelStreamHandle streamHandle)
    {
        try {
            FennelStorage.tupleStreamRestart(
                streamHandle.getLongHandle());
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }
}

// End FennelStreamGraph.java

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
package net.sf.farrago.fennel;

import java.sql.*;

import java.util.*;
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
     *   true: find the adapter intepolated after the stream;
     *   false: find the stream itself.
     *
     * @return handle to stream
     */
    public FennelStreamHandle findStream(
        FarragoMetadataFactory metadataFactory,
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
     * Find the inputs of a specified stream in the graph.
     *
     * @param streamName names a stream
     *
     * @return a list of the names of the inputs, in input-edge order.
     */
    public String [] getInputStreams(String streamName)
    {
        // REVIEW mberkowitz 12-Oct-2008. For simplicity, not using
        // FennelStreamHandle here.
        traceGraphHandle("get inputs of a stream");
        ArrayList<String> inputList = new ArrayList<String>();
        FennelStorage.tupleStreamGraphGetInputStreams(
            streamGraphHandle,
            streamName,
            inputList);
        if (tracer.isLoggable(Level.FINER)) {
            StringBuilder msg = new StringBuilder("The inputs of string ");
            msg.append(streamName).append(" are: ( ");
            for (String input : inputList) {
                msg.append(input).append(", ");
            }
            tracer.finer(msg.append(" ).").toString());
        }
        return inputList.toArray(new String[inputList.size()]);
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
     * @param javaErrorTarget error target handles row errors
     */
    public void open(
        FennelTxnContext fennelTxnContext,
        FennelJavaStreamMap javaStreamMap,
        FennelJavaErrorTarget javaErrorTarget)
    {
        traceGraphHandle("open");
        try {
            FennelStorage.tupleStreamGraphOpen(
                streamGraphHandle,
                fennelTxnContext.getTxnHandleLong(),
                javaStreamMap,
                javaErrorTarget);
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
            return FennelStorage.tupleStreamFetch(
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
            return FennelStorage.tupleStreamTransformFetch(
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
        traceStreamHandle("restart", streamHandle);
        try {
            FennelStorage.tupleStreamRestart(
                streamHandle.getLongHandle());
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Sets the runnable state of a particular stream in this graph.
     *
     * @param streamHandle handle to the stream.
     * @param state the new state
     */
    public void setStreamRunnable(
        FennelStreamHandle streamHandle,
        boolean state)
    {
        traceStreamHandle(
            (state ? "set runnable" : "set not runnable"), streamHandle);
        try {
            FennelStorage.tupleStreamSetRunnable(
                streamHandle.getLongHandle(), state);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

}

// End FennelStreamGraph.java

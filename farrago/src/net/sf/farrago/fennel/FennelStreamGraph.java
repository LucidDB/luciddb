/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.fennel;

import net.sf.farrago.*;
import net.sf.farrago.util.*;
import net.sf.farrago.fem.fennel.*;

import java.sql.*;
import java.util.logging.*;

/**
 * FennelStreamGraphs are FarragoAllocations for FemStreamGraphHandles.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelStreamGraph implements FarragoAllocation
{
    private static Logger tracer =
        TraceUtil.getClassTrace(FennelStreamGraph.class);
    
    private final FennelDbHandle fennelDbHandle;
    private FemStreamGraphHandle streamGraphHandle;

    FennelStreamGraph(
        FennelDbHandle fennelDbHandle,
        FemStreamGraphHandle streamGraphHandle)
    {
        this.fennelDbHandle = fennelDbHandle;
        this.streamGraphHandle = streamGraphHandle;
    }

    /**
     * Find a particular stream within the graph.
     *
     * @param metadataFactory factory for creating Fem instances
     *
     * @param streamName name of stream to find
     *
     * @return handle to stream
     */
    public FemStreamHandle findStream(
        FarragoMetadataFactory metadataFactory,
        String streamName)
    {
        FemCmdCreateStreamHandle cmd =
            metadataFactory.newFemCmdCreateStreamHandle();
        cmd.setStreamGraphHandle(streamGraphHandle);
        cmd.setStreamName(streamName);
        fennelDbHandle.executeCmd(cmd);
        return cmd.getResultHandle();
    }

    /**
     * @return the underlying FemStreamGraphHandle
     */
    public FemStreamGraphHandle getStreamGraphHandle()
    {
        return streamGraphHandle;
    }

    private void traceGraphHandle(String operation)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                operation + " streamGraphHandle = "
                + streamGraphHandle.getLongHandle());
        }
    }

    private void traceStreamHandle(
        String operation,FemStreamHandle streamHandle)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                operation + " streamHandle = "
                + streamHandle.getLongHandle());
        }
    }

    /**
     * Open a prepared stream graph.
     *
     * @param hTxn transaction in which stream graph participates
     *
     * @param javaStreamMap optional FennelJavaStreamMap
     */
    public void open(FemTxnHandle hTxn,FennelJavaStreamMap javaStreamMap)
    {
        traceGraphHandle("open");
        try {
            FennelStorage.tupleStreamGraphOpen(
                streamGraphHandle,hTxn,javaStreamMap);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Fetch buffer of rows from a tuple stream.  If unpositioned, this
     * fetches the first rows.
     *
     * @param streamHandle handle to stream from which to fetch
     *
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (at least one tuple should always be
     *         fetched if any are available, so 0 indicates end of stream)
     */
    public int fetch(FemStreamHandle streamHandle,byte [] byteArray)
    {
        traceStreamHandle("fetch",streamHandle);
        try {
            return FennelStorage.tupleStreamFetch(streamHandle,byteArray);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Close the tuple stream graph (but do not deallocate it).
     */
    public void close()
    {
        traceGraphHandle("close");
        try {
            FennelStorage.tupleStreamGraphClose(streamGraphHandle,false);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (streamGraphHandle == null) {
            return;
        }
        traceGraphHandle("deallocate");
        try {
            FennelStorage.tupleStreamGraphClose(streamGraphHandle,true);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        } finally {
            streamGraphHandle = null;
        }
    }
}

// End FennelStreamGraph.java

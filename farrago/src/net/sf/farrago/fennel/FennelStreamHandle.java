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

import net.sf.farrago.util.*;
import net.sf.farrago.fem.fennel.*;

import java.sql.*;
import java.util.logging.*;

/**
 * FennelStreamHandles are FarragoAllocations for FemStreamHandles.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelStreamHandle implements FarragoAllocation
{
    private static Logger tracer =
        TraceUtil.getClassTrace(FennelStreamHandle.class);
    
    private final FennelDbHandle fennelDbHandle;
    private FemStreamHandle streamHandle;

    FennelStreamHandle(
        FennelDbHandle fennelDbHandle,
        FemStreamHandle streamHandle)
    {
        this.fennelDbHandle = fennelDbHandle;
        this.streamHandle = streamHandle;
    }

    private void traceHandle(String operation)
    {
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine(
                operation + " handle = " + streamHandle.getLongHandle());
        }
    }

    /**
     * Open a prepared TupleStream.
     *
     * @param hTxn transaction in which stream participates
     *
     * @param javaStreamMap optional FennelJavaStreamMap
     */
    public void open(FemTxnHandle hTxn,FennelJavaStreamMap javaStreamMap)
    {
        traceHandle("open");
        try {
            FennelStorage.tupleStreamOpen(streamHandle,hTxn,javaStreamMap);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Fetch buffer of rows from a tuple stream.  If unpositioned, this
     * fetches the first rows.
     *
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (at least one tuple should always be
     *         fetched if any are available, so 0 indicates end of stream)
     */
    public int fetch(byte [] byteArray)
    {
        traceHandle("fetch");
        try {
            return FennelStorage.tupleStreamFetch(streamHandle,byteArray);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    /**
     * Close the tuple stream (but do not deallocate it).
     */
    public void close()
    {
        traceHandle("close");
        try {
            FennelStorage.tupleStreamClose(streamHandle,false);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        }
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (streamHandle == null) {
            return;
        }
        traceHandle("deallocate");
        try {
            FennelStorage.tupleStreamClose(streamHandle,true);
        } catch (SQLException ex) {
            throw fennelDbHandle.handleNativeException(ex);
        } finally {
            streamHandle = null;
        }
    }
}

// End FennelStreamHandle.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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

import java.util.*;

import net.sf.farrago.fem.fennel.*;

import org.eigenbase.util.*;


/**
 * FennelStorage is the JNI interface for calling Fennel from Farrago. Most
 * methods have package access only; other classes in this package expose public
 * wrapper methods.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelStorage
{
    //~ Static fields/initializers ---------------------------------------------

    static {
        Util.loadLibrary("farrago");
    }

    static final int CLOSE_RESULT = 0;

    static final int CLOSE_ABORT = 1;

    static final int CLOSE_DEALLOCATE = 2;

    //~ Methods ----------------------------------------------------------------

    /**
     * Creates a native handle for a Java object for reference by XML commands.
     * After this, the Java object cannot be garbage collected until its owner
     * explicitly calls closeAllocation.
     *
     * @param obj object for which to create a handle, or null to create a
     * placeholder handle
     *
     * @return native handle
     */
    static native long newObjectHandle(Object obj);

    /**
     * Releases a handle obtained via newObjectHandle. This should only be
     * called from FennelJavaHandle.
     *
     * @param handle the handle to delete
     */
    static native void deleteObjectHandle(long handle);

    /**
     * Changes the object referenced by a handle.
     *
     * @param handle the handle to change
     * @param obj new object
     */
    static native void setObjectHandle(
        long handle,
        Object obj);

    /**
     * @return count of handles returned by Fennel which have not yet been
     * deleted
     */
    public static native int getHandleCount();

    /**
     * Constructs a FemTupleAccessor for a FemTupleDescriptor.
     *
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return XMI string representation of FemTupleAccessor
     */
    static native String getAccessorXmiForTupleDescriptor(
        FemTupleDescriptor tupleDesc);

    /**
     * Executes a command represented as a Java object.
     *
     * @param cmd Java representation of object
     * @param execHandle optional execution handle associated with the command
     * that's used to pass execution state from Farrago to Fennel; set to 0 if
     * there is no handle
     *
     * @return output object handle if any
     */
    static native long executeJavaCmd(FemCmd cmd, long execHandle)
        throws SQLException;

    /**
     * Find the input of a given stream node in a stream graph.
     *
     * @param hStreamGraph handle to stream graph
     * @param node stream name
     * @param inputs The names of the input streams are added to this list, in
     * graph edge order.
     */
    static native void tupleStreamGraphGetInputStreams(
        long hStreamGraph,
        String node,
        List<String> inputs);

    /**
     * Opens a stream graph.
     *
     * @param hStreamGraph handle to stream graph
     * @param hTxn handle to txn in which stream is being opened
     * @param javaStreamMap optional FennelJavaStreamMap
     * @param javaErrorTarget error target handles row errors
     */
    static native void tupleStreamGraphOpen(
        long hStreamGraph,
        long hTxn,
        FennelJavaStreamMap javaStreamMap,
        FennelJavaErrorTarget javaErrorTarget)
        throws SQLException;

    /**
     * Fetches a buffer of rows from a stream. If unpositioned, this fetches the
     * first rows.
     *
     * @param hStream handle to stream
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (at least one tuple should always be
     * fetched, so 0 indicates end of stream)
     */
    static native int tupleStreamFetch(
        long hStream,
        byte [] byteArray)
        throws SQLException;

    /**
     * Fetches a buffer of rows from a stream. Specifically, the stream must be
     * a JavaTransformExecStream. If unpositioned, this fetches the first rows.
     * Does not block if no data is available.
     *
     * @param hStream handle to stream
     * @param execStreamInputOrdinal ordinal of the input to fetch from
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (0 indicates end of stream, less than 0
     * indicates no data currently availble)
     */
    static native int tupleStreamTransformFetch(
        long hStream,
        int execStreamInputOrdinal,
        byte [] byteArray)
        throws SQLException;

    /**
     * Restarts a stream.
     *
     * @param hStream handle to stream to restart
     */
    static native void tupleStreamRestart(
        long hStream)
        throws SQLException;

    /**
     * Closes a stream graph.
     *
     * @param hStreamGraph handle to stream graph
     * @param action CLOSE_XXX
     */
    static native void tupleStreamGraphClose(
        long hStreamGraph,
        int action)
        throws SQLException;

    /**
     * Allocates a new object in Fennel that Farrago will use to communicate
     * execution state information from Farrago to Fennel. Access to that object
     * will be through a handle.
     *
     * @return the handle that will be used to access the Fennel object
     */
    static native long newExecutionHandle();

    /**
     * Deletes the Fennel object corresponding to an execution handle.
     *
     * @param execHandle the execution handle
     */
    static native void deleteExecutionHandle(long execHandle);

    /**
     * Cancels execution of a statement associated with an execution handle.
     *
     * @param execHandle the execution handle
     */
    public static native void cancelExecution(long execHandle);
}

// End FennelStorage.java

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

import java.sql.*;

import net.sf.farrago.fem.fennel.*;

import org.eigenbase.util.*;

/**
 * FennelStorage is the JNI interface for calling Fennel from Farrago.  Most
 * methods have package access only; other classes in this package expose
 * public wrapper methods.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelStorage
{
    //~ Static fields/initializers --------------------------------------------

    static {
        Util.loadLibrary("farrago");
    }

    static final int CLOSE_RESULT = 0;

    static final int CLOSE_ABORT = 1;

    static final int CLOSE_DEALLOCATE = 2;

    //~ Methods ---------------------------------------------------------------

    /**
     * Creates a native handle for a Java object for reference by XML commands.
     * After this, the Java object cannot be garbage collected until
     * its owner explicitly calls closeAllocation.
     *
     * @param obj object for which to create a handle, or null to create a
     * placeholder handle
     *
     * @return native handle
     */
    static native long newObjectHandle(Object obj);

    /**
     * Releases a handle obtained via newObjectHandle.  This should only be
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
     * @return count of handles returned by Fennel which have not
     * yet been deleted
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
     *
     * @return output object handle if any
     */
    static native long executeJavaCmd(FemCmd cmd)
        throws SQLException;

    /**
     * Opens a stream graph.
     *
     * @param hStream handle to stream
     * @param hTxn handle to txn in which stream is being opened
     * @param javaStreamMap optional FennelJavaStreamMap
     */
    static native void tupleStreamGraphOpen(
        long hStreamGraph,
        long hTxn,
        FennelJavaStreamMap javaStreamMap)
        throws SQLException;

    /**
     * Fetches a buffer of rows from a stream.  If unpositioned, this
     * fetches the first rows.
     *
     * @param hStream handle to stream
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (at least one tuple should always be
     *         fetched, so 0 indicates end of stream)
     */
    static native int tupleStreamFetch(
        long hStream,
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
     * @param hStream handle to stream graph
     * @param action CLOSE_XXX
     */
    static native void tupleStreamGraphClose(
        long hStreamGraph,
        int action)
        throws SQLException;
}


// End FennelStorage.java

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

import net.sf.farrago.fem.fennel.*;

import java.sql.*;

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
        // TODO:  get Fennel build to produce the right library name
        if (!System.mapLibraryName("farrago").startsWith("lib")) {
            // assume mingw
            System.loadLibrary("cygfarrago-0");
        } else {
            System.loadLibrary("farrago");
        }
    }

    //~ Constructors ----------------------------------------------------------

    //~ Methods ---------------------------------------------------------------

    /**
     * Create a native handle for a Java object for reference by XML commands.
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
     * Release a handle obtained via newObjectHandle.  This should only be
     * called from FennelJavaHandle.
     *
     * @param handle the handle to delete
     */
    static native void deleteObjectHandle(long handle);

    /**
     * Change the object referenced by a handle.
     *
     * @param handle the handle to change
     * @param obj new object
     */
    static native void setObjectHandle(long handle,Object obj);

    /**
     * .
     *
     *
     * @return count of handles returned by Fennel which have not
     * yet been deleted
     */
    public static native int getHandleCount();

    /**
     * Construct a FemTupleAccessor for a FemTupleDescriptor.
     *
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return XMI string representation of FemTupleAccessor
     */
    static native String getAccessorXmiForTupleDescriptor(
        FemTupleDescriptor tupleDesc);

    /**
     * Execute a command represented as a Java object.
     *
     * @param cmd Java representation of object
     *
     * @return output object handle if any
     */
    static native long executeJavaCmd(FemCmd cmd) throws SQLException;

    /**
     * Open a tuple stream graph.
     *
     * @param hStream handle to tuple stream
     * @param hTxn handle to txn in which stream is being reopened
     * @param javaStreamMap optional FennelJavaStreamMap
     */
    static native void tupleStreamGraphOpen(
        FemStreamGraphHandle hStreamGraph,
        FemTxnHandle hTxn,
        FennelJavaStreamMap javaStreamMap) throws SQLException;
    
    /**
     * Fetch buffer of rows from a tuple stream.  If unpositioned, this
     * fetches the first rows.
     *
     * @param hStream handle to tuple stream
     * @param byteArray output buffer receives complete tuples
     *
     * @return number of bytes fetched (at least one tuple should always be
     *         fetched, so 0 indicates end of stream)
     */
    static native int tupleStreamFetch(
        FemStreamHandle hStream,
        byte [] byteArray) throws SQLException;
    
    /**
     * Close a tuple stream graph.
     *
     * @param hStream handle to tuple stream
     * @param deallocate if true, close and deallocate; if false, just close
     */
    static native void tupleStreamGraphClose(
        FemStreamGraphHandle hStreamGraph,
        boolean deallocate) throws SQLException;
}


// End FennelStorage.java

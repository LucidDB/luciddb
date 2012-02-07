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

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.*;


/**
 * FennelDbHandle is a public wrapper for FennelStorage, and represents a handle
 * to a loaded Fennel database.
 *
 * @author John V. Sichi, turned into an interface by Hunter Payne
 * @version $Id$
 */
public interface FennelDbHandle
    extends FarragoAllocation
{
    //~ Methods ----------------------------------------------------------------

    public FarragoMetadataFactory getMetadataFactory();

    /**
     * @param callerFactory override for metadataFactory
     *
     * @return FemDbHandle for this database
     */
    public FemDbHandle getFemDbHandle(FarragoMetadataFactory callerFactory);

    /**
     * Constructs a FemTupleAccessor for a FemTupleDescriptor. This shouldn't be
     * called directly except from FennelRelUtil.
     *
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return XMI string representation of FemTupleAccessor
     */
    public String getAccessorXmiForTupleDescriptorTraced(
        FemTupleDescriptor tupleDesc);

    /**
     * Executes a FemCmd object. If the command produces a resultHandle, it will
     * be set after successful execution.
     *
     * @param cmd instance of FemCmd with all parameters set
     *
     * @return return handle as primitive
     */
    public long executeCmd(FemCmd cmd);

    /**
     * Executes a FemCmd object, associating an optional execution handle with
     * the command. If the command produces a resultHandle, it will be set after
     * successful execution.
     *
     * @param cmd instance of FemCmd with all parameters set
     * @param execHandle the execution handle associated with the command; null
     * if there is no associated execution handle
     *
     * @return return handle as primitive
     */
    public long executeCmd(FemCmd cmd, FennelExecutionHandle execHandle);

    /**
     * Creates a native handle for a Java object for reference by XML commands.
     * After this, the Java object cannot be garbage collected until its owner
     * explicitly calls closeAllocation.
     *
     * @param owner the object which will be made responsible for the handle's
     * allocation as a result of this call
     * @param obj object for which to create a handle, or null to create a
     * placeholder handle
     *
     * @return native handle
     */
    public FennelJavaHandle allocateNewObjectHandle(
        FarragoAllocationOwner owner,
        Object obj);

    /**
     * Changes the object referenced by a handle.
     *
     * @param handle the handle to change
     * @param obj new object
     */
    public void setObjectHandle(
        long handle,
        Object obj);

    // implement FarragoAllocation
    public void closeAllocation();

    public EigenbaseException handleNativeException(SQLException ex);
}

// End FennelDbHandle.java

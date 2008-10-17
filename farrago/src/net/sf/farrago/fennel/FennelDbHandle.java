/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

import net.sf.farrago.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.JmiObjUtil;
import org.eigenbase.util.*;


/**
 * FennelDbHandle is a public wrapper for FennelStorage, and represents a handle
 * to a loaded Fennel database.
 *
 * @author John V. Sichi, turned into an interface by Hunter Payne
 * @version $Id$
 */
public interface FennelDbHandle extends FarragoAllocation
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
     * Executes a FemCmd object. If the command produces a resultHandle,
     * it will be set after successful execution.
     *
     * @param cmd instance of FemCmd with all parameters set
     *
     * @return return handle as primitive
     */
    public long executeCmd(FemCmd cmd);

    /**
     * Executes a FemCmd object, associating an optional execution handle
     * with the command. If the command produces a resultHandle,
     * it will be set after successful execution.
     *
     * @param cmd instance of FemCmd with all parameters set
     * @param execHandle the execution handle associated with the command;
     * null if there is no associated execution handle
     *
     * @return return handle as primitive
     */
    public long executeCmd(FemCmd cmd, FennelExecutionHandle execHandle);

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

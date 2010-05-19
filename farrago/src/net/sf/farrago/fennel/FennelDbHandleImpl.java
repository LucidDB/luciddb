/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import org.eigenbase.jmi.*;
import org.eigenbase.util.*;


/**
 * FennelDbHandle is a public wrapper for FennelStorage, and represents a handle
 * to a loaded Fennel database.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelDbHandleImpl
    implements FennelDbHandle
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getFennelDbHandleTracer();
    private static final Logger jhTracer =
        FarragoTrace.getFennelJavaHandleTracer();

    //~ Instance fields --------------------------------------------------------

    private final FarragoMetadataFactory metadataFactory;
    private final FennelCmdExecutor cmdExecutor;
    private Map<RefPackage, Collection<RefBaseObject>> handleAssociationsMap;
    private long dbHandle;

    //~ Constructors -----------------------------------------------------------

    /**
     * Opens a Fennel database.
     *
     * @param metadataFactory FarragoMetadataFactory for creating Fem instances
     * @param owner the object which will be made responsible for this
     * database's allocation
     * @param cmdExecutor FennelCmdExecutor to use for executing all commands on
     * this database
     * @param cmd instance of FemCmdOpenDatabase with all parameters set
     */
    public FennelDbHandleImpl(
        FarragoMetadataFactory metadataFactory,
        FarragoAllocationOwner owner,
        FennelCmdExecutor cmdExecutor,
        FemCmdOpenDatabase cmd)
    {
        this.metadataFactory = metadataFactory;
        this.cmdExecutor = cmdExecutor;

        handleAssociationsMap =
            new HashMap<RefPackage, Collection<RefBaseObject>>();

        executeCmd(cmd);
        dbHandle = cmd.getResultHandle().getLongHandle();
        owner.addAllocation(this);
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoMetadataFactory getMetadataFactory()
    {
        return metadataFactory;
    }

    private synchronized Collection getHandleAssociations(
        RefPackage fennelPackage)
    {
        Collection<RefBaseObject> handleAssociations =
            handleAssociationsMap.get(fennelPackage);
        if (handleAssociations != null) {
            return handleAssociations;
        }

        handleAssociations = new ArrayList<RefBaseObject>();

        // Use JMI to find all associations between Cmds and Handles.  This
        // information is needed when executing commands.
        for (Object o : fennelPackage.refAllAssociations()) {
            RefAssociation refAssoc = (RefAssociation) o;
            Association assoc = (Association) refAssoc.refMetaObject();
            for (Object o1 : assoc.getContents()) {
                AssociationEnd assocEnd = (AssociationEnd) o1;
                if (assocEnd.getName().endsWith("Handle")) {
                    handleAssociations.add(refAssoc);
                    handleAssociations.add(assocEnd.otherEnd());
                    break;
                }
            }
        }

        handleAssociationsMap.put(fennelPackage, handleAssociations);
        return handleAssociations;
    }

    /**
     * @param callerFactory override for metadataFactory
     *
     * @return FemDbHandle for this database
     */
    public FemDbHandle getFemDbHandle(FarragoMetadataFactory callerFactory)
    {
        // NOTE:  this stupidity is necessary since user and system catalogs
        // have different metamodels instances.
        FemDbHandle newHandle = callerFactory.newFemDbHandle();
        newHandle.setLongHandle(dbHandle);
        return newHandle;
    }

    /**
     * Constructs a FemTupleAccessor for a FemTupleDescriptor. This shouldn't be
     * called directly except from FennelRelUtil.
     *
     * @param tupleDesc source FemTupleDescriptor
     *
     * @return XMI string representation of FemTupleAccessor
     */
    public String getAccessorXmiForTupleDescriptorTraced(
        FemTupleDescriptor tupleDesc)
    {
        if (tracer.isLoggable(Level.FINE)) {
            String xmiInput =
                JmiObjUtil.exportToXmiString(Collections.singleton(tupleDesc));
            tracer.fine(xmiInput);
        }
        if (tracer.isLoggable(Level.FINEST)) {
            tracer.finest("getting accessor XMI for tuple " + tupleDesc);
        }
        String xmiOutput =
            FennelStorage.getAccessorXmiForTupleDescriptor(tupleDesc);
        tracer.fine(xmiOutput);
        return xmiOutput;
    }

    /**
     * Executes a FemCmd object. If the command produces a resultHandle, it will
     * be set after successful execution.
     *
     * @param cmd instance of FemCmd with all parameters set
     *
     * @return return handle as primitive
     */
    public long executeCmd(FemCmd cmd)
    {
        return executeCmd(cmd, null);
    }

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
    public long executeCmd(FemCmd cmd, FennelExecutionHandle execHandle)
    {
        List<RefObject> exportList = null;
        FemHandle resultHandle = null;
        String resultHandleClassName = null;

        if (tracer.isLoggable(Level.FINE)) {
            // Build up list of objects to be exported for tracing purposes.
            // Most of them get pulled in automatically via composition from
            // cmd.  Note that exportList also serves as a flag for whether
            // tracing is being performed.
            exportList = new ArrayList<RefObject>();
            exportList.add(cmd);
        }

        // TODO:  hash on cmd class instead of iterating below Handles require
        // special treatment.  For tracing, input handles have to be added to
        // exportList explicitly since they aren't reachable via composition
        // associations.  So walk the handleAssociations list and determine
        // which handles this command uses.
        RefPackage fennelPackage = cmd.refImmediatePackage();
        Collection handleAssociations = getHandleAssociations(fennelPackage);
        Iterator assocIter = handleAssociations.iterator();
        while (assocIter.hasNext()) {
            RefAssociation refAssoc = (RefAssociation) assocIter.next();
            AssociationEnd assocEnd = (AssociationEnd) assocIter.next();
            if (!cmd.refIsInstanceOf(
                    assocEnd.getType(),
                    true))
            {
                continue;
            }
            if (assocEnd.otherEnd().getName().equals("ResultHandle")) {
                // Act like a factory, producing a result handle of the
                // appropriate subclass.  But don't add it to the export list
                // since it won't be valid until after the command completes.
                assert (resultHandle == null);
                Classifier resultHandleType = assocEnd.otherEnd().getType();
                RefClass resultHandleClass =
                    fennelPackage.refClass(resultHandleType);
                resultHandle =
                    (FemHandle) resultHandleClass.refCreateInstance(
                        Collections.EMPTY_LIST);

                // Remember the new handle in the command so the caller can
                // access it later.
                refAssoc.refAddLink(cmd, resultHandle);
                resultHandleClassName = resultHandleType.getName();
            } else {
                // Trace input handles.
                if (exportList != null) {
                    for (Object o : refAssoc.refQuery(assocEnd, cmd)) {
                        FemHandle handle = (FemHandle) o;
                        exportList.add(handle);
                    }
                }
            }
        }
        if (exportList != null) {
            String xmiString = JmiObjUtil.exportToXmiString(exportList);
            tracer.fine(xmiString);
        }
        long resultHandleLong;
        try {
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("executing command " + cmd);
            }
            resultHandleLong = cmdExecutor.executeJavaCmd(cmd, execHandle);
            if (tracer.isLoggable(Level.FINEST)) {
                tracer.finest("finished executing command " + cmd);
            }
        } catch (SQLException ex) {
            throw handleNativeException(ex);
        }
        if (resultHandle != null) {
            resultHandle.setLongHandle(resultHandleLong);
            if (exportList != null) {
                tracer.fine(
                    "Returning " + resultHandleClassName + " = '"
                    + resultHandleLong
                    + "(" + Long.toHexString(resultHandleLong) + ")'");
            }
        }

        // TODO:  fix CmdCreateIndex so that this becomes unnecessary
        return resultHandleLong;
    }

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
        Object obj)
    {
        long hJavaObj = FennelStorage.newObjectHandle(obj);
        FennelJavaHandle h = new FennelJavaHandle(hJavaObj);
        owner.addAllocation(h);
        if (jhTracer.isLoggable(Level.FINE)) {
            jhTracer.fine(
                "java handle " + h + ", object " + obj + ", owner " + owner);
        }
        return h;
    }

    /**
     * Changes the object referenced by a handle.
     *
     * @param handle the handle to change
     * @param obj new object
     */
    public void setObjectHandle(
        long handle,
        Object obj)
    {
        if (jhTracer.isLoggable(Level.FINE)) {
            jhTracer.fine("java handle " + handle + ", set object " + obj);
        }
        FennelStorage.setObjectHandle(handle, obj);
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (dbHandle == 0) {
            return;
        }

        FemCmdCloseDatabase cmd = metadataFactory.newFemCmdCloseDatabase();
        cmd.setDbHandle(getFemDbHandle(metadataFactory));
        dbHandle = 0;
        executeCmd(cmd);
    }

    public EigenbaseException handleNativeException(SQLException ex)
    {
        return FarragoResource.instance().FennelUntranslated.ex(
            ex.getMessage());
    }
}

// End FennelDbHandleImpl.java

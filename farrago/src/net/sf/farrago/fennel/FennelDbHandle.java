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

import mondrian.resource.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.*;

import net.sf.saffron.util.*;

import org.w3c.dom.*;

import java.io.*;

import java.lang.reflect.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import javax.jmi.model.*;
import javax.jmi.reflect.*;

/**
 * FennelDbHandle is a public wrapper for FennelStorage, and represents
 * a handle to a loaded Fennel database.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelDbHandle implements FarragoAllocation
{
    private static Logger tracer =
        TraceUtil.getClassTrace(FennelDbHandle.class);

    //~ Instance fields -------------------------------------------------------

    private final FarragoMetadataFactory metadataFactory;

    private final FennelCmdExecutor cmdExecutor;
    
    private Map handleAssociationsMap;

    private FemDbHandle dbHandle;

    /**
     * Open a Fennel database.
     *
     * @param metadataFactory FarragoMetadataFactory for creating Fem instances
     *
     * @param owner the object which will be made responsible for this
     * database's allocation
     *
     * @param cmdExecutor FennelCmdExecutor to use for executing all
     * commands on this database
     *
     * @param cmd instance of FemCmdOpenDatabase with all parameters set
     */
    public FennelDbHandle(
        FarragoMetadataFactory metadataFactory,
        FarragoAllocationOwner owner,
        FennelCmdExecutor cmdExecutor,
        FemCmdOpenDatabase cmd)
    {
        this.metadataFactory = metadataFactory;
        this.cmdExecutor = cmdExecutor;

        handleAssociationsMap = new HashMap();

        executeCmd(cmd);
        dbHandle = cmd.getResultHandle();
        owner.addAllocation(this);
    }

    private synchronized Collection getHandleAssociations(
        RefPackage fennelPackage)
    {
        Collection handleAssociations = (Collection)
            handleAssociationsMap.get(fennelPackage);
        if (handleAssociations != null) {
            return handleAssociations;
        }
        
        handleAssociations = new ArrayList();

        // Use JMI to find all associations between Cmds and Handles.  This
        // information is needed when executing commands.
        Iterator assocIter = fennelPackage.refAllAssociations().iterator();
        while (assocIter.hasNext()) {
            RefAssociation refAssoc = (RefAssociation) assocIter.next();
            Association assoc = (Association) refAssoc.refMetaObject();
            Iterator endIter = assoc.getContents().iterator();
            while (endIter.hasNext()) {
                AssociationEnd assocEnd = (AssociationEnd) endIter.next();
                if (assocEnd.getName().endsWith("Handle")) {
                    handleAssociations.add(refAssoc);
                    handleAssociations.add(assocEnd.otherEnd());
                    break;
                }
            }
        }

        handleAssociationsMap.put(fennelPackage,handleAssociations);
        return handleAssociations;
    }

    /**
     * @param callerFactory override for metadataFactory
     *
     * @return FemDbHandle for this database
     */
    public FemDbHandle getFemDbHandle(
        FarragoMetadataFactory callerFactory)
    {
        // NOTE:  this stupidity is necessary since user and system catalogs
        // have different metamodels instances.
        FemDbHandle newHandle = callerFactory.newFemDbHandle();
        newHandle.setLongHandle(dbHandle.getLongHandle());
        return newHandle;
    }

    /**
     * Construct a FemTupleAccessor for a FemTupleDescriptor.  This shouldn't
     * be called directly except from FennelRelUtil.
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
                JmiUtil.exportToXmiString(Collections.singleton(tupleDesc));
            tracer.fine(xmiInput);
        }
        String xmiOutput = FennelStorage.getAccessorXmiForTupleDescriptor(
            tupleDesc);
        tracer.fine(xmiOutput);
        return xmiOutput;
    }

    /**
     * Execute a FemCmd object.  If the command produces a resultHandle, it
     * will be set after successful execution.
     *
     * @param cmd instance of FemCmd with all parameters set
     *
     * @return return handle as primitive
     */
    public long executeCmd(FemCmd cmd)
    {
        List exportList = null;
        FemHandle resultHandle = null;
        String resultHandleClassName = null;

        if (tracer.isLoggable(Level.FINE)) {
            // Build up list of objects to be exported for tracing purposes.
            // Most of them get pulled in automatically via composition from
            // cmd.  Note that exportList also serves as a flag for whether
            // tracing is being performed.
            exportList = new ArrayList();
            exportList.add(cmd);
        }

        // TODO:  hash on cmd class instead of iterating below
        // Handles require special treatment.  For tracing, input handles have
        // to be added to exportList explicitly since they aren't reachable via
        // composition associations.  So walk the handleAssociations list and
        // determine which handles this command uses.
        RefPackage fennelPackage = cmd.refImmediatePackage();
        Collection handleAssociations = getHandleAssociations(fennelPackage);
        Iterator assocIter = handleAssociations.iterator();
        while (assocIter.hasNext()) {
            RefAssociation refAssoc = (RefAssociation) assocIter.next();
            AssociationEnd assocEnd = (AssociationEnd) assocIter.next();
            if (!cmd.refIsInstanceOf(assocEnd.getType(),true)) {
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
                refAssoc.refAddLink(cmd,resultHandle);
                resultHandleClassName = resultHandleType.getName();
            } else {
                // Trace input handles.
                if (exportList != null) {
                    Iterator handleIter =
                        refAssoc.refQuery(assocEnd,cmd).iterator();
                    while (handleIter.hasNext()) {
                        FemHandle handle = (FemHandle) handleIter.next();
                        exportList.add(handle);
                    }
                }
            }
        }
        if (exportList != null) {
            String xmiString = JmiUtil.exportToXmiString(exportList);
            tracer.fine(xmiString);
        }
        long resultHandleLong;
        try {
            resultHandleLong = cmdExecutor.executeJavaCmd(cmd);
        } catch (SQLException ex) {
            throw handleNativeException(ex);
        }
        if (resultHandle != null) {
            resultHandle.setLongHandle(resultHandleLong);
            if (exportList != null) {
                tracer.fine(
                    "Returning " + resultHandleClassName + " = '"
                    + resultHandleLong + "'");
            }
        }

        // TODO:  fix CmdCreateIndex so that this becomes unnecessary
        return resultHandleLong;
    }

    /**
     * Create a native handle for a Java object for reference by XML commands.
     * After this, the Java object cannot be garbage collected until
     * its owner explicitly calls closeAllocation.
     *
     * @param owner the object which will be made responsible for the handle's
     * allocation as a result of this call
     *
     * @param obj object for which to create a handle, or null to create a
     * placeholder handle
     *
     * @return native handle
     */
    public static FennelJavaHandle allocateNewObjectHandle(
        FarragoAllocationOwner owner,Object obj)
    {
        long hJavaObj = FennelStorage.newObjectHandle(obj);
        FennelJavaHandle h = new FennelJavaHandle(hJavaObj);
        owner.addAllocation(h);
        return h;
    }

    /**
     * Change the object referenced by a handle.
     *
     * @param handle the handle to change
     * @param obj new object
     */
    public void setObjectHandle(long handle,Object obj)
    {
        FennelStorage.setObjectHandle(handle,obj);
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (dbHandle == null) {
            return;
        }
        FemCmdCloseDatabase cmd = metadataFactory.newFemCmdCloseDatabase();
        cmd.setDbHandle(dbHandle);
        dbHandle = null;
        executeCmd(cmd);
    }
    
    public FarragoException handleNativeException(SQLException ex)
    {
        return
            FarragoResource.instance().newFennelUntranslated(ex.getMessage());
    }
}

// End FennelDbHandle.java

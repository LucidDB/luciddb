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


/**
 * FennelTxnContext manages the state of at most one Fennel transaction.  A
 * context may be inactive, meaning it has no current transaction.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelTxnContext
{
    //~ Instance fields -------------------------------------------------------

    private final FarragoMetadataFactory metadataFactory;
    
    private final FennelDbHandle fennelDbHandle;

    private long hTxn;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelTxnContext object.
     *
     * @param metadataFactory FarragoMetadataFactory for creating Fem instances
     *
     * @param fennelDbHandle handle to database affected by txns in this context
     */
    public FennelTxnContext(
        FarragoMetadataFactory metadataFactory,
        FennelDbHandle fennelDbHandle)
    {
        this.metadataFactory = metadataFactory;
        this.fennelDbHandle = fennelDbHandle;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * .
     *
     * @return handle to Fennel database accessed by this txn context
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }
    
    /**
     * Get the handle to the current txn.  If no txn is in progress, start one
     * and return the new handle.
     *
     * @return txn handle as long
     */
    long getTxnHandleLong()
    {
        if (hTxn != 0) {
            return hTxn;
        }

        fennelDbHandle.getTransientTxnContext().beginTransientTxn();
        try {
            FemCmdBeginTxn cmd = metadataFactory.newFemCmdBeginTxn();
            cmd.setDbHandle(fennelDbHandle.getFemDbHandle(metadataFactory));
            fennelDbHandle.executeCmd(cmd);
            hTxn = cmd.getResultHandle().getLongHandle();
            return hTxn;
        } finally {
            fennelDbHandle.getTransientTxnContext().endTransientTxn();
        }
    }
    
    private FemTxnHandle getTxnHandle()
    {
        getTxnHandleLong();
        FemTxnHandle newHandle =
            metadataFactory.newFemTxnHandle();
        newHandle.setLongHandle(hTxn);
        return newHandle;
    }

    /**
     * .
     *
     * @return whether a txn is in progress on this connection
     */
    public boolean isTxnInProgress()
    {
        return (hTxn != 0);
    }

    /**
     * Commit the current txn, if any.
     */
    public void commit()
    {
        if (!isTxnInProgress()) {
            return;
        }
        
        fennelDbHandle.getTransientTxnContext().beginTransientTxn();
        try {
            FemCmdCommit cmd = metadataFactory.newFemCmdCommit();
            cmd.setTxnHandle(getTxnHandle());
            fennelDbHandle.executeCmd(cmd);
        } finally {
            fennelDbHandle.getTransientTxnContext().endTransientTxn();
        }

        // TODO:  determine whether txn is still in progress if excn is thrown
        hTxn = 0;
    }

    /**
     * Roll back the current txn, if any.
     */
    public void rollback()
    {
        if (!isTxnInProgress()) {
            return;
        }
        
        fennelDbHandle.getTransientTxnContext().beginTransientTxn();
        try {
            FemCmdRollback cmd = metadataFactory.newFemCmdRollback();
            cmd.setTxnHandle(getTxnHandle());
            fennelDbHandle.executeCmd(cmd);
        } finally {
            fennelDbHandle.getTransientTxnContext().endTransientTxn();
        }

        // TODO:  determine whether txn is still in progress if excn is thrown
        hTxn = 0;
    }

    /**
     * Create a new savepoint, starting a new transaction if necessary.  Note
     * that savepoint handles are "lightweight" and thus don't require
     * deallocation.
     *
     * @return handle to created savepoint
     */
    public FennelSvptHandle newSavepoint()
    {
        fennelDbHandle.getTransientTxnContext().beginTransientTxn();
        try {
            FemCmdSavepoint cmd = metadataFactory.newFemCmdSavepoint();
            cmd.setTxnHandle(getTxnHandle());
            fennelDbHandle.executeCmd(cmd);
            return new FennelSvptHandle(cmd.getResultHandle().getLongHandle());
        } finally {
            fennelDbHandle.getTransientTxnContext().endTransientTxn();
        }
    }

    /**
     * Rollback to an existing savepoint.
     *
     * @param fennelSvptHandle handle to savepoint to rollback to
     */
    public void rollbackToSavepoint(FennelSvptHandle fennelSvptHandle)
    {
        assert(isTxnInProgress());
        
        fennelDbHandle.getTransientTxnContext().beginTransientTxn();
        try {
            FemCmdRollback cmd = metadataFactory.newFemCmdRollback();
            cmd.setTxnHandle(getTxnHandle());
            cmd.setSvptHandle(getFemSvptHandle(fennelSvptHandle));
            fennelDbHandle.executeCmd(cmd);
        } finally {
            fennelDbHandle.getTransientTxnContext().endTransientTxn();
        }
    }

    private FemSvptHandle getFemSvptHandle(FennelSvptHandle fennelSvptHandle)
    {
        FemSvptHandle newHandle =
            metadataFactory.newFemSvptHandle();
        newHandle.setLongHandle(fennelSvptHandle.getLongHandle());
        return newHandle;
    }

    /**
     * Wrapper for executeCmd in the case where cmd is a
     * FemCmdCreateExecutionStreamGraph.  This ensures that some object owns
     * the returned stream graph.
     *
     * @param owner the object which will be made responsible for the stream
     * graph's allocation as a result of this call
     *
     * @return opened FennelStreamGraph
     */
    public FennelStreamGraph newStreamGraph(
        FarragoAllocationOwner owner)
    {
        fennelDbHandle.getTransientTxnContext().beginTransientTxn();
        try {
            FemCmdCreateExecutionStreamGraph cmdCreate =
                metadataFactory.newFemCmdCreateExecutionStreamGraph();
            cmdCreate.setTxnHandle(getTxnHandle());
            fennelDbHandle.executeCmd(cmdCreate);
            FennelStreamGraph streamGraph = new FennelStreamGraph(
                fennelDbHandle,cmdCreate.getResultHandle());
            owner.addAllocation(streamGraph);
            return streamGraph;
        } finally {
            fennelDbHandle.getTransientTxnContext().endTransientTxn();
        }
    }
}


// End FennelTxnContext.java

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

    private FemTxnHandle hTxn;

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
     * @return txn handle
     */
    public FemTxnHandle getTxnHandle()
    {
        if (hTxn != null) {
            return hTxn;
        }

        FemCmdBeginTxn cmd = metadataFactory.newFemCmdBeginTxn();
        cmd.setDbHandle(fennelDbHandle.getFemDbHandle(metadataFactory));
        fennelDbHandle.executeCmd(cmd);
        hTxn = cmd.getResultHandle();
        return hTxn;
    }

    /**
     * .
     *
     * @return whether a txn is in progress on this connection
     */
    public boolean isTxnInProgress()
    {
        return (hTxn != null);
    }

    /**
     * Commit the current txn, if any.
     */
    public void commit()
    {
        if (!isTxnInProgress()) {
            return;
        }
        FemCmdCommit cmd = metadataFactory.newFemCmdCommit();
        cmd.setTxnHandle(hTxn);
        fennelDbHandle.executeCmd(cmd);

        // TODO:  determine whether txn is still in progress if excn is thrown
        hTxn = null;
    }

    /**
     * Roll back the current txn, if any.
     */
    public void rollback()
    {
        if (!isTxnInProgress()) {
            return;
        }
        FemCmdRollback cmd = metadataFactory.newFemCmdRollback();
        cmd.setTxnHandle(hTxn);
        fennelDbHandle.executeCmd(cmd);

        // TODO:  determine whether txn is still in progress if excn is thrown
        hTxn = null;
    }

    /**
     * Create a new savepoint, starting a new transaction if necessary.  Note
     * that savepoint handles are "lightweight" and thus don't require
     * deallocation.
     *
     * @return handle to created savepoint
     */
    public FemSvptHandle newSavepoint()
    {
        FemCmdSavepoint cmd = metadataFactory.newFemCmdSavepoint();
        cmd.setTxnHandle(getTxnHandle());
        fennelDbHandle.executeCmd(cmd);
        return cmd.getResultHandle();
    }

    /**
     * Rollback to an existing savepoint.
     *
     * @param femSvptHandle handle to savepoint to rollback to
     */
    public void rollbackToSavepoint(FemSvptHandle femSvptHandle)
    {
        assert(isTxnInProgress());
        
        FemCmdRollback cmd = metadataFactory.newFemCmdRollback();
        cmd.setTxnHandle(hTxn);
        cmd.setSvptHandle(femSvptHandle);
        fennelDbHandle.executeCmd(cmd);
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
        FemCmdCreateExecutionStreamGraph cmdCreate =
            metadataFactory.newFemCmdCreateExecutionStreamGraph();
        cmdCreate.setTxnHandle(getTxnHandle());
        fennelDbHandle.executeCmd(cmdCreate);
        FennelStreamGraph streamGraph =
            new FennelStreamGraph(fennelDbHandle,cmdCreate.getResultHandle());
        owner.addAllocation(streamGraph);
        return streamGraph;
    }
}


// End FennelTxnContext.java

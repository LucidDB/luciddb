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

import net.sf.farrago.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.util.*;


/**
 * FennelTxnContext manages the state of at most one Fennel transaction. A
 * context may be inactive, meaning it has no current transaction.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelTxnContext
{
    //~ Instance fields --------------------------------------------------------

    private final FarragoMetadataFactory metadataFactory;
    private final FennelDbHandle fennelDbHandle;
    private long hTxn;
    private boolean readOnly;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelTxnContext object.
     *
     * @param metadataFactory FarragoMetadataFactory for creating Fem instances
     * @param fennelDbHandle handle to database affected by txns in this context
     */
    public FennelTxnContext(
        FarragoMetadataFactory metadataFactory,
        FennelDbHandle fennelDbHandle)
    {
        this.metadataFactory = metadataFactory;
        this.fennelDbHandle = fennelDbHandle;
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return handle to Fennel database accessed by this txn context
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    // REVIEW jvs 20-Mar-2006: This txn-initiation API needs to be cleaned up
    // (get rid of implicit start and unification with Farrago-level txn mgmt),
    // along with enforcement of read-only transactions.

    /**
     * Forces a transaction to begin unless one is already in progress.
     */
    public void initiateTxn()
    {
        assert (!readOnly);
        getTxnHandleLong();
    }

    /**
     * Starts a read-only transaction; no transaction is allowed to be in
     * progress already.
     */
    public void initiateReadOnlyTxn()
    {
        assert (hTxn == 0);
        readOnly = true;
        getTxnHandleLong();
    }

    /**
     * Starts a transaction with a commit sequence number so the transaction
     * will read data based on a specific snapshot time.
     *
     * @param csn the commit sequence number
     */
    public void initiateTxnWithCsn(long csn)
    {
        assert (!isTxnInProgress());
        FemCmdBeginTxnWithCsn cmd = metadataFactory.newFemCmdBeginTxnWithCsn();
        cmd.setDbHandle(fennelDbHandle.getFemDbHandle(metadataFactory));
        FemCsnHandle csnHandle = metadataFactory.newFemCsnHandle();
        csnHandle.setLongHandle(csn);
        cmd.setCsnHandle(csnHandle);
        fennelDbHandle.executeCmd(cmd);
        hTxn = cmd.getResultHandle().getLongHandle();
    }

    /**
     * Gets the handle to the current txn. If no txn is in progress, starts one
     * and returns the new handle.
     *
     * @return txn handle as long
     */
    long getTxnHandleLong()
    {
        if (hTxn != 0) {
            return hTxn;
        }

        if (fennelDbHandle == null) {
            return -1;
        }

        FemCmdBeginTxn cmd = metadataFactory.newFemCmdBeginTxn();
        cmd.setReadOnly(readOnly);
        cmd.setDbHandle(fennelDbHandle.getFemDbHandle(metadataFactory));
        fennelDbHandle.executeCmd(cmd);
        hTxn = cmd.getResultHandle().getLongHandle();
        return hTxn;
    }

    public FemTxnHandle getTxnHandle()
    {
        getTxnHandleLong();
        FemTxnHandle newHandle = metadataFactory.newFemTxnHandle();
        newHandle.setLongHandle(hTxn);
        return newHandle;
    }

    /**
     * You really don't want to use this.  Needed for a specific Farrago
     * extension.
     */
    public long getRawTxnHandle()
    {
        return hTxn;
    }

    /**
     * @return whether a txn is in progress on this connection
     */
    public boolean isTxnInProgress()
    {
        return (hTxn != 0);
    }

    /**
     * Retrieves the commit sequence number associated with the current
     * transaction
     *
     * @return the commit sequence number of the current transaction
     */
    public long getTxnCsn()
    {
        assert (isTxnInProgress());
        FemCmdGetTxnCsn cmd = metadataFactory.newFemCmdGetTxnCsn();
        cmd.setTxnHandle(getTxnHandle());
        fennelDbHandle.executeCmd(cmd);
        return cmd.getResultHandle().getLongHandle();
    }

    /**
     * Commits the current txn, if any.
     */
    public void commit()
    {
        if (isTxnInProgress()) {
            FemCmdCommit cmd = metadataFactory.newFemCmdCommit();
            cmd.setTxnHandle(getTxnHandle());
            fennelDbHandle.executeCmd(cmd);
        }

        // TODO:  determine whether txn is still in progress if excn is thrown
        onEndOfTxn();
    }

    /**
     * Rolls back the current txn, if any.
     */
    public void rollback()
    {
        if (isTxnInProgress()) {
            FemCmdRollback cmd = metadataFactory.newFemCmdRollback();
            cmd.setTxnHandle(getTxnHandle());
            fennelDbHandle.executeCmd(cmd);
        }

        // TODO:  determine whether txn is still in progress if excn is thrown
        onEndOfTxn();
    }

    private void onEndOfTxn()
    {
        hTxn = 0;
        readOnly = false;
    }

    /**
     * Creates a new savepoint, starting a new transaction if necessary. Note
     * that savepoint handles are "lightweight" and thus don't require
     * deallocation.
     *
     * @return handle to created savepoint
     */
    public FennelSvptHandle newSavepoint()
    {
        if (fennelDbHandle == null) {
            return null;
        }

        FemCmdSavepoint cmd = metadataFactory.newFemCmdSavepoint();
        cmd.setTxnHandle(getTxnHandle());
        fennelDbHandle.executeCmd(cmd);
        return new FennelSvptHandle(cmd.getResultHandle().getLongHandle());
    }

    /**
     * Rolls back to an existing savepoint.
     *
     * @param fennelSvptHandle handle to savepoint to rollback to
     */
    public void rollbackToSavepoint(FennelSvptHandle fennelSvptHandle)
    {
        assert (isTxnInProgress());

        FemCmdRollback cmd = metadataFactory.newFemCmdRollback();
        cmd.setTxnHandle(getTxnHandle());
        cmd.setSvptHandle(getFemSvptHandle(fennelSvptHandle));
        fennelDbHandle.executeCmd(cmd);
    }

    private FemSvptHandle getFemSvptHandle(FennelSvptHandle fennelSvptHandle)
    {
        FemSvptHandle newHandle = metadataFactory.newFemSvptHandle();
        newHandle.setLongHandle(fennelSvptHandle.getLongHandle());
        return newHandle;
    }

    /**
     * Wrapper for executeCmd in the case where cmd is a
     * FemCmdCreateExecutionStreamGraph. This ensures that some object owns the
     * returned stream graph.
     *
     * @param owner the object which will be made responsible for the stream
     * graph's allocation as a result of this call
     *
     * @return opened FennelStreamGraph
     */
    public FennelStreamGraph newStreamGraph(FarragoAllocationOwner owner)
    {
        assert (fennelDbHandle != null);

        FemCmdCreateExecutionStreamGraph cmdCreate =
            metadataFactory.newFemCmdCreateExecutionStreamGraph();
        cmdCreate.setTxnHandle(getTxnHandle());
        fennelDbHandle.executeCmd(cmdCreate);
        FennelStreamGraph streamGraph =
            new FennelStreamGraph(
                fennelDbHandle,
                cmdCreate.getResultHandle());
        owner.addAllocation(streamGraph);
        return streamGraph;
    }
}

// End FennelTxnContext.java

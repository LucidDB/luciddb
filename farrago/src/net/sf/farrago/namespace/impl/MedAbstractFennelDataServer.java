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
package net.sf.farrago.namespace.impl;

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;

import org.eigenbase.jmi.*;


/**
 * MedAbstractFennelDataServer refines {@link MedAbstractLocalDataServer} with
 * abstract support for using Fennel's btree indexing.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public abstract class MedAbstractFennelDataServer
    extends MedAbstractLocalDataServer
{
    //~ Instance fields --------------------------------------------------------

    protected FarragoRepos repos;

    //~ Constructors -----------------------------------------------------------

    protected MedAbstractFennelDataServer(
        String serverMofId,
        Properties props,
        FarragoRepos repos)
    {
        super(serverMofId, props);
        this.repos = repos;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoMedDataServer
    public Object getRuntimeSupport(Object param)
        throws SQLException
    {
        return null;
    }

    // implement FarragoMedLocalDataServer
    public long createIndex(FemLocalIndex index, FennelTxnContext txnContext)
    {
        FemCmdCreateIndex cmd = repos.newFemCmdCreateIndex();
        boolean implicitTxn = false;
        if (!txnContext.isTxnInProgress()) {
            // If a xact isn't already in progress, the index creation will
            // implicitly start one, so we need to commit that implicit
            // txn after we've created the index.
            implicitTxn = true;
        }
        try {
            initIndexCmd(cmd, index, txnContext);
            long rc = getFennelDbHandle().executeCmd(cmd);
            if (implicitTxn) {
                txnContext.commit();
            }
            return rc;
        } finally {
            if (implicitTxn) {
                txnContext.rollback();
            }
        }
    }

    // implement FarragoMedLocalDataServer
    public void dropIndex(
        FemLocalIndex index,
        long rootPageId,
        boolean truncate,
        FennelTxnContext txnContext)
    {
        FemCmdDropIndex cmd;
        if (truncate) {
            cmd = repos.newFemCmdTruncateIndex();
        } else {
            cmd = repos.newFemCmdDropIndex();
        }
        boolean implicitTxn = false;
        if (!txnContext.isTxnInProgress()) {
            // If a xact isn't already in progress, the index drop will
            // implicitly start one, so we need to commit that implicit
            // txn after we've dropped the index.
            implicitTxn = true;
        }
        try {
            initIndexCmd(cmd, index, txnContext);
            cmd.setRootPageId(rootPageId);
            getFennelDbHandle().executeCmd(cmd);
            if (implicitTxn) {
                txnContext.commit();
            }
        } finally {
            if (implicitTxn) {
                txnContext.rollback();
            }
        }
    }

    // implement FarragoMedLocalDataServer
    public FarragoMedLocalIndexStats computeIndexStats(
        FemLocalIndex index,
        long rootPageId,
        boolean estimate,
        FennelTxnContext txnContext)
    {
        FemCmdVerifyIndex cmd = repos.newFemCmdVerifyIndex();
        boolean implicitTxn = false;
        if (!txnContext.isTxnInProgress()) {
            // If a xact isn't already in progress, the index verification will
            // implicitly start one, so we need to commit that implicit
            // txn after we've verified the index.
            implicitTxn = true;
        }
        try {
            initIndexCmd(cmd, index, txnContext);
            cmd.setRootPageId(rootPageId);
            cmd.setEstimate(estimate);
            cmd.setIncludeTuples(getIncludeTuples(index));
            getFennelDbHandle().executeCmd(cmd);
            long pageCount = cmd.getResultPageCount();
            long uniqueKeyCount =
                (cmd.getResultUniqueKeyCount() == null) ? -1
                : cmd.getResultUniqueKeyCount().longValue();
            if (implicitTxn) {
                txnContext.commit();
            }
            return new FarragoMedLocalIndexStats(pageCount, uniqueKeyCount);
        } finally {
            if (implicitTxn) {
                txnContext.rollback();
            }
        }
    }

    private void initIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index,
        FennelTxnContext txnContext)
    {
        cmd.setTxnHandle(txnContext.getTxnHandle());
        cmd.setSegmentId(getIndexSegmentId(index));
        cmd.setIndexId(JmiObjUtil.getObjectId(index));
        prepareIndexCmd(cmd, index);
    }

    /**
     * Prepares an index command based on the catalog definition of the index.
     * The parameterization details of command preparation are
     * subclass-dependent.
     *
     * @param cmd command to be initialized
     * @param index catalog definition of index
     */
    protected abstract void prepareIndexCmd(
        FemIndexCmd cmd,
        FemLocalIndex index);

    /**
     * Whether or not to include tuples in page count statistics
     */
    protected abstract boolean getIncludeTuples(
        FemLocalIndex index);

    /**
     * Gets the SegmentId of the segment storing an index.
     *
     * @param index the index of interest
     *
     * @return containing SegmentId
     */
    public static long getIndexSegmentId(FemLocalIndex index)
    {
        // TODO:  share symbolic enum with Fennel rather than hard-coding
        // values here
        if (FarragoCatalogUtil.isIndexTemporary(index)) {
            return 2;
        } else {
            return 1;
        }
    }

    //  implement FarragoMedLocalDataServer
    public void versionIndexRoot(
        Long oldRoot,
        Long newRoot,
        FennelTxnContext txnContext)
    {
        FemCmdVersionIndexRoot cmd = repos.newFemCmdVersionIndexRoot();
        boolean implicitTxn = false;
        if (!txnContext.isTxnInProgress()) {
            implicitTxn = true;
        }
        try {
            cmd.setOldRootPageId(oldRoot);
            cmd.setNewRootPageId(newRoot);
            cmd.setTxnHandle(txnContext.getTxnHandle());
            getFennelDbHandle().executeCmd(cmd);
            if (implicitTxn) {
                txnContext.commit();
            }
        } finally {
            if (implicitTxn) {
                txnContext.rollback();
            }
        }
    }
}

// End MedAbstractFennelDataServer.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
package net.sf.farrago.db;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * FarragoDbStmtContext implements the {@link
 * net.sf.farrago.session.FarragoSessionStmtContext} interface in terms of a
 * {@link FarragoDbSession}.
 *
 * <p>Most non-trivial public methods on this class must be synchronized on the
 * parent session; see superclass for details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbStmtContext
    extends FarragoDbStmtContextBase
    implements FarragoSessionStmtContext
{
    //~ Instance fields --------------------------------------------------------

    private long updateCount;
    private ResultSet resultSet;
    private FarragoSessionExecutableStmt executableStmt;
    private FarragoCompoundAllocation allocations;
    private FarragoSessionRuntimeContext runningContext;
    private final FarragoWarningQueue warningQueue;
    private boolean isExecDirect;

    /**
     * query timeout in seconds, default to 0.
     */
    private int queryTimeoutMillis = 0;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoDbStmtContext object.
     *
     * @param session the session creating this statement
     */
    public FarragoDbStmtContext(
        FarragoDbSession session,
        FarragoSessionStmtParamDefFactory paramDefFactory,
        FarragoDdlLockManager ddlLockManager)
    {
        this(session, paramDefFactory, ddlLockManager, null);
    }

    /**
     * Creates a new FarragoDbStmtContext object.
     *
     * @param session the session creating this statement
     */
    public FarragoDbStmtContext(
        FarragoDbSession session,
        FarragoSessionStmtParamDefFactory paramDefFactory,
        FarragoDdlLockManager ddlLockManager,
        FarragoSessionStmtContext rootStmtContext)
    {
        super(session, paramDefFactory, ddlLockManager, rootStmtContext);

        updateCount = -1;
        warningQueue = new FarragoWarningQueue();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtContext
    public boolean isPrepared()
    {
        return (executableStmt != null);
    }

    // implement FarragoSessionStmtContext
    public boolean isPreparedDml()
    {
        return executableStmt.isDml();
    }

    // implement FarragoSessionStmtContext
    public void prepare(
        String sql,
        boolean isExecDirect)
    {
        synchronized (session) {
            warningQueue.clearWarnings();
            unprepare();
            allocations = new FarragoCompoundAllocation();
            this.sql = sql;
            this.isExecDirect = isExecDirect;
            executableStmt =
                session.prepare(
                    this,
                    sql,
                    allocations,
                    isExecDirect,
                    null);
            finishPrepare();
        }
    }

    private void finishPrepare()
    {
        if (isPrepared()) {
            final RelDataType dynamicParamRowType =
                executableStmt.getDynamicParamRowType();
            initDynamicParams(dynamicParamRowType);
        } else {
            // always zero for DDL
            updateCount = 0;
        }
    }

    // implement FarragoSessionStmtContext
    public void prepare(
        RelNode plan,
        SqlKind kind,
        boolean logical,
        FarragoSessionPreparingStmt prep)
    {
        synchronized (session) {
            warningQueue.clearWarnings();
            unprepare();
            allocations = new FarragoCompoundAllocation();
            this.sql = ""; // not available

            executableStmt =
                session.getDatabase().implementStmt(
                    prep,
                    plan,
                    kind,
                    logical,
                    allocations);
            if (isPrepared()) {
                lockObjectsInUse(executableStmt);
            }
            finishPrepare();
        }
    }

    // implement FarragoSessionStmtContext
    public RelDataType getPreparedRowType()
    {
        synchronized (session) {
            assert (isPrepared());
            return executableStmt.getRowType();
        }
    }

    // implement FarragoSessionStmtContext
    public RelDataType getPreparedParamType()
    {
        synchronized (session) {
            assert (isPrepared());
            return executableStmt.getDynamicParamRowType();
        }
    }

    // implement FarragoSessionStmtContext
    public void setQueryTimeout(int millis)
    {
        queryTimeoutMillis = millis;
    }

    // implement FarragoSessionStmtContext
    public int getQueryTimeout()
    {
        return queryTimeoutMillis;
    }

    // implement FarragoSessionStmtContext
    public void execute()
    {
        synchronized (session) {
            executeImpl();
        }
    }

    private void executeImpl()
    {
        assert (isPrepared());
        if (!isExecDirect) {
            warningQueue.clearWarnings();
        }
        closeResultSet();
        traceExecute();
        boolean isDml = executableStmt.isDml();
        boolean success = false;

        if (session.isAutoCommit()) {
            // REVIEW jvs 26-Nov-2006:  What about CALL?  Maybe
            // we can start it as read-only (regardless of
            // whether the statements inside are DML, since they
            // will do their own autocommits).
            startAutocommitTxn(!isDml);
            if ((rootStmtContext != null)
                && rootStmtContext.needToSaveFirstTxnCsn())
            {
                rootStmtContext.saveFirstTxnCsn(
                    session.getFennelTxnContext().getTxnCsn());
            }
        }

        session.getRepos().beginReposSession();

        FarragoSessionRuntimeContext newContext = null;
        try {
            checkDynamicParamsSet();
            FarragoSessionRuntimeParams params =
                session.newRuntimeContextParams();
            if (executableStmt.getTableModOp() == null) {
                // only use txnCodeCache for real DML, not CALL
                params.txnCodeCache = null;
            }

            // FIXME: using Statement-level queue, but should use
            // ResultSet-level queue for isDml=false
            params.warningQueue = warningQueue;

            params.isDml = isDml;
            params.resultSetTypeMap = executableStmt.getResultSetTypeMap();
            params.iterCalcTypeMap = executableStmt.getIterCalcTypeMap();
            params.dynamicParamValues = dynamicParamValues;

            // REVIEW zfong 3/21/08 - Should this time be set to a non-zero
            // value even if this isn't an internal statement?  Currently,
            // it is, and therefore, it means that the current time is always
            // set based on the time when the statement was created, rather
            // than executed.
            params.currentTime = getStmtCurrentTime();
            assert (runningContext == null);

            initExecutingStmtInfo(executableStmt);
            params.stmtId = getExecutingStmtInfo().getId();

            newContext = session.getPersonality().newRuntimeContext(params);
            if (allocations != null) {
                newContext.addAllocation(allocations);
                allocations = null;
            }
            if (daemon) {
                newContext.addAllocation(this);
            }

            // Acquire locks (or whatever transaction manager wants) on all
            // tables accessed by this statement.
            accessTables(executableStmt);

            // If cancel request already came in, propagate it to
            // new context, which will then see it as part of execution.
            if (cancelFlag.isCancelRequested()) {
                newContext.cancel();
            }
            resultSet = executableStmt.execute(newContext);
            runningContext = newContext;
            newContext = null;

            if (queryTimeoutMillis > 0) {
                AbstractIterResultSet iteratorRS =
                    (AbstractIterResultSet) resultSet;
                iteratorRS.setTimeout(queryTimeoutMillis);
            }
            success = true;
        } finally {
            if (newContext != null) {
                newContext.closeAllocation();
                newContext = null;
            }
            if (!success) {
                session.endTransactionIfAuto(false);
                if (resultSet == null) {
                    session.getRepos().endReposSession();
                }
            }
        }
        if (isDml) {
            success = false;
            List<Long> rowCounts = new ArrayList<Long>();
            try {
                session.getPersonality().getRowCounts(
                    resultSet,
                    rowCounts,
                    executableStmt.getTableModOp());
                updateCount = updateRowCounts(rowCounts, runningContext);
                success = true;
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine("Update count = " + updateCount);
                }
            } catch (SQLException ex) {
                throw FarragoResource.instance().DmlFailure.ex(ex);
            } finally {
                try {
                    resultSet.close();
                } catch (SQLException ex) {
                    throw Util.newInternal(ex);
                } finally {
                    resultSet = null;
                    runningContext = null;
                    if (!success) {
                        session.endTransactionIfAuto(false);
                    }
                    clearExecutingStmtInfo();
                }
            }
        }

        // NOTE:  for result sets, autocommit is taken care of by
        // FarragoTupleIterResultSet and FennelTxnContext
        if (resultSet == null) {
            session.endTransactionIfAuto(true);
            if (!isDml) {
                session.getRepos().endReposSession();
            }
        }

        if (session.shutdownRequested()) {
            session.closeAllocation();
            FarragoDatabase db = ((FarragoDbSession) session).getDatabase();
            db.shutdown();
            session.setShutdownRequest(false);
        }
    }

    // implement FarragoSessionStmtContext
    public ResultSet getResultSet()
    {
        return resultSet;
    }

    // implement FarragoSessionStmtContext
    public long getUpdateCount()
    {
        synchronized (session) {
            long count = updateCount;
            updateCount = -1;
            return count;
        }
    }

    // implement FarragoSessionStmtContext
    public void cancel()
    {
        // request cancel, but don't wait for it to take effect
        cancel(false);
    }

    private void cancel(boolean wait)
    {
        tracer.info("cancel");

        // First, see if there are any child contexts that need to be
        // canceled
        for (FarragoSessionStmtContext childStmtContext : childrenStmtContexts) {
            childStmtContext.cancel();
        }

        // Record the cancellation request even if we haven't started
        // a runtime context yet.  We'll check this once the
        // runtime context gets created (FRG-349).
        cancelFlag.requestCancel();

        FarragoSessionRuntimeContext contextToCancel = runningContext;
        if (contextToCancel == null) {
            return;
        }
        contextToCancel.cancel();
        if (wait) {
            // if the cursor was executing when the cancel request was
            // received, this will wait for it to finish and return;
            // after that, it's safe to proceed with cleanup, since
            // any further fetch requests will see the cancel flag
            // already set and fail immediately
            contextToCancel.waitForCursor();
        }
    }

    // implement FarragoSessionStmtContext
    public void kill()
    {
        cancel(true);
        closeAllocation();
    }

    // implement FarragoSessionStmtContext
    public void closeResultSet()
    {
        synchronized (session) {
            if (resultSet == null) {
                return;
            }
            try {
                resultSet.close();
            } catch (Throwable ex) {
                throw Util.newInternal(ex);
            }
            resultSet = null;
            FarragoSessionRuntimeContext contextToClose = runningContext;
            if (contextToClose != null) {
                contextToClose.closeAllocation();
            }
            runningContext = null;
            clearExecutingStmtInfo();
        }
    }

    // implement FarragoSessionStmtContext
    public void unprepare()
    {
        synchronized (session) {
            // request cancel, and wait until it takes effect before
            // proceeding with cleanup, otherwise we may yank stuff
            // out from under executing threads in a bad way
            cancel(true);

            closeResultSet();
            if (allocations != null) {
                allocations.closeAllocation();
                allocations = null;
            }

            // reset the csn now that we've unprepared the root stmt context
            if (rootStmtContext == null) {
                snapshotCsn = null;
            }
            executableStmt = null;
            isExecDirect = false;

            super.unprepare();
        }
    }

    // implement FarragoSessionStmtContext
    public FarragoWarningQueue getWarningQueue()
    {
        return warningQueue;
    }

    /**
     * Update catalog row counts
     *
     * @param rowCounts row counts returned by the DML operation
     *
     * @return rowcount affected by the DML operation
     */
    private long updateRowCounts(
        List<Long> rowCounts,
        FarragoSessionRuntimeContext runningContext)
    {
        TableModificationRel.Operation tableModOp =
            executableStmt.getTableModOp();

        // marked as DML, but doesn't actually modify a table; e.g., a
        // procedure call
        if (tableModOp == null) {
            return 0;
        }
        List<String> targetTable = getDmlTarget();

        // if there's no target table (e.g., for a create index), then this
        // isn't really a DML statement
        if (targetTable == null) {
            return 0;
        }

        return session.getPersonality().updateRowCounts(
            session,
            targetTable,
            rowCounts,
            executableStmt.getTableModOp(),
            runningContext);
    }

    private List<String> getDmlTarget()
    {
        TableAccessMap tableAccessMap = executableStmt.getTableAccessMap();
        Set<List<String>> tablesAccessed = tableAccessMap.getTablesAccessed();
        for (List<String> table : tablesAccessed) {
            if (tableAccessMap.isTableAccessedForWrite(table)) {
                return table;
            }
        }
        return null;
    }
}

// End FarragoDbStmtContext.java

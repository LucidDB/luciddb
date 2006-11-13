/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
// Copyright (C) 2005-2006 LucidEra, Inc.
// Portions Copyright (C) 2003-2006 John V. Sichi
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

import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.sql.*;
import org.eigenbase.util.*;


/**
 * FarragoDbStmtContext implements the {@link
 * net.sf.farrago.session.FarragoSessionStmtContext} interface in terms of a
 * {@link FarragoDbSession}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbStmtContext
    extends FarragoDbStmtContextBase
    implements FarragoSessionStmtContext
{

    //~ Instance fields --------------------------------------------------------

    private int updateCount;
    private ResultSet resultSet;
    private FarragoSessionExecutableStmt executableStmt;
    private FarragoCompoundAllocation allocations;
    private FarragoSessionRuntimeContext runningContext;
    private final FarragoWarningQueue warningQueue;

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
        super(session, paramDefFactory, ddlLockManager);

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
        warningQueue.clearWarnings();
        unprepare();
        allocations = new FarragoCompoundAllocation();
        this.sql = sql;
        executableStmt =
            session.prepare(
                this,
                sql,
                allocations,
                isExecDirect,
                null);
        finishPrepare();
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
        warningQueue.clearWarnings();
        unprepare();
        allocations = new FarragoCompoundAllocation();
        this.sql = ""; // not available

        executableStmt =
            session.getDatabase().implementStmt(prep,
                plan,
                kind,
                logical,
                allocations);
        if (isPrepared()) {
            lockObjectsInUse(executableStmt);
        }
        finishPrepare();
    }

    // implement FarragoSessionStmtContext
    public RelDataType getPreparedRowType()
    {
        assert (isPrepared());
        return executableStmt.getRowType();
    }

    // implement FarragoSessionStmtContext
    public RelDataType getPreparedParamType()
    {
        assert (isPrepared());
        return executableStmt.getDynamicParamRowType();
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
        assert (isPrepared());
        warningQueue.clearWarnings();
        closeResultSet();
        traceExecute();
        boolean isDml = executableStmt.isDml();
        boolean success = false;

        if (session.isAutoCommit()) {
            startAutocommitTxn(!isDml);
        }

        FarragoSessionRuntimeContext newContext = null;
        try {
            checkDynamicParamsSet();
            FarragoSessionRuntimeParams params =
                session.newRuntimeContextParams();
            if (!isDml) {
                params.txnCodeCache = null;
            } else {
                params.warningQueue = warningQueue;
            }
            params.isDml = isDml;
            params.resultSetTypeMap = executableStmt.getResultSetTypeMap();
            params.iterCalcTypeMap = executableStmt.getIterCalcTypeMap();
            params.dynamicParamValues = dynamicParamValues;
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
            }
        }
        if (isDml) {
            success = false;
            try {
                boolean found = resultSet.next();
                assert (found);
                updateCount = resultSet.getInt(1);
                boolean superfluousRowCounts = resultSet.next();
                assert (!superfluousRowCounts);
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine("Update count = " + updateCount);
                }
                success = true;
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
        }
    }

    // implement FarragoSessionStmtContext
    public ResultSet getResultSet()
    {
        return resultSet;
    }

    // implement FarragoSessionStmtContext
    public int getUpdateCount()
    {
        int count = updateCount;
        updateCount = -1;
        return count;
    }

    // implement FarragoSessionStmtContext
    public void cancel()
    {
        tracer.info("cancel");
        FarragoSessionRuntimeContext contextToCancel = runningContext;
        if (contextToCancel != null) {
            contextToCancel.cancel();
        }
        // NOTE jvs 10-Apr-2006:  Don't call clearExecutingStmtInfo here,
        // because the cancel doesn't take effect immediately.  We
        // could set a flag to indicate that cancel is pending.
    }

    // implement FarragoSessionStmtContext
    public void closeResultSet()
    {
        if (resultSet == null) {
            return;
        }
        try {
            resultSet.close();
        } catch (Throwable ex) {
            throw Util.newInternal(ex);
        }
        resultSet = null;
        runningContext = null;
        clearExecutingStmtInfo();
    }

    // implement FarragoSessionStmtContext
    public void unprepare()
    {
        closeResultSet();
        if (allocations != null) {
            allocations.closeAllocation();
            allocations = null;
        }
        executableStmt = null;

        super.unprepare();
    }

    // implement FarragoSessionStmtContext
    public FarragoWarningQueue getWarningQueue()
    {
        return warningQueue;
    }
}

// End FarragoDbStmtContext.java

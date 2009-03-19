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

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.util.*;


/**
 * FarragoDbStmtContextBase provides a partial implementation of {@link
 * FarragoSessionStmtContext} in terms of {@link FarragoDbSession}.
 *
 * <p>The basic funtionality provided here may be extended to implement
 * statements in a way specific to extension projects.
 *
 * <p>See individual methods for assistance in determining when they may be
 * called.
 *
 * <p>Most non-trivial public methods on this class must be synchronized on the
 * parent session, since closeAllocation may be called from a thread shutting
 * down the database. The exception is cancel, which must NOT be synchronized,
 * since it needs to return immediately. (We synchronize on the parent session
 * to avoid deadlocks from session/stmt vs. stmt/session lock order; see
 * http://issues.eigenbase.org/browse/LDB-150 for an example.)
 *
 * @author Stephan Zuercher
 */
public abstract class FarragoDbStmtContextBase
    implements FarragoSessionStmtContext
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer =
        FarragoTrace.getDatabaseStatementContextTracer();

    //~ Instance fields --------------------------------------------------------

    protected final FarragoDbSession session;
    protected final FarragoSessionStmtParamDefFactory paramDefFactory;

    /**
     * Definitions of dynamic parameters.
     */
    protected FarragoSessionStmtParamDef [] dynamicParamDefs;

    /**
     * Current dynamic parameter bindings.
     */
    protected Object [] dynamicParamValues;

    /**
     * Indicates if dynamic parameter is set Used to validate that all dynamic
     * parameters have been set
     */
    protected boolean [] dynamicParamValuesSet;

    protected boolean daemon;

    protected String sql;

    private FarragoDdlLockManager ddlLockManager;

    private FarragoSessionExecutingStmtInfo info = null;

    private final long stmtCurrentTime;
    protected final FarragoSessionStmtContext rootStmtContext;

    /**
     * The children statement contexts associated with a root statement context.
     */
    protected List<FarragoSessionStmtContext> childrenStmtContexts;

    /**
     * If non-null, the commit sequence number to be used for all transactions
     * associated with a root context as well as children contexts associated
     * with that root context. Only used if the personality supports snapshots.
     */
    protected Long snapshotCsn;

    /**
     * Indicates whether the csn associated with the first txn initiated from a
     * root context or one of its children contexts needs to be saved. Only used
     * if the personality supports snapshots.
     */
    protected boolean saveFirstCsn;

    protected final CancelFlag cancelFlag;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoDbStmtContextBase object.
     *
     * @param session the session creating this statement
     * @param paramDefFactory dynamic parameter definition factory
     * @param ddlLockManager ddl object lock manager
     */
    protected FarragoDbStmtContextBase(
        FarragoDbSession session,
        FarragoSessionStmtParamDefFactory paramDefFactory,
        FarragoDdlLockManager ddlLockManager)
    {
        this(session, paramDefFactory, ddlLockManager, null);
    }

    /**
     * Creates a new FarragoDbStmtContextBase object.
     *
     * @param session the session creating this statement
     * @param paramDefFactory dynamic parameter definition factory
     * @param ddlLockManager ddl object lock manager
     * @param rootStmtContext the root statement context for an internally
     * prepared statement; for an externally prepared statement, this will be
     * null
     */
    protected FarragoDbStmtContextBase(
        FarragoDbSession session,
        FarragoSessionStmtParamDefFactory paramDefFactory,
        FarragoDdlLockManager ddlLockManager,
        FarragoSessionStmtContext rootStmtContext)
    {
        this.session = session;
        this.paramDefFactory = paramDefFactory;
        this.ddlLockManager = ddlLockManager;
        this.rootStmtContext = rootStmtContext;
        this.snapshotCsn = null;
        this.saveFirstCsn = false;

        // For the root context, set the current time that will be used
        // throughout the statement.  For non-root contexts, inherit the
        // time from the root context.  Also, keep track of all of the
        // children contexts associated with a root context.
        this.childrenStmtContexts = new ArrayList<FarragoSessionStmtContext>();
        if (rootStmtContext == null) {
            this.stmtCurrentTime = System.currentTimeMillis();
        } else {
            this.stmtCurrentTime = rootStmtContext.getStmtCurrentTime();
            rootStmtContext.addChildStmtContext(this);
        }
        cancelFlag = new CancelFlag();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionStmtContext
    public void closeAllocation()
    {
        synchronized (session) {
            unprepare();

            // purge self from session's list
            session.forgetAllocation(this);
        }
    }

    // implement FarragoSessionStmtContext
    public FarragoSession getSession()
    {
        return session;
    }

    // implement FarragoSessionStmtContext
    public FarragoSessionExecutingStmtInfo getExecutingStmtInfo()
    {
        return info;
    }

    // implement FarragoSessionStmtContext
    public void daemonize()
    {
        daemon = true;
    }

    // implement FarragoSessionStmtContext
    public void unprepare()
    {
        cancelFlag.clearCancel();
        synchronized (session) {
            sql = null;
            dynamicParamValues = null;
            dynamicParamValuesSet = null;

            ddlLockManager.removeObjectsInUse(this);
        }
    }

    // implement FarragoSessionStmtContext
    public void setDynamicParam(
        int parameterIndex,
        Object x)
    {
        synchronized (session) {
            assert (isPrepared());
            Object y = dynamicParamDefs[parameterIndex].scrubValue(x);
            dynamicParamValues[parameterIndex] = y;
            dynamicParamValuesSet[parameterIndex] = true;
        }
    }

    // implement FarragoSessionStmtContext
    public void setDynamicParam(
        int parameterIndex,
        Object x,
        Calendar cal)
    {
        synchronized (session) {
            assert (isPrepared());
            Object y = dynamicParamDefs[parameterIndex].scrubValue(x, cal);
            dynamicParamValues[parameterIndex] = y;
            dynamicParamValuesSet[parameterIndex] = true;
        }
    }

    // implement FarragoSessionStmtContext
    public void clearParameters()
    {
        synchronized (session) {
            assert (isPrepared());
            Arrays.fill(dynamicParamValuesSet, false);
            Arrays.fill(dynamicParamValues, null);
        }
    }

    // implement FarragoSessionStmtContext
    public String getSql()
    {
        return sql;
    }

    // implement FarragoSessionStmtContext
    public long getStmtCurrentTime()
    {
        return stmtCurrentTime;
    }

    // implement FarragoSessionStmtContext
    public void setSaveFirstTxnCsn()
    {
        assert (rootStmtContext == null);
        if (session.getPersonality().supportsFeature(
                EigenbaseResource.instance().PersonalitySupportsSnapshots))
        {
            saveFirstCsn = true;
        }
    }

    // implement FarragoSessionStmtContext
    public boolean needToSaveFirstTxnCsn()
    {
        assert (rootStmtContext == null);
        return saveFirstCsn;
    }

    // implement FarragoSessionStmtContext
    public void saveFirstTxnCsn(long csn)
    {
        assert (rootStmtContext == null);

        // Only the very first csn needs to be saved; so if one is already
        // set, don't overwrite it.  Also, only do this if the personality
        // supports snapshots.
        if ((snapshotCsn == null)
            && session.getPersonality().supportsFeature(
                EigenbaseResource.instance().PersonalitySupportsSnapshots))
        {
            snapshotCsn = new Long(csn);
        }
    }

    // implement FarragoSessionStmtContext
    public void addChildStmtContext(FarragoSessionStmtContext childStmtContext)
    {
        assert (rootStmtContext == null);
        childrenStmtContexts.add(childStmtContext);
    }

    /**
     * Starts an auto commit transaction.
     *
     * <p>Call near the beginning of {@link
     * FarragoSessionStmtContext#execute()}.
     *
     * @param readOnly true if statement is read only (e.g. not DML)
     */
    protected void startAutocommitTxn(boolean readOnly)
    {
        if (session.isTxnInProgress()) {
            ResourceDefinition stmtFeature =
                EigenbaseResource.instance()
                .SQLConformance_MultipleActiveAutocommitStatements;
            if (!session.getPersonality().supportsFeature(stmtFeature)) {
                throw EigenbaseResource.instance()
                .SQLConformance_MultipleActiveAutocommitStatements.ex();
            }
        } else {
            Long sessionLabelCsn = session.getSessionLabelCsn();
            if (sessionLabelCsn != null) {
                session.getFennelTxnContext().initiateTxnWithCsn(
                    sessionLabelCsn.longValue());
            } else if (snapshotCsn != null) {
                session.getFennelTxnContext().initiateTxnWithCsn(
                    snapshotCsn.longValue());
            } else if (readOnly) {
                session.getFennelTxnContext().initiateReadOnlyTxn();
            }
        }
    }

    /**
     * Initializes the {@link #dynamicParamDefs} and {@link #dynamicParamValues}
     * arrays based on the dynamic param row type,
     *
     * <p>Call after statement preparation, when the dynamic param row type is
     * known, but before {@link FarragoSessionStmtContext#setDynamicParam(int,
     * Object)} or {@link FarragoSessionStmtContext#clearParameters()}.
     *
     * @param dynamicParamRowType dynamic param row type (e.g., {@link
     * FarragoSessionExecutableStmt#getDynamicParamRowType()}
     */
    protected void initDynamicParams(final RelDataType dynamicParamRowType)
    {
        final RelDataTypeField [] fields = dynamicParamRowType.getFields();

        // Allocate an array to hold parameter values.
        dynamicParamValues = new Object[fields.length];

        dynamicParamValuesSet = new boolean[fields.length];

        // Allocate an array of validators, one for each parameter.
        dynamicParamDefs = new FarragoSessionStmtParamDef[fields.length];
        for (int i = 0; i < fields.length; i++) {
            final RelDataTypeField field = fields[i];
            dynamicParamDefs[i] =
                paramDefFactory.newParamDef(
                    field.getName(),
                    field.getType());
        }
    }

    /**
     * Checks that all dynamic parameters have been set.
     */
    protected void checkDynamicParamsSet()
    {
        if (dynamicParamValuesSet != null) {
            for (int i = 0; i < dynamicParamValuesSet.length; i++) {
                if (!dynamicParamValuesSet[i]) {
                    throw FarragoResource.instance().ParameterNotSet.ex(
                        Integer.toString(i + 1));
                }
            }
        }
    }

    /**
     * Acquires locks (or whatever transaction manager wants) on all tables
     * accessed by this statement.
     *
     * <p>Call during {@link FarragoSessionStmtContext#execute()} to lock tables
     * used by your {@link FarragoSessionExecutableStmt}.
     *
     * @param executableStmt executable statement
     */
    protected void accessTables(FarragoSessionExecutableStmt executableStmt)
    {
        TableAccessMap accessMap = executableStmt.getTableAccessMap();
        lockTables(accessMap);
    }

    /**
     * Acquires locks (or whatever transaction manager wants) on a single table.
     *
     * @param table fully qualified table name, represented as a list
     * @param mode access mode for the table
     */
    protected void accessTable(List<String> table, TableAccessMap.Mode mode)
    {
        TableAccessMap accessMap = new TableAccessMap(table, mode);
        lockTables(accessMap);
    }

    /**
     * Calls the transaction manager to access a set of tables.
     *
     * @param accessMap map containing the tables being accessed and their
     * access modes
     */
    private void lockTables(TableAccessMap accessMap)
    {
        FarragoSessionTxnMgr txnMgr = session.getDatabase().getTxnMgr();
        FarragoSessionTxnId txnId = session.getTxnId(true);
        txnMgr.accessTables(
            txnId,
            accessMap);
    }

    /**
     * See FarragoDbSession.
     *
     * <p>Call from {@link FarragoSessionStmtContext#prepare(RelNode, SqlKind,
     * boolean, FarragoSessionPreparingStmt)} after preparation is complete.
     *
     * @param newExecutableStmt
     */
    protected void lockObjectsInUse(
        FarragoSessionExecutableStmt newExecutableStmt)
    {
        // TODO jvs 17-Mar-2006:  as a sanity check, verify at the
        // beginning of each execution that all objects still exist
        // (to make sure that a DROP didn't sneak in somehow)
        ddlLockManager.addObjectsInUse(
            this,
            newExecutableStmt.getReferencedObjectIds());
    }

    /**
     * Marks a single object, represented by its mofId, as in-use.
     *
     * @param mofId mofId of the object being marked as in-use
     */
    protected void lockObjectInUse(String mofId)
    {
        Set<String> mofIds = new HashSet<String>();
        mofIds.add(mofId);
        ddlLockManager.addObjectsInUse(this, mofIds);
    }

    /**
     * Initializes the session's {@link FarragoSessionExecutingStmtInfo}.
     *
     * <p>Call before {@link FarragoSessionExecutableStmt#execute(
     * FarragoSessionRuntimeContext)}.
     *
     * @param executableStmt executable statement
     */
    protected void initExecutingStmtInfo(
        FarragoSessionExecutableStmt executableStmt)
    {
        Set<String> objectsInUse = executableStmt.getReferencedObjectIds();
        info =
            new FarragoDbSessionExecutingStmtInfo(
                this,
                session.getDatabase(),
                sql,
                Arrays.asList(dynamicParamValues),
                Arrays.asList(
                    objectsInUse.toArray(new String[objectsInUse.size()])));
        FarragoDbSessionInfo sessionInfo =
            (FarragoDbSessionInfo) session.getSessionInfo();
        sessionInfo.addExecutingStmtInfo(info);
    }

    /**
     * Clears session's {@link FarragoSessionExecutingStmtInfo}.
     *
     * <p>Call on {@link FarragoSessionStmtContext#cancel()}, {@link
     * FarragoSessionStmtContext#closeResultSet()}, or whenever the statement is
     * known to have stopped executing.
     */
    protected void clearExecutingStmtInfo()
    {
        cancelFlag.clearCancel();
        if (info == null) {
            return;
        }
        long key = info.getId();
        getSessionInfo().removeExecutingStmtInfo(key);
        info = null;
    }

    // implement FarragoSessionStmtContext
    public CancelFlag getCancelFlag()
    {
        return cancelFlag;
    }

    /**
     * Traces execution.
     *
     * <p>Optionally, call from {@link FarragoSessionStmtContext#execute()}.
     */
    protected void traceExecute()
    {
        if (!tracer.isLoggable(Level.FINE)) {
            return;
        }
        tracer.fine(sql);
        if (!tracer.isLoggable(Level.FINER)) {
            return;
        }
        for (int i = 0; i < dynamicParamValues.length; ++i) {
            tracer.finer("?" + (i + 1) + " = [" + dynamicParamValues[i] + "]");
        }
    }

    private FarragoDbSessionInfo getSessionInfo()
    {
        return (FarragoDbSessionInfo) session.getSessionInfo();
    }
}

// End FarragoDbStmtContextBase.java

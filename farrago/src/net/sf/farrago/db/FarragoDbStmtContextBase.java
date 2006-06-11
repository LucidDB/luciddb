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

import java.util.Arrays;
import java.util.Set;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.eigenbase.relopt.TableAccessMap;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.resgen.ResourceDefinition;
import org.eigenbase.resource.EigenbaseResource;

import net.sf.farrago.session.FarragoSession;
import net.sf.farrago.session.FarragoSessionExecutableStmt;
import net.sf.farrago.session.FarragoSessionExecutingStmtInfo;
import net.sf.farrago.session.FarragoSessionStmtContext;
import net.sf.farrago.session.FarragoSessionStmtParamDef;
import net.sf.farrago.session.FarragoSessionStmtParamDefFactory;
import net.sf.farrago.session.FarragoSessionTxnId;
import net.sf.farrago.session.FarragoSessionTxnMgr;
import net.sf.farrago.trace.FarragoTrace;
import net.sf.farrago.util.FarragoDdlLockManager;
import net.sf.farrago.resource.FarragoResource;

/**
 * FarragoDbStmtContextBase provides a partial implementation of
 * {@link FarragoSessionStmtContext} in terms of {@link FarragoDbSession}.
 * 
 * <p>The basic funtionality provided here may be extended to implement
 * statements in a way specific to extension projects.
 * 
 * <p>See individual methods for assistance in determining when they may
 * be called.
 * 
 * @author Stephan Zuercher
 */
public abstract class FarragoDbStmtContextBase 
    implements FarragoSessionStmtContext
{
    //~ Static fields/initializers --------------------------------------------

    protected static final Logger tracer =
        FarragoTrace.getDatabaseStatementContextTracer();

    //~ Instance fields -------------------------------------------------------

    protected FarragoDbSession session;
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
     * Indicates if dynamic parameter is set
     * Used to validate that all dynamic parameters have been set
     */
    protected boolean[] dynamicParamValuesSet;

    protected boolean daemon;

    protected String sql;

    private FarragoDdlLockManager ddlLockManager;

    private FarragoSessionExecutingStmtInfo info = null;

    //~ Constructors ----------------------------------------------------------

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
        this.session = session;
        this.paramDefFactory = paramDefFactory;
        this.ddlLockManager = ddlLockManager;
    }
    
    
    // implement FarragoSessionStmtContext
    public void closeAllocation()
    {
        unprepare();
    
        // purge self from session's list
        session.forgetAllocation(this);
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
        sql = null;
        dynamicParamValues = null;
        dynamicParamValuesSet = null;

        ddlLockManager.removeObjectsInUse(this);
    }
    
    // implement FarragoSessionStmtContext
    public void setDynamicParam(int parameterIndex, Object x)
    {
        assert (isPrepared());
        Object y = dynamicParamDefs[parameterIndex].scrubValue(x);
        dynamicParamValues[parameterIndex] = y;
        dynamicParamValuesSet[parameterIndex] = true;
    }

    // implement FarragoSessionStmtContext
    public void setDynamicParam(int parameterIndex, Object x, Calendar cal)
    {
        assert (isPrepared());
        Object y = dynamicParamDefs[parameterIndex].scrubValue(x, cal);
        dynamicParamValues[parameterIndex] = y;
        dynamicParamValuesSet[parameterIndex] = true;
    }

    // implement FarragoSessionStmtContext
    public void clearParameters()
    {
        assert (isPrepared());
        Arrays.fill(dynamicParamValuesSet, false);
        Arrays.fill(dynamicParamValues, null);
    }

    // implement FarragoSessionStmtContext
    public String getSql()
    {
        return sql;
    }

    /**
     * Starts an auto commit transaction.
     * 
     * <p>Call near the beginning of 
     * {@link FarragoSessionStmtContext#execute()}.
     * 
     * @param readOnly true if statement is read only (e.g. not DML)
     */
    protected void startAutocommitTxn(boolean readOnly)
    {
        if (session.isTxnInProgress()) {
            ResourceDefinition stmtFeature = EigenbaseResource.instance()
                .SQLConformance_MultipleActiveAutocommitStatements;
            if (!session.getPersonality().supportsFeature(stmtFeature)) {
                throw EigenbaseResource.instance()
                    .SQLConformance_MultipleActiveAutocommitStatements.ex();
            }
        } else {
            if (readOnly) {
                session.getFennelTxnContext().initiateReadOnlyTxn();
            }
        }
    }

    /**
     * Initializes the {@link #dynamicParamDefs} and 
     * {@link #dynamicParamValues} arrays based on the dynamic param row
     * type,
     * 
     * <p>Call after statement preparation, when the dynamic param row
     * type is known, but before 
     * {@link FarragoSessionStmtContext#setDynamicParam(int, Object)} or
     * {@link FarragoSessionStmtContext#clearParameters()}.
     * 
     * @param dynamicParamRowType dynamic param row type (e.g.,
     *            {@link FarragoSessionExecutableStmt#getDynamicParamRowType()}
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
     * Checks that all dynamic parameters have been set
     */
    protected void checkDynamicParamsSet()
    {
        if (dynamicParamValuesSet != null) {
            for (int i = 0; i < dynamicParamValuesSet.length; i++) {
                if (!dynamicParamValuesSet[i]) {
                    throw FarragoResource.instance().ParameterNotSet.ex(
                        Integer.toString(i+1));
                }
            }
        }
    }

    /**
     * Acquires locks (or whatever transaction manager wants) on all
     * tables accessed by this statement.
     * 
     * <p>Call during {@link FarragoSessionStmtContext#execute()} to lock
     * tables used by your {@link FarragoSessionExecutableStmt}.
     *
     * @param executableStmt executable statement
     */
    protected void accessTables(FarragoSessionExecutableStmt executableStmt)
    {
        TableAccessMap accessMap = executableStmt.getTableAccessMap();
        FarragoSessionTxnMgr txnMgr = 
            session.getDatabase().getTxnMgr();
        FarragoSessionTxnId txnId = session.getTxnId(true);
        txnMgr.accessTables(
            txnId,
            accessMap);
    }
    
    /**
     * See FarragoDbSession.
     * 
     * <p>Call from 
     * {@link FarragoSessionStmtContext#prepare(RelNode, SqlKind, boolean, 
     *                                            FarragoSessionPreparingStmt)}
     * after preparation is complete.
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
            this, newExecutableStmt.getReferencedObjectIds());
    }

    /**
     * Initializes the session's {@link FarragoSessionExecutingStmtInfo}.
     * 
     * <p>Call before
     * {@link FarragoSessionExecutableStmt#execute(
     *                                          FarragoSessionRuntimeContext)}.
     * 
     * @param executableStmt executable statement
     */
    protected void initExecutingStmtInfo(
        FarragoSessionExecutableStmt executableStmt)
    {
        Set<String> objectsInUse = executableStmt.getReferencedObjectIds();
        info = new FarragoDbSessionExecutingStmtInfo(
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
     * <p>Call on {@link FarragoSessionStmtContext#cancel()},
     * {@link FarragoSessionStmtContext#closeResultSet()}, or
     * whenever the statement is known to have stopped executing.
     */
    protected void clearExecutingStmtInfo()
    {
        if (info == null) {
            return;
        }
        long key = info.getId();
        getSessionInfo().removeExecutingStmtInfo(key);
        info = null;
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
        return (FarragoDbSessionInfo)session.getSessionInfo();
    }
}

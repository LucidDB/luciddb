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

package net.sf.farrago.db;

import net.sf.farrago.catalog.*;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;

import net.sf.saffron.core.*;
import net.sf.saffron.util.*;
import net.sf.saffron.oj.stmt.*;

import java.util.*;
import java.util.logging.*;
import java.sql.*;

/**
 * FarragoDbStmtContext implements the
 * {@link net.sf.farrago.session.FarragoSessionStmtContext} interface
 * in terms of a {@link FarragoDbSession}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FarragoDbStmtContext
    implements FarragoSessionStmtContext
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoDbStmtContext.class);

    //~ Instance fields -------------------------------------------------------

    private int updateCount;

    private FarragoDbSession session;

    private Object [] dynamicParamValues;

    private boolean daemon;

    private ResultSet resultSet;

    private FarragoExecutableStmt executableStmt;

    private FarragoCompoundAllocation allocations;
    
    private String sql;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoDbStmtContext object.
     *
     * @param session the session creating this statement
     */
    FarragoDbStmtContext(FarragoDbSession session)
    {
        this.session = session;
        updateCount = -1;
        this.daemon = daemon;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoAllocation
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
    public void daemonize()
    {
        daemon = true;
    }

    // implement FarragoSessionStmtContext
    public void prepare(String sql,boolean isExecDirect)
    {
        unprepare();
        allocations = new FarragoCompoundAllocation();
        this.sql = sql;
        executableStmt = session.prepare(
            sql,allocations,isExecDirect,null);
        if (isPrepared()) {
            dynamicParamValues = new Object[
                executableStmt.getDynamicParamRowType().getFieldCount()];
        } else {
            // always zero for DDL
            updateCount = 0;
        }
    }

    // implement FarragoSessionStmtContext
    public SaffronType getPreparedRowType()
    {
        assert(isPrepared());
        return executableStmt.getRowType();
    }
    
    // implement FarragoSessionStmtContext
    public SaffronType getPreparedParamType()
    {
        assert(isPrepared());
        return executableStmt.getDynamicParamRowType();
    }

    // implement FarragoSessionStmtContext
    public void setDynamicParam(int parameterIndex,Object x)
    {
        assert(isPrepared());
        // TODO:  type/null checking
        dynamicParamValues[parameterIndex] = x;
    }
    
    // implement FarragoSessionStmtContext
    public void clearParameters()
    {
        assert(isPrepared());
        Arrays.fill(dynamicParamValues,null);
    }
    
    // implement FarragoSessionStmtContext
    public void execute()
    {
        assert(isPrepared());
        closeResultSet();
        traceExecute();
        boolean isDml = executableStmt.isDml();
        boolean success = false;
        try {
            FarragoRuntimeContextParams params =
                session.newRuntimeContextParams();
            if (!isDml) {
                params.txnCodeCache = null;
            }
            params.dynamicParamValues = dynamicParamValues;
            FarragoRuntimeContext context = session.newRuntimeContext(params);
            if (allocations != null) {
                context.addAllocation(allocations);
                allocations = null;
            }
            if (daemon) {
                context.addAllocation(this);
            }
            resultSet = executableStmt.execute(context);
            success = true;
        } finally {
            if (!success) {
                session.endTransactionIfAuto(false);
            }
        }
        if (isDml) {
            success = false;
            try {
                if (session.getCatalog().isFennelEnabled()) {
                    boolean found = resultSet.next();
                    assert (found);
                    updateCount = resultSet.getInt(1);
                } else {
                    updateCount = 0;
                }
                if (tracer.isLoggable(Level.FINE)) {
                    tracer.fine("Update count = " + updateCount);
                }
                success = true;
            } catch (SQLException ex) {
                throw Util.newInternal(ex);
            } finally {
                if (!success) {
                    session.endTransactionIfAuto(false);
                }
                try {
                    resultSet.close();
                } catch (SQLException ex) {
                    throw Util.newInternal(ex);
                } finally {
                    resultSet = null;
                }
            }
        }

        // NOTE:  for now, we only auto-commit after DML.  Queries aren't an
        // issue until locking gets implemented.
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
    public void closeResultSet()
    {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (Throwable ex) {
                throw Util.newInternal(ex);
            }
            resultSet = null;
        }
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
        sql = null;
        dynamicParamValues = null;
    }

    void traceExecute()
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

}


// End FarragoDbStmtContext.java

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

package net.sf.farrago.jdbc;

import net.sf.farrago.catalog.*;
import net.sf.farrago.query.*;
import net.sf.farrago.util.*;
import net.sf.farrago.runtime.*;

import net.sf.saffron.jdbc.*;
import net.sf.saffron.util.*;
import net.sf.saffron.oj.stmt.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;


/**
 * FarragoJdbcStatement subclasses SaffronJdbcStatement to implement
 * Farrago-specific details of statement execution.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcStatement
    extends SaffronJdbcStatement
    implements FarragoAllocation
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoJdbcStatement.class);

    protected static final String ERRMSG_NOT_A_QUERY =
    "Not a query:  ";

    protected static final String ERRMSG_IS_A_QUERY =
    "Can't executeUpdate a query:  ";

    //~ Instance fields -------------------------------------------------------

    /**
     * Number of rows affected by last update, or -1 if last statement was not
     * DML or its update count was already returned.
     */
    protected int updateCount;

    /**
     * Connection through which this stmt was created.
     */
    protected FarragoJdbcConnection farragoConnection;

    /**
     * Values to bind to dynamic parameters.
     */
    protected Object [] dynamicParamValues;

    private boolean daemon;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcStatement object.
     *
     * @param connection the connection creating this statement
     */
    FarragoJdbcStatement(FarragoJdbcConnection connection)
    {
        super(connection);
        this.farragoConnection = connection;
        updateCount = -1;
        this.daemon = daemon;
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Turn this statement into a daemon.
     */
    public void daemonize()
    {
        daemon = true;
    }
    
    // implement FarragoAllocation
    public void closeAllocation()
    {
        try {
            close();
        } catch (SQLException ex) {
            throw Util.newInternal(ex);
        }
    }
    
    // implement Statement
    public void setEscapeProcessing(boolean enable) throws SQLException
    {
        // TODO:
    }
    
    // implement Statement
    public boolean getMoreResults() throws SQLException
    {
        if (resultSet != null) {
            resultSet.close();
            resultSet = null;
        }
        return false;
    }

    // implement Statement
    public int getUpdateCount() throws SQLException
    {
        int count = updateCount;
        updateCount = -1;
        return count;
    }

    // implement Statement
    public boolean execute(String sql) throws SQLException
    {
        FarragoCompoundAllocation allocations = new FarragoCompoundAllocation();
        FarragoExecutableStmt stmt = farragoConnection.prepareImpl(
            sql,allocations,true,null);
        if (stmt != null) {
            executeImpl(stmt,allocations);
        }
        return (resultSet != null);
    }

    // implement Statement
    public int executeUpdate(String sql) throws SQLException
    {
        FarragoCompoundAllocation allocations = new FarragoCompoundAllocation();
        FarragoExecutableStmt stmt = farragoConnection.prepareImpl(
            sql,allocations,true,null);
        if (stmt != null) {
            if (!stmt.isDml()) {
                allocations.closeAllocation();
                throw new SQLException(ERRMSG_IS_A_QUERY + sql);
            }
            executeImpl(stmt,allocations);
        }
        assert(resultSet == null);
        int count = getUpdateCount();
        if (count == -1) {
            count = 0;
        }
        return count;
    }
    
    // implement Statement
    public ResultSet executeQuery(String sql) throws SQLException
    {
        FarragoCompoundAllocation allocations = new FarragoCompoundAllocation();
        FarragoExecutableStmt stmt = farragoConnection.prepareImpl(
            sql,allocations,true,null);
        if (stmt == null || stmt.isDml()) {
            allocations.closeAllocation();
            throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
        }
        executeImpl(stmt,allocations);
        assert(resultSet != null);
        return resultSet;
    }

    /**
     * Execute a previously prepared FarragoExecutableStmt.
     *
     * @param stmt the prepared FarragoExecutableStmt to execute
     *
     * @param allocation allocation to close when result set is closed,
     * or null if none
     */
    protected void executeImpl(
        FarragoExecutableStmt stmt,
        FarragoAllocation allocation)
        throws SQLException
    {
        boolean isDml = stmt.isDml();
        try {
            Map txnCodeCache = null;
            if (isDml) {
                txnCodeCache = farragoConnection.txnCodeCache;
            }
            FarragoRuntimeContext context = new FarragoRuntimeContext(
                farragoConnection.getFarragoCatalog(),
                farragoConnection.getDatabase().getCodeCache(),
                txnCodeCache,
                farragoConnection.getFennelTxnContext(),
                farragoConnection.sessionIndexMap,
                dynamicParamValues,
                farragoConnection.connectionDefaults.cloneDefaults(),
                farragoConnection.getDatabase().getDataWrapperCache());
            if (allocation != null) {
                context.addAllocation(allocation);
            }
            if (daemon) {
                context.addAllocation(this);
            }
            resultSet = stmt.execute(context);
        } catch (Throwable ex) {
            farragoConnection.endTransactionIfAuto(false);
            throw FarragoJdbcDriver.newSqlException(ex);
        }
        if (isDml) {
            try {
                if (farragoConnection.getFarragoCatalog().isFennelEnabled()) {
                    boolean found = resultSet.next();
                    assert (found);
                    updateCount = resultSet.getInt(1);
                } else {
                    updateCount = 0;
                }
            } catch (Throwable ex) {
                farragoConnection.endTransactionIfAuto(false);
                throw FarragoJdbcDriver.newSqlException(ex);
            } finally {
                try {
                    resultSet.close();
                } finally {
                    resultSet = null;
                }
            }
        }

        // NOTE:  for now, we only auto-commit after DML.  Queries aren't an
        // issue until locking gets implemented.
        if (resultSet == null) {
            farragoConnection.endTransactionIfAuto(true);
        }
    }
}


// End FarragoJdbcStatement.java

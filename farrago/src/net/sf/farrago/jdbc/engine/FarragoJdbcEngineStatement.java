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
package net.sf.farrago.jdbc.engine;

import java.sql.*;

import net.sf.farrago.jdbc.*;
import net.sf.farrago.session.*;


/**
 * FarragoJdbcEngineStatement implements the {@link java.sql.Statement}
 * interface for the Farrago JDBC driver, including extensions from {@link
 * net.sf.farrago.jdbc.FarragoStatement}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineStatement
    implements FarragoStatement
{

    //~ Static fields/initializers ---------------------------------------------

    protected static final String ERRMSG_NOT_A_QUERY = "Not a query:  ";
    protected static final String ERRMSG_IS_A_QUERY =
        "Can't executeUpdate a query:  ";

    //~ Instance fields --------------------------------------------------------

    /**
     * Connection through which this stmt was created.
     */
    protected FarragoJdbcEngineConnection connection;

    /**
     * Underlying statement context.
     */
    protected FarragoSessionStmtContext stmtContext;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEngineStatement object.
     *
     * @param connection the connection creating this statement
     * @param stmtContext underlying FarragoSessionStmtContext
     */
    FarragoJdbcEngineStatement(
        FarragoJdbcEngineConnection connection,
        FarragoSessionStmtContext stmtContext)
    {
        this.connection = connection;
        this.stmtContext = stmtContext;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Statement
    public void setEscapeProcessing(boolean enable)
        throws SQLException
    {
        // TODO:
    }

    // implement Statement
    public boolean getMoreResults()
        throws SQLException
    {
        stmtContext.closeResultSet();
        return false;
    }

    // implement Statement
    public int getUpdateCount()
        throws SQLException
    {
        return (int) stmtContext.getUpdateCount();
    }

    // implement Statement
    public boolean execute(String sql)
        throws SQLException
    {
        boolean unprepare = true;
        try {
            stmtContext.prepare(sql, true);
            if (stmtContext.isPrepared()) {
                stmtContext.execute();

                if (stmtContext.getResultSet() != null) {
                    unprepare = false;
                    return true;
                } else {
                    return false;
                }
            } else {
                return false;
            }
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        } finally {
            if (unprepare) {
                stmtContext.unprepare();
            } else {
                // REVIEW:  make sure everything is going to get
                // deallocated correctly when result set is closed
            }
        }
    }

    // implement Statement
    public int executeUpdate(String sql)
        throws SQLException
    {
        try {
            stmtContext.prepare(sql, true);
            if (stmtContext.isPrepared()) {
                if (!stmtContext.isPreparedDml()) {
                    throw new SQLException(ERRMSG_IS_A_QUERY + sql);
                }
                stmtContext.execute();
                assert (stmtContext.getResultSet() == null);
                int count = (int) stmtContext.getUpdateCount();
                if (count == -1) {
                    count = 0;
                }
                return count;
            } else {
                return 0;
            }
        } catch (SQLException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        } finally {
            stmtContext.unprepare();
        }
    }

    // implement Statement
    public ResultSet executeQuery(String sql)
        throws SQLException
    {
        boolean unprepare = true;
        try {
            stmtContext.prepare(sql, true);
            if (!stmtContext.isPrepared() || stmtContext.isPreparedDml()) {
                throw new SQLException(ERRMSG_NOT_A_QUERY + sql);
            }
            stmtContext.execute();
            assert (stmtContext.getResultSet() != null);
            unprepare = false;
            return stmtContext.getResultSet();
        } catch (SQLException ex) {
            throw ex;
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        } finally {
            if (unprepare) {
                stmtContext.unprepare();
            } else {
                // REVIEW:  make sure everything is going to get
                // deallocated correctly when result set is closed
            }
        }
    }

    // implement Statement
    public Connection getConnection()
        throws SQLException
    {
        return connection;
    }

    // implement Statement
    public void setCursorName(String name)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void setFetchDirection(int direction)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int getFetchDirection()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void setFetchSize(int rows)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int getFetchSize()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public ResultSet getGeneratedKeys()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void setMaxFieldSize(int max)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int getMaxFieldSize()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void setMaxRows(int max)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int getMaxRows()
        throws SQLException
    {
        return 0;
    }

    // implement Statement
    public boolean getMoreResults(int current)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void setQueryTimeout(int seconds)
        throws SQLException
    {
        stmtContext.setQueryTimeout(seconds * 1000);
    }

    // implement Statement
    public int getQueryTimeout()
        throws SQLException
    {
        int timeoutMillis = stmtContext.getQueryTimeout();
        if ((timeoutMillis > 0) && (timeoutMillis < 1000)) {
            // Don't let 1ms become 0s (because that means no timeout).
            timeoutMillis = 1000;
        }
        return timeoutMillis / 1000;
    }

    // implement Statement
    public ResultSet getResultSet()
        throws SQLException
    {
        return stmtContext.getResultSet();
    }

    // implement Statement
    public int getResultSetConcurrency()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int getResultSetHoldability()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int getResultSetType()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void addBatch(String sql)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public void cancel()
        throws SQLException
    {
        if (stmtContext != null) {
            stmtContext.cancel();
        }
    }

    // implement Statement
    public void clearBatch()
        throws SQLException
    {
    }

    // implement Statement
    public void close()
        throws SQLException
    {
        try {
            if (stmtContext != null) {
                stmtContext.closeAllocation();
            }
        } finally {
            stmtContext = null;
        }
    }

    // implement Statement
    public boolean execute(
        String sql,
        int autoGeneratedKeys)
        throws SQLException
    {
        if (autoGeneratedKeys != NO_GENERATED_KEYS) {
            throw new UnsupportedOperationException();
        }
        return execute(sql);
    }

    // implement Statement
    public boolean execute(
        String sql,
        int [] columnIndexes)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public boolean execute(
        String sql,
        String [] columnNames)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int [] executeBatch()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int executeUpdate(
        String sql,
        int autoGeneratedKeys)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int executeUpdate(
        String sql,
        int [] columnIndexes)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Statement
    public int executeUpdate(
        String sql,
        String [] columnNames)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement FarragoStatement
    public long getFarragoExecutingStmtId()
    {
        if (stmtContext == null) {
            return 0;
        }
        FarragoSessionExecutingStmtInfo info =
            stmtContext.getExecutingStmtInfo();
        if (info == null) {
            return 0;
        }
        return info.getId();
    }
    
    // implement Statement
    public SQLWarning getWarnings()
        throws SQLException
    {
        if (stmtContext == null) {
            return null;
        }
        return stmtContext.getWarningQueue().getWarnings();
    }

    // implement Statement
    public void clearWarnings()
        throws SQLException
    {
        if (stmtContext == null) {
            return;
        }
        stmtContext.getWarningQueue().clearWarnings();
    }
}

// End FarragoJdbcEngineStatement.java

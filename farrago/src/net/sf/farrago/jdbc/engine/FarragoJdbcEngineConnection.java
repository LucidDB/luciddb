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
package net.sf.farrago.jdbc.engine;

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.util.*;


/**
 * FarragoJdbcEngineConnection implements the {@link java.sql.Connection}
 * interface for the Farrago JDBC engine driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineConnection implements Connection
{
    //~ Instance fields -------------------------------------------------------

    private FarragoSessionFactory sessionFactory;
    private FarragoSession session;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEngineConnection object.
     *
     * @param url URL used to connect
     *
     * @param info properties for this connection
     *
     * @param sessionFactory FarragoSessionFactory governing this connection's
     * behavior
     */
    public FarragoJdbcEngineConnection(
        String url,
        Properties info,
        FarragoSessionFactory sessionFactory)
        throws SQLException
    {
        this.sessionFactory = sessionFactory;
        session = sessionFactory.newSession(url, info);
        session.setDatabaseMetaData(getMetaData());
    }

    //~ Methods ---------------------------------------------------------------

    public FarragoSession getSession()
    {
        return session;
    }

    // implement Connection
    public boolean isClosed()
        throws SQLException
    {
        return (session == null);
    }

    // implement Connection
    public void setAutoCommit(boolean autoCommit)
        throws SQLException
    {
        try {
            session.setAutoCommit(autoCommit);
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public boolean getAutoCommit()
        throws SQLException
    {
        return session.isAutoCommit();
    }

    public void setCatalog(String catalog)
        throws SQLException
    {
        // TODO
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public String getCatalog()
        throws SQLException
    {
        return session.getSessionVariables().catalogName;
    }

    // implement Connection
    public void close()
        throws SQLException
    {
        try {
            try {
                session.closeAllocation();
                if (session.isClone()) {
                    return;
                }
            } catch (Throwable ex) {
                throw FarragoJdbcEngineDriver.newSqlException(ex);
            }
            if (session.isTxnInProgress()) {
                // TODO:  generate SQLException in FarragoResource?
                throw new SQLException(
                    FarragoResource.instance().getJdbcInvalidTxnState(),
                    "25000");
            }
            sessionFactory.cleanupSessions();
        } finally {
            session = null;
        }
    }

    public void commit()
        throws SQLException
    {
        try {
            session.commit();
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public Statement createStatement()
        throws SQLException
    {
        try {
            return new FarragoJdbcEngineStatement(
                this,
                session.newStmtContext());
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public void rollback()
        throws SQLException
    {
        try {
            session.rollback(null);
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public void rollback(Savepoint savepoint)
        throws SQLException
    {
        FarragoSessionSavepoint farragoSavepoint =
            validateSavepoint(savepoint);
        try {
            session.rollback(farragoSavepoint);
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public void setTransactionIsolation(int level)
        throws SQLException
    {
        // TODO:  implement this; dummied out for now to shut sqlline up
    }

    public int getTransactionIsolation()
        throws SQLException
    {
        // TODO:  implement this; dummied out for now to shut sqlline up
        return TRANSACTION_READ_UNCOMMITTED;
    }

    // implement Connection
    public Savepoint setSavepoint()
        throws SQLException
    {
        try {
            return new FarragoJdbcEngineSavepoint(session.newSavepoint(null));
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public Savepoint setSavepoint(String name)
        throws SQLException
    {
        try {
            return new FarragoJdbcEngineSavepoint(session.newSavepoint(name));
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    private FarragoSessionSavepoint validateSavepoint(Savepoint savepoint)
        throws SQLException
    {
        if (!(savepoint instanceof FarragoJdbcEngineSavepoint)) {
            throw new SQLException("Savepoint class not recognized");
        }
        return ((FarragoJdbcEngineSavepoint) savepoint).farragoSavepoint;
    }

    // implement Connection
    public void releaseSavepoint(Savepoint savepoint)
        throws SQLException
    {
        FarragoSessionSavepoint farragoSavepoint =
            validateSavepoint(savepoint);
        try {
            session.releaseSavepoint(farragoSavepoint);
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public DatabaseMetaData getMetaData()
        throws SQLException
    {
        return new FarragoJdbcEngineDatabaseMetaData(this);
    }

    // implement Connection
    public PreparedStatement prepareStatement(String sql)
        throws SQLException
    {
        FarragoSessionStmtContext stmtContext = null;
        try {
            stmtContext = session.newStmtContext();
            stmtContext.prepare(sql, false);
            FarragoJdbcEnginePreparedStatement preparedStmt;
            if (!stmtContext.isPrepared()) {
                preparedStmt =
                    new FarragoJdbcEnginePreparedDdl(this, stmtContext, sql);
            } else {
                preparedStmt =
                    new FarragoJdbcEnginePreparedNonDdl(this, stmtContext, sql);
                stmtContext = null;
            }
            return preparedStmt;
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        } finally {
            if (stmtContext != null) {
                stmtContext.unprepare();
            }
        }
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int autoGeneratedKeys)
        throws SQLException
    {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new UnsupportedOperationException();
        }
        return prepareStatement(sql);
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int [] columnIndexes)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        String [] columnNames)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public void setHoldability(int holdability)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public int getHoldability()
        throws SQLException
    {
        return 0;
    }

    public void setReadOnly(boolean readOnly)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public boolean isReadOnly()
        throws SQLException
    {
        return false;
    }

    public void setTypeMap(Map map)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public Map getTypeMap()
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public SQLWarning getWarnings()
        throws SQLException
    {
        return null;
    }

    public void clearWarnings()
        throws SQLException
    {
    }

    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency)
        throws SQLException
    {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY) {
            throw new UnsupportedOperationException();
        }
        if (resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new UnsupportedOperationException();
        }
        return createStatement();
    }

    public Statement createStatement(
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public String nativeSQL(String sql)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(String sql)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    public CallableStatement prepareCall(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }
}


// End FarragoJdbcEngineConnection.java

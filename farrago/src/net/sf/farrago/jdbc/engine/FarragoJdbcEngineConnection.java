/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.db.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.jdbc.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.sql.*;
import org.eigenbase.sql.parser.*;
import org.eigenbase.util.Util;


/**
 * FarragoJdbcEngineConnection implements the {@link java.sql.Connection}
 * interface for the Farrago JDBC engine driver.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcEngineConnection
    implements FarragoConnection,
        FarragoSessionConnectionSource
{
    //~ Instance fields --------------------------------------------------------

    private FarragoSessionFactory sessionFactory;
    private FarragoSession session;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcEngineConnection object.
     *
     * @param url URL used to connect
     * @param info properties for this connection
     * @param sessionFactory FarragoSessionFactory governing this connection's
     * behavior
     */
    public FarragoJdbcEngineConnection(
        String url,
        Properties info,
        FarragoSessionFactory sessionFactory)
        throws SQLException
    {
        this(sessionFactory.newSession(url, info));
        this.sessionFactory = sessionFactory;
        try {
            initConnection(info);
        } catch (SQLException e) {
            close();        // prevent leak
            throw e;
        }
    }

    private FarragoJdbcEngineConnection(
        FarragoSession session)
    {
        this.session = session;
        session.setDatabaseMetaData(
            new FarragoJdbcEngineDatabaseMetaData(this));
        session.setConnectionSource(this);
    }

    //~ Methods ----------------------------------------------------------------

    public FarragoSession getSession()
    {
        return session;
    }

    // implement FarragoConnection
    public long getFarragoSessionId()
    {
        if ((session == null) || session.isClosed()) {
            return 0;
        }
        return session.getSessionInfo().getId();
    }

    // implement Connection
    public boolean isClosed()
        throws SQLException
    {
        return (session == null);
    }

    // implement FarragoSessionConnectionSource
    public Connection newConnection()
    {
        return new FarragoJdbcEngineConnection(
            session.cloneSession(null));
    }

    // implement Connection
    public void setAutoCommit(boolean autoCommit)
        throws SQLException
    {
        validateSession();

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
        validateSession();

        return session.isAutoCommit();
    }

    // implement Connection
    public void setCatalog(String catalog)
        throws SQLException
    {
        // TODO
        return; // until implemented, JDBC API doc says to silently ignore
    }

    // implement Connection
    public String getCatalog()
        throws SQLException
    {
        validateSession();

        return session.getSessionVariables().catalogName;
    }

    // implement Connection
    public void close()
        throws SQLException
    {
        if (session == null) {
            return;
        }
        try {
            try {
                if (session.isClosed()) {
                    // Already closed internally by something like
                    // a database shutdown; pop out now to avoid assertions.
                    return;
                }
                session.closeAllocation();
                if (session.isClone()) {
                    return;
                }
            } catch (Throwable ex) {
                throw FarragoJdbcEngineDriver.newSqlException(ex);
            }
            sessionFactory.cleanupSessions();
        } finally {
            session = null;
        }
    }

    public void commit()
        throws SQLException
    {
        validateSession();

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
        validateSession();

        try {
            // FarragoSessionStmtContext created without a param def factory
            // because plain Statements cannot use dynamic parameters.
            return new FarragoJdbcEngineStatement(
                this,
                session.newStmtContext(null));
        } catch (Throwable ex) {
            throw FarragoJdbcEngineDriver.newSqlException(ex);
        }
    }

    // implement Connection
    public void rollback()
        throws SQLException
    {
        validateSession();

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
        validateSession();

        FarragoSessionSavepoint farragoSavepoint = validateSavepoint(savepoint);
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
        if (getMetaData().supportsTransactions()) {
            return TRANSACTION_READ_UNCOMMITTED;
        } else {
            return TRANSACTION_NONE;
        }
    }

    // implement Connection
    public Savepoint setSavepoint()
        throws SQLException
    {
        validateSession();

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
        validateSession();

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
        validateSession();

        FarragoSessionSavepoint farragoSavepoint = validateSavepoint(savepoint);
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
        validateSession();

        return session.getDatabaseMetaData();
    }

    // implement Connection
    public PreparedStatement prepareStatement(String sql)
        throws SQLException
    {
        validateSession();

        FarragoSessionStmtContext stmtContext = null;
        try {
            stmtContext =
                session.newStmtContext(new FarragoJdbcEngineParamDefFactory());
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
        // TODO jvs 16-June-2006: Enforce read-only.  For now we just ignore
        // it, since the JDBC javadoc says this is just a hint, and
        // some clients (such as Mondrian) choke if we throw an
        // exception.
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

    // implement Connection
    public SQLWarning getWarnings()
        throws SQLException
    {
        validateSession();

        return session.getWarningQueue().getWarnings();
    }

    // implement Connection
    public void clearWarnings()
        throws SQLException
    {
        validateSession();

        session.getWarningQueue().clearWarnings();
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

    /**
     * Performs additional setup based on connection property settings.
     *
     * @param info connection properties
     *
     * @throws SQLException
     */
    protected void initConnection(Properties info)
        throws SQLException
    {
        String initialSchema = info.getProperty("schema");
        if (initialSchema != null) {
            Statement stmt = this.createStatement();
            try {
                stmt.executeUpdate("set schema '" + initialSchema + "'");
            } finally {
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // allow executeUpdate() exception to propagate
                    Util.swallow(e, null);
                }
            }
        }
    }

    protected void validateSession()
        throws SQLException
    {
        // REVIEW: hersker: 5/23/2007: if the session is closed, the
        // "session" var is null, and the session.wasKilled(), below,
        // will throw an NPE. Throwing "session closed" seems better.
        if (isClosed()) {
            throw FarragoJdbcEngineDriver.newSqlException(
                FarragoResource.instance().JdbcConnSessionClosed.ex());
        }

        // REVIEW: SWZ: 4/19/2006: Some DDL can cause a shutdown.  In that
        // event, the session is closed.  FarragoTestCase doesn't handle
        // this and attempts to use methods that call this validation method.
        // Therefore, we only check for killed, not closed, sessions.  This
        // may need to be modified if there are reasons for a session to be
        // closed out from under a connection other than killing.
        if (session.wasKilled()) {
            throw FarragoJdbcEngineDriver.newSqlException(
                FarragoResource.instance().JdbcConnSessionKilled.ex());
        }
    }

    public String findMofId(String wrapperName)
        throws SQLException
    {
        validateSession();

        FarragoDbSession session = (FarragoDbSession) getSession();
        SqlIdentifier wrapperSqlIdent =
            new SqlIdentifier(wrapperName, SqlParserPos.ZERO);

        FemDataWrapper wrapper =
            FarragoCatalogUtil.getModelElementByName(
                session.getRepos().allOfType(FemDataWrapper.class),
                wrapperSqlIdent.getSimple());

        if (wrapper != null) {
            if (!wrapper.isForeign()) {
                wrapper = null;
            }
        }

        if (wrapper != null) {
            return wrapper.refMofId();
        } else {
            return null;
        }
    }

    public FarragoMedDataWrapperInfo getWrapper(
        String mofId,
        String libraryName,
        Properties options)
        throws SQLException
    {
        validateSession();

        return new FleetingMedDataWrapperInfo(mofId, libraryName, options);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Implementation of {@link FarragoMedDataWrapperInfo} which fleetingly
     * grabs a {@link FarragoMedDataWrapper} at the start of a call and unpins
     * it before the end of the call.
     */
    private class FleetingMedDataWrapperInfo
        implements FarragoMedDataWrapperInfo
    {
        private final String mofId;
        private final String libraryName;
        private final Properties options;

        private FarragoDataWrapperCache dataWrapperCache;

        FleetingMedDataWrapperInfo(
            String mofId,
            String libraryName,
            Properties options)
        {
            this.mofId = mofId;
            this.libraryName = libraryName;
            this.options = (Properties) options.clone();
        }

        private FarragoMedDataWrapper getWrapper()
        {
            assert (dataWrapperCache == null);

            final FarragoDbSession session = (FarragoDbSession) getSession();
            final FarragoDatabase db = session.getDatabase();
            final FarragoObjectCache sharedCache = db.getDataWrapperCache();

            dataWrapperCache =
                new FarragoDataWrapperCache(
                    session,
                    sharedCache,
                    db.getPluginClassLoader(),
                    session.getRepos(),
                    db.getFennelDbHandle(),
                    null);

            final FarragoMedDataWrapper dataWrapper =
                dataWrapperCache.loadWrapper(mofId, libraryName, options);
            return dataWrapper;
        }

        private void closeWrapperCache()
        {
            dataWrapperCache.closeAllocation();
            dataWrapperCache = null;
        }

        public DriverPropertyInfo [] getPluginPropertyInfo(
            Locale locale,
            Properties wrapperProps)
        {
            FarragoMedDataWrapper dataWrapper = getWrapper();
            try {
                return dataWrapper.getPluginPropertyInfo(locale, wrapperProps);
            } finally {
                closeWrapperCache();
            }
        }

        public DriverPropertyInfo [] getServerPropertyInfo(
            Locale locale,
            Properties wrapperProps,
            Properties serverProps)
        {
            FarragoMedDataWrapper dataWrapper = getWrapper();
            try {
                return dataWrapper.getServerPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps);
            } finally {
                closeWrapperCache();
            }
        }

        public DriverPropertyInfo [] getColumnSetPropertyInfo(
            Locale locale,
            Properties wrapperProps,
            Properties serverProps,
            Properties tableProps)
        {
            FarragoMedDataWrapper dataWrapper = getWrapper();
            try {
                return dataWrapper.getColumnSetPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps,
                    tableProps);
            } finally {
                closeWrapperCache();
            }
        }

        public DriverPropertyInfo [] getColumnPropertyInfo(
            Locale locale,
            Properties wrapperProps,
            Properties serverProps,
            Properties tableProps,
            Properties columnProps)
        {
            FarragoMedDataWrapper dataWrapper = getWrapper();
            try {
                return dataWrapper.getColumnPropertyInfo(
                    locale,
                    wrapperProps,
                    serverProps,
                    tableProps,
                    columnProps);
            } finally {
                closeWrapperCache();
            }
        }

        public boolean isForeign()
        {
            FarragoMedDataWrapper dataWrapper = getWrapper();
            try {
                return dataWrapper.isForeign();
            } finally {
                closeWrapperCache();
            }
        }
    }
}

// End FarragoJdbcEngineConnection.java

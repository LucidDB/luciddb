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
import net.sf.farrago.ddl.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.db.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.util.*;
import net.sf.saffron.jdbc.*;
import net.sf.saffron.oj.stmt.*;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

// TODO:  i18n for SQLExceptions in this class

/**
 * FarragoJdbcConnection subclasses SaffronJdbcConnection to implement
 * Farrago-specific details.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoJdbcConnection
    extends SaffronJdbcConnection
    implements CloneableJdbcConnection, DdlConnection
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoJdbcConnection.class);

    public static final String MDR_USER_NAME = "MDR";

    //~ Instance fields -------------------------------------------------------

    /** Fennel transaction context for this connection */
    private final FennelTxnContext fennelTxnContext;

    /** Qualifiers to assume for unqualified object references */
    FarragoConnectionDefaults connectionDefaults;

    /** Database accessed by this connection */
    FarragoDatabase database;
    
    /** Catalog accessed by this connection */
    FarragoCatalog catalog;

    String url;

    /** Was this Connection produced by cloning? */
    boolean isClone;

    // FIXME:  If many stmts are executed against this connection, this
    // will become a leak.  Need to purge immediately when a stmt is closed.
    /** Allocations owned by this connection. */
    FarragoCompoundAllocation allocations;

    boolean isAutoCommit;

    /**
     * List of savepoints established within current transaction which have
     * not been released or rolled back; order is from earliest to latest.
     */
    List savepointList;

    /**
     * Generator for savepoint Id's.
     */
    int nextSavepointId;

    /**
     * Map of temporary indexes created by this connection's session.
     */
    SessionIndexMap sessionIndexMap;

    /**
     * Private cache of executable code pinned by the current txn.
     */
    Map txnCodeCache;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoJdbcConnection object.
     *
     * @param url URL used to connect
     * @param info properties for this connection
     */
    public FarragoJdbcConnection(String url,Properties info)
    {
        // TODO:  excn handling
        database = FarragoDatabase.connect(
            new FennelCmdExecutorImpl());
        
        connectionDefaults = new FarragoConnectionDefaults();

        connectionDefaults.sessionUserName = info.getProperty("user");
        connectionDefaults.currentUserName = connectionDefaults.sessionUserName;
        connectionDefaults.systemUserName = System.getProperty("user.name");

        // TODO:  authenticate sessionUserName with password

        if (MDR_USER_NAME.equals(connectionDefaults.sessionUserName)) {
            // This is a reentrant connection from MDR.
            catalog = database.getSystemCatalog();
        } else {
            // This is a normal connection.
            catalog = database.getUserCatalog();
        }

        fennelTxnContext = new FennelTxnContext(
            catalog,
            database.getFennelDbHandle());

        // TODO:  look up from user profile
        connectionDefaults.catalogName =
            catalog.getSelfAsCwmCatalog().getName();

        this.url = url;

        allocations = new FarragoCompoundAllocation();
        txnCodeCache = new HashMap();

        isAutoCommit = true;

        savepointList = new ArrayList();

        sessionIndexMap = new SessionIndexMap(allocations,database,catalog);
    }

    //~ Methods ---------------------------------------------------------------

    // implement CloneableJdbcConnection
    public DdlConnection cloneJdbcConnection()
    {
        // TODO:  keep track of clones and make sure they aren't left hanging
        // around by the time stmt finishes executing
        try {
            FarragoJdbcConnection clone = (FarragoJdbcConnection) super.clone();
            clone.isClone = true;
            clone.allocations = new FarragoCompoundAllocation();
            clone.savepointList = new ArrayList();
            clone.connectionDefaults = connectionDefaults.cloneDefaults();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw Util.newInternal(ex);
        }
    }
    
    // implement Connection
    public boolean isClosed() throws SQLException
    {
        return (database == null);
    }

    // implement Connection
    public void setAutoCommit(boolean autoCommit) throws SQLException
    {
        commitImpl();
        isAutoCommit = autoCommit;
    }

    // implement Connection
    public boolean getAutoCommit() throws SQLException
    {
        return isAutoCommit;
    }

    // implement Connection
    public String getCatalog() throws SQLException
    {
        return connectionDefaults.catalogName;
    }
    
    void endTransactionIfAuto(boolean commit)
        throws SQLException
    {
        if (isAutoCommit) {
            if (commit) {
                commitImpl();
            } else {
                rollbackImpl();
            }
        }
    }
    
    /**
     * .
     *
     * @return catalog accessed by this connection
     */
    public FarragoCatalog getFarragoCatalog()
    {
        return catalog;
    }

    /**
     * .
     *
     * @return database accessed by this connection
     */
    public FarragoDatabase getDatabase()
    {
        return database;
    }

    /**
     * .
     *
     * @return Fennel transaction context for this connection
     */
    public FennelTxnContext getFennelTxnContext()
    {
        return fennelTxnContext;
    }

    // implement Connection
    public void close() throws SQLException
    {
        allocations.closeAllocation();
        if (isClone) {
            return;
        }
        endTransactionIfAuto(true);
        if (fennelTxnContext.isTxnInProgress()) {
            // TODO:  generate SQLException in FarragoResource?
            throw new SQLException(
                FarragoResource.instance().getJdbcInvalidTxnState(),
                "25000");
        }
        super.close();
        try {
            FarragoDatabase.disconnect(database);
        } finally {
            database = null;
            catalog = null;
        }
    }

    public void commit() throws SQLException
    {
        if (isAutoCommit) {
            throw new SQLException(
                "Can't COMMIT in auto-commit mode");
        }
        commitImpl();
    }
    
    // TODO:  excn conversion
    void commitImpl() throws SQLException
    {
        tracer.info("commit");
        fennelTxnContext.commit();
        onEndOfTransaction();
        sessionIndexMap.onCommit();
    }

    // implement Connection
    public Statement createStatement() throws SQLException
    {
        FarragoJdbcStatement stmt = new FarragoJdbcStatement(this);
        allocations.addAllocation(stmt);
        return stmt;
    }

    // implement Connection
    public void rollback() throws SQLException
    {
        if (isAutoCommit) {
            throw new SQLException(
                "Can't ROLLBACK in auto-commit mode");
        }
        rollbackImpl();
    }

    void rollbackImpl() throws SQLException
    {
        tracer.info("rollback");
        fennelTxnContext.rollback();
        onEndOfTransaction();
    }

    private void onEndOfTransaction()
    {
        savepointList.clear();
        Iterator iter = txnCodeCache.values().iterator();
        while (iter.hasNext()) {
            FarragoAllocation alloc = (FarragoAllocation) iter.next();
            alloc.closeAllocation();
        }
        txnCodeCache.clear();
    }

    // implement Connection
    public void rollback(Savepoint savepoint) throws SQLException
    {
        int iSavepoint = validateSavepoint(savepoint);
        rollbackToSavepoint(iSavepoint);
    }

    // implement Connection
    public void setTransactionIsolation(int level) throws SQLException
    {
        // TODO:  implement this; dummied out for now to shut sqlline up
    }

    public int getTransactionIsolation() throws SQLException
    {
        // TODO:  implement this; dummied out for now to shut sqlline up
        return TRANSACTION_READ_UNCOMMITTED;
    }

    // implement Connection
    public Savepoint setSavepoint() throws SQLException
    {
        return setSavepointImpl(null);
    }

    // implement Connection
    public Savepoint setSavepoint(String name) throws SQLException
    {
        if (findSavepointByName(name,false) != -1) {
            throw new SQLException("Savepoint name already in use:  " + name);
        }
        return setSavepointImpl(name);
    }

    private Savepoint setSavepointImpl(String name) throws SQLException
    {
        if (isAutoCommit) {
            throw new SQLException(
                "Can't create SAVEPOINT in auto-commit mode");
        }
        FemSvptHandle femSvptHandle;
        if (catalog.isFennelEnabled()) {
            femSvptHandle = fennelTxnContext.newSavepoint();
        } else {
            femSvptHandle = null;
        }
        Savepoint newSavepoint = new FarragoJdbcSavepoint(
            nextSavepointId++,name,femSvptHandle,this);
        savepointList.add(newSavepoint);
        return newSavepoint;
    }

    private int validateSavepoint(Savepoint savepoint) throws SQLException
    {
        if (!(savepoint instanceof FarragoJdbcSavepoint)) {
            throw new SQLException("Savepoint class not recognized");
        }
        FarragoJdbcSavepoint farragoSavepoint =
            (FarragoJdbcSavepoint) savepoint;
        if (farragoSavepoint.connection != this) {
            throw new SQLException("Savepoint from wrong Connection");
        }
        int iSavepoint = findSavepoint(savepoint);
        if (iSavepoint == -1) {
            if (farragoSavepoint.name == null) {
                throw new SQLException(
                    "Invalid Savepoint ID " + farragoSavepoint.id);
            } else {
                throw new SQLException(
                    "Invalid Savepoint " + farragoSavepoint.name);
            }
        }
        return iSavepoint;
    }

    // implement Connection
    public void releaseSavepoint(Savepoint savepoint) throws SQLException
    {
        int iSavepoint = validateSavepoint(savepoint);
        releaseSavepoint(iSavepoint);
    }

    private int findSavepointByName(
        String name,boolean throwIfNotFound) throws SQLException
    {
        for (int i = 0; i < savepointList.size(); ++i) {
            FarragoJdbcSavepoint savepoint =
                (FarragoJdbcSavepoint) savepointList.get(i);
            if (name.equals(savepoint.name)) {
                return i;
            }
        }
        if (throwIfNotFound) {
            throw new SQLException(
                "Unknown Savepoint " + name);
        }
        return -1;
    }

    private int findSavepoint(Savepoint savepoint) throws SQLException
    {
        return savepointList.indexOf(savepoint);
    }

    private void releaseSavepoint(int iSavepoint) throws SQLException
    {
        // TODO:  need Fennel support
        assert(false);
    }

    private void rollbackToSavepoint(int iSavepoint) throws SQLException
    {
        if (isAutoCommit) {
            throw new SQLException(
                "Can't ROLLBACK TO SAVEPOINT in auto-commit mode");
        }
        FarragoJdbcSavepoint savepoint =
            (FarragoJdbcSavepoint) savepointList.get(iSavepoint);
        if (catalog.isFennelEnabled()) {
            fennelTxnContext.rollbackToSavepoint(savepoint.femSvptHandle);
        }

        // TODO:  list truncation util
        while (savepointList.size() > (iSavepoint + 1)) {
            savepointList.remove(savepointList.size() - 1);
        }
    }

    // implement SaffronJdbcConnection
    protected String getUrlPrefix()
    {
        return FarragoJdbcDriver.getUrlPrefixStatic();
    }

    // implement Connection
    public DatabaseMetaData getMetaData() throws SQLException
    {
        return new FarragoDatabaseMetaData(this);
    }

    /**
     * Internally prepare an SQL statement.
     *
     * @param sql the SQL statement string
     *
     * @param owner owner to use if a stmt is returned (can be null only if sql
     * is already known to be DDL)
     *
     * @param isExecDirect whether statement is being prepared as part of
     * direct execution; if so, and the SQL turns out to be DDL, execute
     * it immediately
     *
     * @param viewInfo receives view query info if non-null
     *
     * @return if non-DDL, the prepared FarragoExecutableStmt; null for DDL
     * (regardless of whether it was executed)
     */
    FarragoExecutableStmt prepareImpl(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoViewInfo viewInfo)
        throws SQLException
    {
        tracer.info(sql);
        FarragoParser parser = new FarragoParser(catalog,sql);
        boolean rollback = true;
        DdlValidator ddlValidator = null;
        try {

            // REVIEW: For !isExecDirect, maybe should disable all DDL
            // validation: just parse, because the catalog may change by the
            // time the statement is executed.  Also probably need to disallow
            // some types of prepared DDL.
            
            ddlValidator = new DdlValidator(
                this,
                catalog,
                database.getFennelDbHandle(),
                parser,
                connectionDefaults,
                sessionIndexMap,
                database.getDataWrapperCache());
            parser.ddlValidator = ddlValidator;
            Object parsedObj = parser.parseSqlStatement();
            if (parsedObj instanceof SqlNode) {
                SqlNode sqlNode = (SqlNode) parsedObj;
                rollback = false;
                ddlValidator.closeAllocation();
                ddlValidator = null;
                FarragoExecutableStmt stmt =
                    database.prepareStmt(
                        this,
                        catalog,
                        sqlNode,
                        owner,
                        connectionDefaults,
                        sessionIndexMap,
                        viewInfo);
                if (isExecDirect
                    && (stmt.getDynamicParamRowType().getFieldCount() > 0))
                {
                    owner.closeAllocation();
                    throw new SQLException(
                        "Can't EXECUTE IMMEDIATE a statement with dynamic "
                        + "parameters:  " + sql);
                }
                return stmt;
            }
            if (!isExecDirect) {
                return null;
            }

            executeDdl(
                ddlValidator,
                (DdlStmt) parsedObj);
            
            rollback = false;
        } catch (FarragoUnvalidatedDependencyException ex) {
            // pass this one on through
            throw ex;
        } catch (Throwable ex) {
            throw FarragoJdbcDriver.newSqlException(ex);
        } finally {
            if (ddlValidator != null) {
                ddlValidator.closeAllocation();
            }
            if (rollback) {
                tracer.fine("rolling back DDL");
                parser.rollbackReposTxn();
            }
        }
        return null;
    }

    private void executeDdl(
        DdlValidator ddlValidator,DdlStmt ddlStmt)
        throws SQLException
    {
        if (ddlStmt.requiresCommit()) {
            // For now, DDL causes implicit commit of any pending txn.
            // TODO:  commit at end of DDL too in case it updated something?
            commitImpl();
        }
        
        tracer.fine("validating DDL");
        ddlValidator.validate(ddlStmt);
        tracer.fine("updating storage");
        ddlValidator.executeStorage();

        // TODO: Some statements aren't real DDL, and should be traced
        // differently.  

        // handle some special cases here
        ddlStmt.visit(new DdlExecutionVisitor());

        tracer.fine("committing DDL");
        ddlValidator.getParser().commitReposTxn();
    }

    private class DdlExecutionVisitor extends DdlVisitor
    {
        // implement DdlVisitor
        public void visit(DdlCommitStmt stmt)
            throws SQLException
        {
            commit();
        }

        // implement DdlVisitor
        public void visit(DdlRollbackStmt rollbackStmt)
            throws SQLException
        {
            if (rollbackStmt.getSavepointName() == null) {
                rollback();
            } else {
                int iSavepoint =
                    findSavepointByName(rollbackStmt.getSavepointName(),true);
                rollbackToSavepoint(iSavepoint);
            }
        }

        // implement DdlVisitor
        public void visit(DdlSavepointStmt savepointStmt)
            throws SQLException
        {
            setSavepoint(savepointStmt.getSavepointName());
        }

        // implement DdlVisitor
        public void visit(DdlReleaseSavepointStmt releaseStmt)
            throws SQLException
        {
            int iSavepoint =
                findSavepointByName(releaseStmt.getSavepointName(),true);
            releaseSavepoint(iSavepoint);
        }

        // implement DdlVisitor
        public void visit(DdlSetQualifierStmt stmt)
            throws SQLException
        {
            CwmModelElement qualifier = stmt.getModelElement();
            if (qualifier instanceof CwmCatalog) {
                connectionDefaults.catalogName = qualifier.getName();
            } else {
                assert(qualifier instanceof CwmSchema);
                connectionDefaults.schemaName = qualifier.getName();
                connectionDefaults.catalogName =
                    qualifier.getNamespace().getName();
                connectionDefaults.schemaCatalogName =
                    connectionDefaults.catalogName;
            }
        }
    
        // implement DdlVisitor
        public void visit(DdlSetSystemParamStmt stmt)
            throws SQLException
        {
            database.updateSystemParameter(stmt);
        }

        // implement DdlVisitor
        public void visit(DdlCheckpointStmt stmt)
            throws SQLException
        {
            database.requestCheckpoint(false,false);
        }
    }

    // implement Connection
    public PreparedStatement prepareStatement(String sql)
        throws SQLException
    {
        FarragoCompoundAllocation stmtAllocations =
            new FarragoCompoundAllocation();
        FarragoExecutableStmt stmt = prepareImpl(
            sql,
            stmtAllocations,
            false,
            null);
        FarragoJdbcPreparedStatement preparedStmt;
        if (stmt == null) {
            stmtAllocations.closeAllocation();
            preparedStmt = new FarragoJdbcPreparedDdl(this,sql);
        } else {
            preparedStmt = new FarragoJdbcPreparedNonDdl(
                this,
                sql,
                stmt,
                stmtAllocations);
        }
        allocations.addAllocation(preparedStmt);
        return preparedStmt;
    }

    // implement DdlConnection
    public FarragoViewInfo prepareViewQuery(
        String sql) throws SQLException
    {
        FarragoViewInfo info = new FarragoViewInfo();
        FarragoExecutableStmt stmt = prepareImpl(
            sql,
            null,
            false,
            info);
        assert(stmt == null);
        return info;
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int resultSetType,
        int resultSetConcurrency,
        int resultSetHoldability) throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        int autoGeneratedKeys) throws SQLException
    {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new UnsupportedOperationException();
        }
        return prepareStatement(sql);
    }

    // implement Connection
    public PreparedStatement prepareStatement(String sql,int [] columnIndexes)
        throws SQLException
    {
        throw new UnsupportedOperationException();
    }

    // implement Connection
    public PreparedStatement prepareStatement(
        String sql,
        String [] columnNames) throws SQLException
    {
        throw new UnsupportedOperationException();
    }
}

// End FarragoJdbcConnection.java

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
import net.sf.farrago.ddl.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.util.*;
import net.sf.farrago.session.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.util.*;
import net.sf.saffron.oj.stmt.*;

import java.util.*;
import java.util.logging.*;
import java.sql.DatabaseMetaData;

/**
 * FarragoDbSession implements the {@link
 * net.sf.farrago.session.FarragoSession} interface as a connection to a {@link
 * FarragoDatabase} instance.  It manages private authorization and transaction
 * context.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDbSession
    extends FarragoCompoundAllocation
    implements FarragoSession, Cloneable                            
{
    //~ Static fields/initializers --------------------------------------------

    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoDbSession.class);

    public static final String MDR_USER_NAME = "MDR";

    //~ Instance fields -------------------------------------------------------

    /** Fennel transaction context for this session */
    private final FennelTxnContext fennelTxnContext;

    /** Qualifiers to assume for unqualified object references */
    private FarragoConnectionDefaults connectionDefaults;

    /** Database accessed by this session */
    private FarragoDatabase database;
    
    /** Catalog accessed by this session */
    private FarragoCatalog catalog;

    private String url;

    /** Was this session produced by cloning? */
    private boolean isClone;

    private boolean isAutoCommit;

    /**
     * List of savepoints established within current transaction which have
     * not been released or rolled back; order is from earliest to latest.
     */
    private List savepointList;

    /**
     * Generator for savepoint Id's.
     */
    private int nextSavepointId;

    /**
     * Map of temporary indexes created by this session.
     */
    private SessionIndexMap sessionIndexMap;

    /**
     * Private cache of executable code pinned by the current txn.
     */
    private Map txnCodeCache;

    private DatabaseMetaData dbMetaData;

    private FarragoSessionFactory sessionFactory;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoDbSession object.
     *
     * @param url URL used to connect (same as JDBC)
     *
     * @param info properties for this session
     *
     * @param sessionFactory factory which created this session
     */
    public FarragoDbSession(
        String url,
        Properties info,
        FarragoSessionFactory sessionFactory)
    {
        this.sessionFactory = sessionFactory;
        
        // TODO:  excn handling
        database = FarragoDatabase.pinReference(
            sessionFactory.newFennelCmdExecutor());

        FarragoDatabase.addSession(database,this);
        
        connectionDefaults = new FarragoConnectionDefaults();

        connectionDefaults.sessionUserName = info.getProperty("user");
        connectionDefaults.currentUserName = connectionDefaults.sessionUserName;
        connectionDefaults.systemUserName = System.getProperty("user.name");

        // TODO:  authenticate sessionUserName with password

        if (MDR_USER_NAME.equals(connectionDefaults.sessionUserName)) {
            // This is a reentrant session from MDR.
            catalog = database.getSystemCatalog();
        } else {
            // This is a normal session.
            catalog = database.getUserCatalog();
        }

        fennelTxnContext = new FennelTxnContext(
            catalog,
            database.getFennelDbHandle());

        // TODO:  look up from user profile
        connectionDefaults.catalogName =
            catalog.getSelfAsCwmCatalog().getName();

        this.url = url;

        txnCodeCache = new HashMap();

        isAutoCommit = true;

        savepointList = new ArrayList();

        sessionIndexMap = new SessionIndexMap(this,database,catalog);
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSession
    public void setDatabaseMetaData(DatabaseMetaData dbMetaData)
    {
        this.dbMetaData = dbMetaData;
    }

    // implement FarragoSession
    public DatabaseMetaData getDatabaseMetaData()
    {
        return dbMetaData;
    }
    
    // implement FarragoSession
    public String getUrl()
    {
        return url;
    }
    
    // implement FarragoSession
    public FarragoSessionStmtContext newStmtContext()
    {
        FarragoDbStmtContext stmtContext = new FarragoDbStmtContext(this);
        addAllocation(stmtContext);
        return stmtContext;
    }

    // implement FarragoSession
    public FarragoSessionParser newParser()
    {
        return new FarragoParser();
    }
    
    // implement FarragoSession
    public FarragoSessionDdlValidator newDdlValidator()
    {
        return new DdlValidator(
            this,
            getCatalog(),
            getDatabase().getFennelDbHandle(),
            newParser(),
            getConnectionDefaults(),
            getSessionIndexMap(),
            getDatabase().getDataWrapperCache());
    }
    
    // implement FarragoSession
    public FarragoSession cloneSession()
    {
        // TODO:  keep track of clones and make sure they aren't left hanging
        // around by the time stmt finishes executing
        try {
            FarragoDbSession clone = (FarragoDbSession) super.clone();
            clone.isClone = true;
            clone.allocations = new LinkedList();
            clone.savepointList = new ArrayList();
            clone.connectionDefaults = connectionDefaults.cloneDefaults();
            return clone;
        } catch (CloneNotSupportedException ex) {
            throw Util.newInternal(ex);
        }
    }

    // implement FarragoSession
    public boolean isClone()
    {
        return isClone;
    }
    
    // implement FarragoSession
    public boolean isClosed()
    {
        return (database == null);
    }

    // implement FarragoSession
    public boolean isTxnInProgress()
    {
        return fennelTxnContext.isTxnInProgress();
    }

    // implement FarragoSession
    public void setAutoCommit(boolean autoCommit)
    {
        commitImpl();
        isAutoCommit = autoCommit;
    }

    // implement FarragoSession
    public boolean isAutoCommit()
    {
        return isAutoCommit;
    }

    // implement FarragoSession
    public FarragoConnectionDefaults getConnectionDefaults()
    {
        return connectionDefaults;
    }
    
    void endTransactionIfAuto(boolean commit)
    {
        if (isAutoCommit) {
            if (commit) {
                commitImpl();
            } else {
                rollbackImpl();
            }
        }
    }
    
    // implement FarragoSession
    public FarragoCatalog getCatalog()
    {
        return catalog;
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        super.closeAllocation();
        if (isClone) {
            return;
        }
        endTransactionIfAuto(true);
        try {
            FarragoDatabase.disconnectSession(this);
        } finally {
            database = null;
            catalog = null;
        }
    }

    // implement FarragoSession
    public void commit()
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().newSessionNoCommitInAutocommit();
        }
        commitImpl();
    }
    
    // implement FarragoSession
    public FarragoSessionSavepoint newSavepoint(String name)
    {
        if (name != null) {
            if (findSavepointByName(name,false) != -1) {
                throw
                    FarragoResource.instance().newSessionDupSavepointName(name);
            }
        }
        return newSavepointImpl(name);
    }

    // implement FarragoSession
    public void releaseSavepoint(FarragoSessionSavepoint savepoint)
    {
        int iSavepoint = validateSavepoint(savepoint);
        releaseSavepoint(iSavepoint);
    }

    // implement FarragoSession
    public FarragoSessionViewInfo analyzeViewQuery(String sql)
    {
        FarragoSessionViewInfo info = new FarragoSessionViewInfo();
        FarragoExecutableStmt stmt = prepare(
            sql,
            null,
            false,
            info);
        assert(stmt == null);
        return info;
    }

    SessionIndexMap getSessionIndexMap()
    {
        return sessionIndexMap;
    }

    Map getTxnCodeCache()
    {
        return txnCodeCache;
    }

    FarragoDatabase getDatabase()
    {
        return database;
    }

    FennelTxnContext getFennelTxnContext()
    {
        return fennelTxnContext;
    }

    void commitImpl()
    {
        tracer.info("commit");
        fennelTxnContext.commit();
        onEndOfTransaction();
        sessionIndexMap.onCommit();
    }

    void rollbackImpl()
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

    // implement FarragoSession
    public void rollback(FarragoSessionSavepoint savepoint)
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().newSessionNoRollbackInAutocommit();
        }
        if (savepoint == null) {
            rollbackImpl();
        } else {
            int iSavepoint = validateSavepoint(savepoint);
            rollbackToSavepoint(iSavepoint);
        }
    }

    private FarragoSessionSavepoint newSavepointImpl(String name)
    {
        if (isAutoCommit) {
            throw
                FarragoResource.instance().newSessionNoSavepointInAutocommit();
        }
        FemSvptHandle femSvptHandle;
        if (catalog.isFennelEnabled()) {
            femSvptHandle = fennelTxnContext.newSavepoint();
        } else {
            femSvptHandle = null;
        }
        FarragoDbSavepoint newSavepoint = new FarragoDbSavepoint(
            nextSavepointId++,name,femSvptHandle,this);
        savepointList.add(newSavepoint);
        return newSavepoint;
    }

    private int validateSavepoint(FarragoSessionSavepoint savepoint)
    {
        if (!(savepoint instanceof FarragoDbSavepoint)) {
            throw FarragoResource.instance().newSessionWrongSavepoint(
                savepoint.getName());
        }
        FarragoDbSavepoint dbSavepoint = (FarragoDbSavepoint) savepoint;
        if (dbSavepoint.session != this) {
            throw FarragoResource.instance().newSessionWrongSavepoint(
                savepoint.getName());
        }
        int iSavepoint = findSavepoint(savepoint);
        if (iSavepoint == -1) {
            if (savepoint.getName() == null) {
                throw FarragoResource.instance().newSessionInvalidSavepointId(
                    new Integer(savepoint.getId()));
            } else {
                throw FarragoResource.instance().newSessionInvalidSavepointName(
                    savepoint.getName());
            }
        }
        return iSavepoint;
    }

    private int findSavepointByName(
        String name,boolean throwIfNotFound)
    {
        for (int i = 0; i < savepointList.size(); ++i) {
            FarragoDbSavepoint savepoint =
                (FarragoDbSavepoint) savepointList.get(i);
            if (name.equals(savepoint.getName())) {
                return i;
            }
        }
        if (throwIfNotFound) {
            throw FarragoResource.instance().newSessionInvalidSavepointName(
                name);
        }
        return -1;
    }

    private int findSavepoint(FarragoSessionSavepoint savepoint)
    {
        return savepointList.indexOf(savepoint);
    }

    private void releaseSavepoint(int iSavepoint)
    {
        // TODO:  need Fennel support
        assert(false);
    }

    private void rollbackToSavepoint(int iSavepoint)
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().newSessionNoRollbackInAutocommit();
        }
        FarragoDbSavepoint savepoint =
            (FarragoDbSavepoint) savepointList.get(iSavepoint);
        if (catalog.isFennelEnabled()) {
            fennelTxnContext.rollbackToSavepoint(
                savepoint.getFemSvptHandle());
        }

        // TODO:  list truncation util
        while (savepointList.size() > (iSavepoint + 1)) {
            savepointList.remove(savepointList.size() - 1);
        }
    }

    FarragoExecutableStmt prepare(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionViewInfo viewInfo)
    {
        tracer.info(sql);
        FarragoReposTxnContext reposTxnContext =
            new FarragoReposTxnContext(catalog);
        boolean rollback = true;
        FarragoSessionDdlValidator ddlValidator = null;
        try {

            // REVIEW: For !isExecDirect, maybe should disable all DDL
            // validation: just parse, because the catalog may change by the
            // time the statement is executed.  Also probably need to disallow
            // some types of prepared DDL.
            
            ddlValidator = newDdlValidator();
            FarragoSessionParser parser = ddlValidator.getParser();
            Object parsedObj = parser.parseSqlStatement(
                ddlValidator,
                reposTxnContext,
                sql);
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
                    throw FarragoResource.instance().
                        newSessionNoExecuteImmediateParameters(sql);
                }
                return stmt;
            }
            if (!isExecDirect) {
                return null;
            }

            executeDdl(
                ddlValidator,
                reposTxnContext,
                (FarragoSessionDdlStmt) parsedObj);
            
            rollback = false;
        } finally {
            if (ddlValidator != null) {
                ddlValidator.closeAllocation();
            }
            if (rollback) {
                tracer.fine("rolling back DDL");
                reposTxnContext.rollback();
            }
        }
        return null;
    }

    private void executeDdl(
        FarragoSessionDdlValidator ddlValidator,
        FarragoReposTxnContext reposTxnContext,
        FarragoSessionDdlStmt ddlStmt)
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
        if (ddlStmt instanceof DdlStmt) {
            // REVIEW jvs 22-Mar-2004:  can we make this truly extensible?
            ((DdlStmt) ddlStmt).visit(new DdlExecutionVisitor());
        }

        tracer.fine("committing DDL");
        reposTxnContext.commit();
    }

    private class DdlExecutionVisitor extends DdlVisitor
    {
        // implement DdlVisitor
        public void visit(DdlCommitStmt stmt)
        {
            commit();
        }

        // implement DdlVisitor
        public void visit(DdlRollbackStmt rollbackStmt)
        {
            if (rollbackStmt.getSavepointName() == null) {
                rollback(null);
            } else {
                int iSavepoint =
                    findSavepointByName(rollbackStmt.getSavepointName(),true);
                rollbackToSavepoint(iSavepoint);
            }
        }

        // implement DdlVisitor
        public void visit(DdlSavepointStmt savepointStmt)
        {
            newSavepoint(savepointStmt.getSavepointName());
        }

        // implement DdlVisitor
        public void visit(DdlReleaseSavepointStmt releaseStmt)
        {
            int iSavepoint =
                findSavepointByName(releaseStmt.getSavepointName(),true);
            releaseSavepoint(iSavepoint);
        }

        // implement DdlVisitor
        public void visit(DdlSetQualifierStmt stmt)
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
        {
            database.updateSystemParameter(stmt);
        }

        // implement DdlVisitor
        public void visit(DdlCheckpointStmt stmt)
        {
            database.requestCheckpoint(false,false);
        }
    }
}

// End FarragoDbSession.java

/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
import net.sf.farrago.trace.*;
import net.sf.farrago.session.*;
import net.sf.farrago.runtime.*;

import org.eigenbase.sql.*;
import org.eigenbase.util.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.oj.rex.*;

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

    private static final Logger tracer =
        FarragoTrace.getDatabaseSessionTracer();

    public static final String MDR_USER_NAME = "MDR";

    //~ Instance fields -------------------------------------------------------

    /** Fennel transaction context for this session */
    private final FennelTxnContext fennelTxnContext;

    /** Qualifiers to assume for unqualified object references */
    private FarragoSessionVariables sessionVariables;

    /** Database accessed by this session */
    private FarragoDatabase database;

    /** Repos accessed by this session */
    private FarragoRepos repos;

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
        database = FarragoDatabase.pinReference(sessionFactory);

        FarragoDatabase.addSession(database,this);

        sessionVariables = new FarragoSessionVariables();

        sessionVariables.sessionUserName = info.getProperty("user");
        sessionVariables.currentUserName = sessionVariables.sessionUserName;
        sessionVariables.systemUserName = System.getProperty("user.name");

        // TODO:  authenticate sessionUserName with password

        if (MDR_USER_NAME.equals(sessionVariables.sessionUserName)) {
            // This is a reentrant session from MDR.
            repos = database.getSystemRepos();
        } else {
            // This is a normal session.
            repos = database.getUserRepos();
        }

        fennelTxnContext = sessionFactory.newFennelTxnContext(
            repos,
            database.getFennelDbHandle());

        // TODO:  look up from user profile
        sessionVariables.catalogName =
            repos.getSelfAsCwmCatalog().getName();

        this.url = url;

        txnCodeCache = new HashMap();

        isAutoCommit = true;

        savepointList = new ArrayList();

        sessionIndexMap = new SessionIndexMap(this,database,repos);
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
    public SqlOperatorTable getSqlOperatorTable()
    {
        return SqlOperatorTable.instance();
    }

    // implement FarragoSession
    public OJRexImplementorTable getOJRexImplementorTable()
    {
        return database.getOJRexImplementorTable();
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
    public FarragoSessionStmtValidator newStmtValidator()
    {
        return new FarragoStmtValidator(
            getRepos(),
            getDatabase().getFennelDbHandle(),
            this,
            getDatabase().getCodeCache(),
            getDatabase().getDataWrapperCache(),
            getSessionIndexMap());
    }
    
    // implement FarragoSession
    public FarragoSessionDdlValidator newDdlValidator(
        FarragoSessionStmtValidator stmtValidator)
    {
        return new DdlValidator(stmtValidator);
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
            clone.sessionVariables = sessionVariables.cloneVariables();
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
    public FarragoSessionVariables getSessionVariables()
    {
        return sessionVariables;
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
    public FarragoRepos getRepos()
    {
        return repos;
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
            repos = null;
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
        FarragoSessionExecutableStmt stmt = prepare(
            sql,
            null,
            false,
            info);
        assert(stmt == null);
        return info;
    }

    public FarragoDatabase getDatabase()
    {
        return database;
    }

    public SessionIndexMap getSessionIndexMap()
    {
        return sessionIndexMap;
    }

    Map getTxnCodeCache()
    {
        return txnCodeCache;
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

    // implement FarragoSession
    public Class getRuntimeContextClass()
    {
        return FarragoRuntimeContext.class;
    }

    // implement FarragoSession
    public FarragoSessionRuntimeContext newRuntimeContext(
        FarragoSessionRuntimeParams params)
    {
        return new FarragoRuntimeContext(params);
    }

    protected FarragoSessionRuntimeParams newRuntimeContextParams()
    {
        FarragoSessionRuntimeParams params = new FarragoSessionRuntimeParams();
        params.repos = getRepos();
        params.codeCache = getDatabase().getCodeCache();
        params.txnCodeCache = getTxnCodeCache();
        params.fennelTxnContext = getFennelTxnContext();
        params.indexMap = getSessionIndexMap();
        params.sessionVariables = getSessionVariables().cloneVariables();
        params.sharedDataWrapperCache = getDatabase().getDataWrapperCache();
        return params;
    }

    private FarragoSessionSavepoint newSavepointImpl(String name)
    {
        if (isAutoCommit) {
            throw
                FarragoResource.instance().newSessionNoSavepointInAutocommit();
        }
        FennelSvptHandle fennelSvptHandle;
        if (repos.isFennelEnabled()) {
            fennelSvptHandle = fennelTxnContext.newSavepoint();
        } else {
            fennelSvptHandle = null;
        }
        FarragoDbSavepoint newSavepoint = new FarragoDbSavepoint(
            nextSavepointId++,name,fennelSvptHandle,this);
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
        throw Util.needToImplement(null);
    }

    private void rollbackToSavepoint(int iSavepoint)
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().newSessionNoRollbackInAutocommit();
        }
        FarragoDbSavepoint savepoint =
            (FarragoDbSavepoint) savepointList.get(iSavepoint);
        if (repos.isFennelEnabled()) {
            fennelTxnContext.rollbackToSavepoint(
                savepoint.getFennelSvptHandle());
        }

        // TODO:  list truncation util
        while (savepointList.size() > (iSavepoint + 1)) {
            savepointList.remove(savepointList.size() - 1);
        }
    }


    // implement FarragoSession
    public FarragoSessionPreparingStmt newPreparingStmt(
        FarragoSessionStmtValidator stmtValidator)
    {
        return new FarragoPreparingStmt(stmtValidator);
    }

    FarragoSessionExecutableStmt prepare(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionViewInfo viewInfo)
    {
        tracer.info(sql);

        // TODO jvs 11-Aug-2004:  Get rid of this big mutex.  It needs to stay
        // until we have proper object-level DDL-locking.  For now the
        // contention is the same as that due to the TODO below since the
        // MDR write lock is exclusive.
        synchronized(database.DDL_LOCK) {
            FarragoReposTxnContext reposTxnContext =
                new FarragoReposTxnContext(repos);

            // TODO jvs 21-June-2004: It would be preferable to start with a
            // read lock and only upgrade to write once we know we're dealing
            // with DDL.  However, at the moment that doesn't work because a
            // write txn is required for creating transient objects.  And MDR
            // doesn't support upgrade.  It might be possible to reorder
            // catalog access to solve this.
            reposTxnContext.beginWriteTxn();

            boolean [] pRollback = new boolean[1];
            pRollback[0] = true;
            FarragoSessionStmtValidator stmtValidator = newStmtValidator();
            FarragoSessionExecutableStmt stmt = null;
            try {
                stmt = prepareImpl(
                    sql,owner,isExecDirect,viewInfo,stmtValidator,
                    reposTxnContext,pRollback);
            } finally {
                if (stmtValidator != null) {
                    stmtValidator.closeAllocation();
                }
                if (pRollback[0]) {
                    tracer.fine("rolling back DDL");
                    reposTxnContext.rollback();
                } else {
                    reposTxnContext.commit();
                }
            }
            return stmt;
        }
    }

    private FarragoSessionExecutableStmt prepareImpl(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionViewInfo viewInfo,
        FarragoSessionStmtValidator stmtValidator,
        FarragoReposTxnContext reposTxnContext,
        boolean [] pRollback)
    {
        // REVIEW: For !isExecDirect, maybe should disable all DDL
        // validation: just parse, because the catalog may change by the
        // time the statement is executed.  Also probably need to disallow
        // some types of prepared DDL.

        FarragoSessionDdlValidator ddlValidator =
            newDdlValidator(stmtValidator);
        FarragoSessionParser parser = stmtValidator.getParser();
        Object parsedObj = parser.parseSqlStatement(
            ddlValidator,
            sql);
        if (parsedObj instanceof SqlNode) {
            SqlNode sqlNode = (SqlNode) parsedObj;
            pRollback[0] = false;
            ddlValidator.closeAllocation();
            ddlValidator = null;
            validate(stmtValidator,sqlNode);
            FarragoSessionExecutableStmt stmt =
                database.prepareStmt(
                    stmtValidator,
                    sqlNode,
                    owner,
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

        pRollback[0] = false;
        return null;
    }

    /**
     * Does some custom sql validations which can't be performed by
     * the vanilla validator.
     */
    public void validate(
        FarragoSessionStmtValidator stmtValidator,
        SqlNode sqlNode)
    {
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
                sessionVariables.catalogName = qualifier.getName();
            } else {
                assert(qualifier instanceof CwmSchema);
                sessionVariables.schemaName = qualifier.getName();
                sessionVariables.catalogName =
                    qualifier.getNamespace().getName();
                sessionVariables.schemaCatalogName =
                    sessionVariables.catalogName;
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

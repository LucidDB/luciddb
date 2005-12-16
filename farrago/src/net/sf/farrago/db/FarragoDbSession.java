/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.sql.*;
import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.parser.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.plugin.*;

import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.oj.rex.*;
import org.eigenbase.oj.stmt.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;
import org.eigenbase.jmi.*;


/**
 * FarragoDbSession implements the {@link
 * net.sf.farrago.session.FarragoSession} interface as a connection to a {@link
 * FarragoDatabase} instance.  It manages private authorization and transaction
 * context.
 *
 * @author John V. Sichi
 * @version $Id: //open/dev/farrago/src/net/sf/farrago/db/FarragoDbSession.java#27
 */
public class FarragoDbSession extends FarragoCompoundAllocation
    implements FarragoSession,
        Cloneable
{
    //~ Static fields/initializers --------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getDatabaseSessionTracer();
    public static final String MDR_USER_NAME = "MDR";

    //~ Instance fields -------------------------------------------------------

    /** Default personality for this session. */
    private FarragoSessionPersonality defaultPersonality;

    /** Current personality for this session. */
    private FarragoSessionPersonality personality;
    
    /** Fennel transaction context for this session */
    private FennelTxnContext fennelTxnContext;

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
    private boolean shutDownRequested;
    private boolean catalogDumpRequested;

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
    private FarragoDbSessionIndexMap sessionIndexMap;

    /**
     * The connection source for this session.
     */
    private FarragoSessionConnectionSource connectionSource;

    /**
     * Private cache of executable code pinned by the current txn.
     */
    private Map txnCodeCache;
    private DatabaseMetaData dbMetaData;
    protected FarragoSessionFactory sessionFactory;
    
    private FarragoSessionPrivilegeMap privilegeMap;

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
        this.url = url;

        database = FarragoDbSingleton.pinReference(sessionFactory);
        FarragoDbSingleton.addSession(database, this);
        boolean success = false;
        try {
            init(info);
            success = true;
        } finally {
            if (!success) {
                closeAllocation();
            }
        }
    }

    private void init(Properties info)
    {
        sessionVariables = new FarragoSessionVariables();

        String sessionUser = info.getProperty("user", "GUEST");
        sessionVariables.sessionUserName = sessionUser;
        sessionVariables.currentUserName = sessionUser;
        sessionVariables.currentRoleName = "";
        // TODO jvs 27-Aug-2005:  for remote connections, get this from client
        sessionVariables.systemUserName = System.getProperty("user.name");
        sessionVariables.schemaSearchPath = Collections.EMPTY_LIST;

        FemUser femUser = null;
        
        if (MDR_USER_NAME.equals(sessionVariables.sessionUserName)) {
            // This is a reentrant session from MDR.
            repos = database.getSystemRepos();
        } else {
            // This is a normal session.
            repos = database.getUserRepos();

            femUser = 
                FarragoCatalogUtil.getUserByName(repos, sessionUser);
            if (femUser == null) {
                throw FarragoResource.instance().SessionUnknownUser.ex(
                    repos.getLocalizedObjectName(sessionUser));
            } else {
                // TODO:  authenticate
            }
        }

        fennelTxnContext =
            sessionFactory.newFennelTxnContext(
                repos,
                database.getFennelDbHandle());

        CwmNamespace defaultNamespace = null;
        if (femUser != null) {
            defaultNamespace = femUser.getDefaultNamespace();
        }
        if (defaultNamespace == null) {
            sessionVariables.catalogName = repos.getSelfAsCatalog().getName();
        } else if (defaultNamespace instanceof CwmCatalog) {
            sessionVariables.catalogName = defaultNamespace.getName();
        } else {
            sessionVariables.schemaName = defaultNamespace.getName();
            sessionVariables.catalogName =
                defaultNamespace.getNamespace().getName();
        }

        txnCodeCache = new HashMap();

        isAutoCommit = true;

        savepointList = new ArrayList();

        sessionIndexMap = new FarragoDbSessionIndexMap(this, database, repos);
        
        personality = sessionFactory.newSessionPersonality(this, null);
        defaultPersonality = personality;
    }

    //~ Methods ---------------------------------------------------------------

    // implement FarragoSession
    public FarragoSessionFactory getSessionFactory()
    {
        return sessionFactory;
    }
    
    // implement FarragoSession
    public FarragoSessionPersonality getPersonality()
    {
        return personality;
    }
    
    // implement FarragoSession
    public void setDatabaseMetaData(DatabaseMetaData dbMetaData)
    {
        this.dbMetaData = dbMetaData;
    }

    // implement FarragoSession
    public void setConnectionSource(FarragoSessionConnectionSource source)
    {
        this.connectionSource = source;
    }
    
    // implement FarragoSession
    public DatabaseMetaData getDatabaseMetaData()
    {
        return dbMetaData;
    }

    // implement FarragoSession
    public FarragoSessionConnectionSource getConnectionSource()
    {
        return connectionSource;
    }
    
    // implement FarragoSession
    public String getUrl()
    {
        return url;
    }

    // implement FarragoSession
    public FarragoPluginClassLoader getPluginClassLoader()
    {
        return database.getPluginClassLoader();
    }

    // implement FarragoSession
    public List getModelExtensions()
    {
        return database.getModelExtensions();
    }

    // implement FarragoSession
    public FarragoSessionStmtContext newStmtContext(
        FarragoSessionStmtParamDefFactory paramDefFactory)
    {
        FarragoDbStmtContext stmtContext = 
            new FarragoDbStmtContext(this, paramDefFactory);
        addAllocation(stmtContext);
        return stmtContext;
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
    public FarragoSessionPrivilegeChecker newPrivilegeChecker()
    {
        // Instantiate a new privilege checker
        return new FarragoDbSessionPrivilegeChecker(this);
    }

    // implement FarragoSession
    public FarragoSessionPrivilegeMap getPrivilegeMap()
    {
        if (privilegeMap == null) {
            FarragoDbSessionPrivilegeMap newPrivilegeMap =
                new FarragoDbSessionPrivilegeMap(repos.getModelView());
            getPersonality().definePrivileges(newPrivilegeMap);
            Iterator iter = getModelExtensions().iterator();
            while (iter.hasNext()) {
                FarragoSessionModelExtension ext =
                    (FarragoSessionModelExtension) iter.next();
                ext.definePrivileges(newPrivilegeMap);
            }
            newPrivilegeMap.makeImmutable();
            privilegeMap = newPrivilegeMap;
        }
        return privilegeMap;
    }
    
    // implement FarragoSession
    public FarragoSession cloneSession(
        FarragoSessionVariables inheritedVariables)
    {
        // TODO:  keep track of clones and make sure they aren't left hanging
        // around by the time stmt finishes executing
        try {
            FarragoDbSession clone = (FarragoDbSession) super.clone();
            clone.isClone = true;
            clone.allocations = new LinkedList();
            clone.savepointList = new ArrayList();
            if (inheritedVariables == null) {
                inheritedVariables = sessionVariables;
            }
            clone.sessionVariables = inheritedVariables.cloneVariables();
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
        if (fennelTxnContext == null) {
            return false;
        }
        return fennelTxnContext.isTxnInProgress();
    }

    // implement FarragoSession
    public void setAutoCommit(boolean autoCommit)
    {
        ResourceDefinition txnFeature =
            EigenbaseResource.instance().SQLFeature_E151;
        if (!autoCommit) {
            if (!personality.supportsFeature(txnFeature)) {
                throw EigenbaseResource.instance().SQLFeature_E151.ex();
            }
        }
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

    protected void endTransactionIfAuto(boolean commit)
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
        if (isClone || isClosed()) {
            return;
        }
        if (isTxnInProgress()) {
            if (isAutoCommit) {
                commitImpl();
            } else {
                // NOTE jvs 10-May-2005:  Technically,  we're supposed to throw
                // an invalid state exception here.  However, it's very
                // unlikely that the caller is going to handle it properly,
                // so instead we roll back here.  If they wanted their
                // changes committed, they should have said so.
                rollbackImpl();
            }
        }
        try {
            FarragoDbSingleton.disconnectSession(this);
        } finally {
            database = null;
            repos = null;
        }
    }

    // implement FarragoSession
    public void commit()
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().SessionNoCommitInAutocommit.ex();
        }
        commitImpl();
    }

    // implement FarragoSession
    public FarragoSessionSavepoint newSavepoint(String name)
    {
        if (name != null) {
            if (findSavepointByName(name, false) != -1) {
                throw FarragoResource.instance().SessionDupSavepointName.ex(
                    repos.getLocalizedObjectName(name));
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
    public FarragoSessionAnalyzedSql analyzeSql(
        String sql,
        RelDataTypeFactory typeFactory,
        RelDataType paramRowType)
    {
        FarragoSessionAnalyzedSql analyzedSql =
            getAnalysisBlock(typeFactory);
        analyzedSql.paramRowType = paramRowType;
        FarragoSessionExecutableStmt stmt = prepare(
            sql, null, false, analyzedSql);
        assert (stmt == null);
        if (typeFactory != null) {
            // Have to copy types into the caller's factory since
            // analysis uses a private factory.
            if (analyzedSql.paramRowType != null) {
                analyzedSql.paramRowType = typeFactory.copyType(
                    analyzedSql.paramRowType);
            }
            if (analyzedSql.resultType != null) {
                analyzedSql.resultType = typeFactory.copyType(
                    analyzedSql.resultType);
            }
        }
        return analyzedSql;
    }

    public FarragoSessionAnalyzedSql getAnalysisBlock(RelDataTypeFactory typeFactory)
    {
        return new FarragoSessionAnalyzedSql();
    }

    public FarragoDatabase getDatabase()
    {
        return database;
    }

    public FarragoDbSessionIndexMap getSessionIndexMap()
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
            throw FarragoResource.instance().SessionNoRollbackInAutocommit.ex();
        }
        if (savepoint == null) {
            rollbackImpl();
        } else {
            int iSavepoint = validateSavepoint(savepoint);
            rollbackToSavepoint(iSavepoint);
        }
    }

    // implement FarragoSession
    public Collection executeLurqlQuery(
        String lurql,
        Map argMap)
    {
        // TODO jvs 24-May-2005:  query cache
        Connection connection = null;
        try {
            if (connectionSource != null) {
                connection = connectionSource.newConnection();
            }
            JmiQueryProcessor queryProcessor =
                getPersonality().newJmiQueryProcessor("LURQL");
            JmiPreparedQuery query = queryProcessor.prepare(
                getRepos().getModelView(), lurql);
            return query.execute(connection, argMap);
        } catch (JmiQueryException ex) {
            throw FarragoResource.instance().SessionInternalQueryFailed.ex(ex);
        } finally {
            Util.squelchConnection(connection);
        }
    }

    protected FarragoSessionRuntimeParams newRuntimeContextParams()
    {
        FarragoSessionRuntimeParams params = new FarragoSessionRuntimeParams();
        params.session = this;
        params.repos = getRepos();
        params.codeCache = getDatabase().getCodeCache();
        params.txnCodeCache = getTxnCodeCache();
        params.fennelTxnContext = getFennelTxnContext();
        params.indexMap = getSessionIndexMap();
        params.sessionVariables = getSessionVariables().cloneVariables();
        params.sharedDataWrapperCache = getDatabase().getDataWrapperCache();
        params.streamFactoryProvider = personality;
        return params;
    }

    private FarragoSessionSavepoint newSavepointImpl(String name)
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().SessionNoSavepointInAutocommit.ex();
        }
        FennelSvptHandle fennelSvptHandle;
        if (repos.isFennelEnabled()) {
            fennelSvptHandle = fennelTxnContext.newSavepoint();
        } else {
            fennelSvptHandle = null;
        }
        FarragoDbSavepoint newSavepoint =
            new FarragoDbSavepoint(nextSavepointId++, name, fennelSvptHandle,
                this);
        savepointList.add(newSavepoint);
        return newSavepoint;
    }

    private int validateSavepoint(FarragoSessionSavepoint savepoint)
    {
        if (!(savepoint instanceof FarragoDbSavepoint)) {
            throw FarragoResource.instance().SessionWrongSavepoint.ex(
                repos.getLocalizedObjectName(savepoint.getName()));
        }
        FarragoDbSavepoint dbSavepoint = (FarragoDbSavepoint) savepoint;
        if (dbSavepoint.session != this) {
            throw FarragoResource.instance().SessionWrongSavepoint.ex(
                savepoint.getName());
        }
        int iSavepoint = findSavepoint(savepoint);
        if (iSavepoint == -1) {
            if (savepoint.getName() == null) {
                throw FarragoResource.instance().SessionInvalidSavepointId.ex(
                    new Integer(savepoint.getId()));
            } else {
                throw FarragoResource.instance().SessionInvalidSavepointName.ex(
                        repos.getLocalizedObjectName(savepoint.getName()));
            }
        }
        return iSavepoint;
    }

    private int findSavepointByName(
        String name,
        boolean throwIfNotFound)
    {
        for (int i = 0; i < savepointList.size(); ++i) {
            FarragoDbSavepoint savepoint =
                (FarragoDbSavepoint) savepointList.get(i);
            if (name.equals(savepoint.getName())) {
                return i;
            }
        }
        if (throwIfNotFound) {
            throw FarragoResource.instance().SessionInvalidSavepointName.ex(name);
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
            throw FarragoResource.instance().SessionNoRollbackInAutocommit.ex();
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

    protected FarragoSessionExecutableStmt prepare(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionAnalyzedSql analyzedSql)
    {
        tracer.info(sql);

        // TODO jvs 11-Aug-2004:  Get rid of this big mutex.  It needs to stay
        // until we have proper object-level DDL-locking.  For now the
        // contention is the same as that due to the TODO below since the
        // MDR write lock is exclusive.
        synchronized (database.DDL_LOCK) {
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
            FarragoSessionStmtValidator stmtValidator =
                newStmtValidator();
            FarragoSessionExecutableStmt stmt = null;
            try {
                stmt =
                    prepareImpl(sql, owner, isExecDirect, analyzedSql,
                        stmtValidator, reposTxnContext, pRollback);
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
        FarragoSessionAnalyzedSql analyzedSql,
        FarragoSessionStmtValidator stmtValidator,
        FarragoReposTxnContext reposTxnContext,
        boolean [] pRollback)
    {
        // REVIEW: For !isExecDirect, maybe should disable all DDL
        // validation: just parse, because the catalog may change by the
        // time the statement is executed.  Also probably need to disallow
        // some types of prepared DDL.
        FarragoSessionDdlValidator ddlValidator =
            personality.newDdlValidator(stmtValidator);
        FarragoSessionParser parser = stmtValidator.getParser();

        boolean expectStatement = true;
        if ((analyzedSql != null) && (analyzedSql.paramRowType != null)) {
            expectStatement = false;
        }

        Object parsedObj = parser.parseSqlText(
            stmtValidator, ddlValidator, sql, expectStatement);

        if (parsedObj instanceof SqlNode) {
            SqlNode sqlNode = (SqlNode) parsedObj;
            pRollback[0] = false;
            ddlValidator.closeAllocation();
            ddlValidator = null;
            personality.validate(stmtValidator, sqlNode);
            FarragoSessionExecutableStmt stmt =
                database.prepareStmt(
                    stmtValidator, sqlNode, owner, analyzedSql);
            if (isExecDirect
                && (stmt.getDynamicParamRowType().getFieldList().size() > 0))
            {
                owner.closeAllocation();
                throw FarragoResource.instance().
                    SessionNoExecuteImmediateParameters.ex(sql);
            }
            return stmt;
        }

        FarragoSessionDdlStmt ddlStmt = (FarragoSessionDdlStmt) parsedObj;

        validateDdl(ddlValidator, reposTxnContext, ddlStmt);
        
        if (!isExecDirect) {
            return null;
        }

        executeDdl(ddlValidator, reposTxnContext, ddlStmt);

        pRollback[0] = false;
        return null;
    }

    private void validateDdl(
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
    }
    
    private void executeDdl(
        FarragoSessionDdlValidator ddlValidator,
        FarragoReposTxnContext reposTxnContext,
        FarragoSessionDdlStmt ddlStmt)
    {
        tracer.fine("updating storage");
        ddlValidator.executeStorage();

        try {
            ddlStmt.preExecute();
            if (ddlStmt instanceof DdlStmt) {
                ((DdlStmt) ddlStmt).visit(new DdlExecutionVisitor());
            }
            ddlStmt.postExecute();

            tracer.fine("committing DDL");
            reposTxnContext.commit();

            if (shutDownRequested) {
                closeAllocation();
                database.shutdown();
                if (catalogDumpRequested) {
                    try {
                        FarragoReposUtil.dumpRepository();
                    } catch (Exception ex) {
                        throw FarragoResource.instance().CatalogDumpFailed.ex(
                            ex);
                    }
                }
            }
            
        } finally {
            shutDownRequested = false;
            catalogDumpRequested = false;
        }
    }

    //~ Inner Classes ---------------------------------------------------------

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
                    findSavepointByName(
                        rollbackStmt.getSavepointName(),
                        true);
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
                findSavepointByName(
                    releaseStmt.getSavepointName(),
                    true);
            releaseSavepoint(iSavepoint);
        }

        // implement DdlVisitor
        public void visit(DdlSetCatalogStmt stmt)
        {
            SqlIdentifier id = stmt.getCatalogName();
            sessionVariables.catalogName = id.getSimple();
        }

        // implement DdlVisitor
        public void visit(DdlSetSchemaStmt stmt)
        {
            SqlIdentifier id = stmt.getSchemaName();
            if (id.isSimple()) {
                sessionVariables.schemaName = id.getSimple();
            } else {
                sessionVariables.catalogName = id.names[0];
                sessionVariables.schemaName = id.names[1];
            }
        }

        // implement DdlVisitor
        public void visit(DdlSetPathStmt stmt)
        {
            sessionVariables.schemaSearchPath =
                Collections.unmodifiableList(stmt.getSchemaList());
        }

        // implement DdlVisitor
        public void visit(DdlSetSystemParamStmt stmt)
        {
            database.updateSystemParameter(stmt);
        }

        // implement DdlVisitor
        public void visit(DdlCheckpointStmt stmt)
        {
            database.requestCheckpoint(false, false);
        }

        // implement DdlVisitor
        public void visit(DdlSetSessionImplementationStmt stmt)
        {
            personality = stmt.newPersonality(
                FarragoDbSession.this,
                defaultPersonality);
        }

        // implement DdlVisitor
        public void visit(DdlExtendCatalogStmt stmt)
        {
            // record the model extension plugin jar URL outside of the catalog
            // so that when we reboot it will be available to MDR
            database.saveBootUrl(stmt.getJarUrl());
            shutDownRequested = true;
            catalogDumpRequested = true;
        }
    }
}


// End FarragoDbSession.java

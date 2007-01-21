/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2003-2006 Disruptive Tech
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

import java.sql.*;

import java.util.*;
import java.util.logging.*;
import java.util.regex.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;

import org.eigenbase.jmi.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;

import javax.jmi.reflect.RefObject;


/**
 * FarragoDbSession implements the {@link net.sf.farrago.session.FarragoSession}
 * interface as a connection to a {@link FarragoDatabase} instance. It manages
 * private authorization and transaction context.
 *
 * <p>Most non-trivial public methods on this class must be synchronized, since
 * closeAllocation may be called from a thread shutting down the database.
 *
 * @author John V. Sichi
 * @version $Id: //open/dev/farrago/src/net/sf/farrago/db/FarragoDbSession.java#27
 */
public class FarragoDbSession
    extends FarragoCompoundAllocation
    implements FarragoSession,
        Cloneable
{

    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer =
        FarragoTrace.getDatabaseSessionTracer();

    private static final Logger sqlTimingTracer =
        EigenbaseTrace.getSqlTimingTracer();

    public static final String MDR_USER_NAME = "MDR";

    //~ Instance fields --------------------------------------------------------

    /**
     * Default personality for this session.
     */
    private FarragoSessionPersonality defaultPersonality;

    /**
     * Current personality for this session.
     */
    private FarragoSessionPersonality personality;

    /**
     * Fennel transaction context for this session
     */
    private FennelTxnContext fennelTxnContext;

    /**
     * Current transaction ID, or null if none active.
     */
    private FarragoSessionTxnId txnId;

    /**
     * Qualifiers to assume for unqualified object references
     */
    private FarragoSessionVariables sessionVariables;

    /**
     * Database accessed by this session
     */
    private FarragoDatabase database;

    /**
     * Repos accessed by this session
     */
    private FarragoRepos repos;

    /**
     * URL used to connect this session.
     */
    private String url;

    /**
     * Warnings accumulated on this session.
     */
    FarragoWarningQueue warningQueue;

    /**
     * Was this session produced by cloning?
     */
    private boolean isClone;
    private boolean isAutoCommit;
    private boolean shutDownRequested;
    private boolean catalogDumpRequested;
    private boolean wasKilled;

    /**
     * List of savepoints established within current transaction which have not
     * been released or rolled back; order is from earliest to latest.
     */
    private List savepointList;

    /**
     * Generator for savepoint Id's.
     */
    private int nextSavepointId;

    /**
     * Map of temporary indexes created by this session.
     */
    private FarragoSessionIndexMap sessionIndexMap;

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

    private FarragoDbSessionInfo sessionInfo;

    private Pattern optRuleDescExclusionFilter;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoDbSession object.
     *
     * @param url URL used to connect (same as JDBC)
     * @param info properties for this session
     * @param sessionFactory factory which created this session
     */
    public FarragoDbSession(
        String url,
        Properties info,
        FarragoSessionFactory sessionFactory)
    {
        this.sessionFactory = sessionFactory;
        this.url = url;
        warningQueue = new FarragoWarningQueue();

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

    //~ Methods ----------------------------------------------------------------

    private void init(Properties info)
    {
        sessionVariables = new FarragoSessionVariables();

        String sessionUser = info.getProperty("user", "GUEST");
        sessionVariables.sessionUserName = sessionUser;
        sessionVariables.currentUserName = sessionUser;
        sessionVariables.currentRoleName = "";
        sessionVariables.systemUserName =
            info.getProperty(
                "clientUserName",
                System.getProperty("user.name"));
        sessionVariables.systemUserFullName =
            info.getProperty(
                "clientUserFullName");
        sessionVariables.schemaSearchPath = Collections.emptyList();
        sessionVariables.sessionName = info.getProperty("sessionName");
        sessionVariables.programName = info.getProperty("clientProgramName");
        sessionVariables.processId = 0L;
        String processStr = info.getProperty("clientProcessId");
        if ((processStr != null) && (processStr.length() > 0)) {
            try {
                sessionVariables.processId = Long.parseLong(processStr);
            } catch (NumberFormatException ex) {
                // NOTE jvs 12-Nov-2006:  It's OK to discard ex here
                // because it provides only useless information.
                getWarningQueue().postWarning(
                    FarragoResource.instance().
                    SessionClientProcessIdNotNumeric.ex(processStr));
            }
        }

        FemUser femUser = null;

        if (MDR_USER_NAME.equals(sessionVariables.sessionUserName)) {
            // This is a reentrant session from MDR.
            repos = database.getSystemRepos();
            if (sessionVariables.sessionName == null) {
                sessionVariables.sessionName = MDR_USER_NAME;
            }
        } else {
            // This is a normal session.
            // Security best practices for failed login attempts:
            // * report only that username/password combination is invalid
            // * use same error for "no such user" and "wrong password"
            // * do not reveal that username exists but password wrong
            repos = database.getUserRepos();
            femUser = FarragoCatalogUtil.getUserByName(repos, sessionUser);
            if (femUser == null) {
                throw FarragoResource.instance().SessionLoginFailed.ex(
                    repos.getLocalizedObjectName(sessionUser));
            } else {
                // TODO:  authenticate; use same SessionLoginFailed if fails
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
        personality.loadDefaultSessionVariables(sessionVariables);

        sessionInfo = new FarragoDbSessionInfo(this, database);
    }

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
    public List<FarragoSessionModelExtension> getModelExtensions()
    {
        return database.getModelExtensions();
    }

    // implement FarragoSession
    public FarragoSessionInfo getSessionInfo()
    {
        return sessionInfo;
    }

    // implement FarragoSession
    public FarragoWarningQueue getWarningQueue()
    {
        return warningQueue;
    }
    
    // implement FarragoSession
    public synchronized FarragoSessionStmtContext newStmtContext(
        FarragoSessionStmtParamDefFactory paramDefFactory)
    {
        FarragoDbStmtContext stmtContext =
            new FarragoDbStmtContext(
                this,
                paramDefFactory,
                database.getDdlLockManager());
        addAllocation(stmtContext);
        return stmtContext;
    }

    // implement FarragoSession
    public synchronized FarragoSessionStmtValidator newStmtValidator()
    {
        return
            new FarragoStmtValidator(
                getRepos(),
                getDatabase().getFennelDbHandle(),
                this,
                getDatabase().getCodeCache(),
                getDatabase().getDataWrapperCache(),
                getSessionIndexMap(),
                getDatabase().getDdlLockManager());
    }

    // implement FarragoSession
    public synchronized FarragoSessionPrivilegeChecker newPrivilegeChecker()
    {
        // Instantiate a new privilege checker
        return new FarragoDbSessionPrivilegeChecker(this);
    }

    // implement FarragoSession
    public synchronized FarragoSessionPrivilegeMap getPrivilegeMap()
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
    public synchronized FarragoSession cloneSession(
        FarragoSessionVariables inheritedVariables)
    {
        // TODO:  keep track of clones and make sure they aren't left hanging
        // around by the time stmt finishes executing; also,
        // maybe auto-propagate unretrieved warnings from clones?
        try {
            FarragoDbSession clone = (FarragoDbSession) super.clone();
            clone.isClone = true;
            clone.allocations = new LinkedList();
            clone.savepointList = new ArrayList();
            clone.warningQueue = new FarragoWarningQueue();
            if (isTxnInProgress()) {
                // Calling statement has already started a transaction:
                // make sure clone doesn't interfere by autocommitting.
                clone.isAutoCommit = false;
            } else {
                // Otherwise, inherit autocommit setting.
                clone.isAutoCommit = isAutoCommit;
            }
            if (inheritedVariables == null) {
                inheritedVariables = sessionVariables;
            }
            clone.sessionVariables =
                personality.createInheritedSessionVariables(inheritedVariables);
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
    public synchronized boolean isClosed()
    {
        return (database == null);
    }

    // implement FarragoSession
    public synchronized boolean wasKilled()
    {
        return isClosed() && wasKilled;
    }

    // implement FarragoSession
    public synchronized boolean isTxnInProgress()
    {
        // TODO jvs 9-Mar-2006:  Unify txn state.
        if (txnId != null) {
            return true;
        }
        if (fennelTxnContext == null) {
            return false;
        }
        return fennelTxnContext.isTxnInProgress();
    }

    // implement FarragoSession
    public synchronized FarragoSessionTxnId getTxnId(boolean createIfNeeded)
    {
        if ((txnId == null) && createIfNeeded) {
            txnId = getTxnMgr().beginTxn(this);
        }
        return txnId;
    }

    // implement FarragoSession
    public FarragoSessionTxnMgr getTxnMgr()
    {
        return database.getTxnMgr();
    }

    // implement FarragoSession
    public synchronized void setAutoCommit(boolean autoCommit)
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

    // implement FarragoSession
    public synchronized void endTransactionIfAuto(boolean commit)
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

    // NOTE jvs 16-Jan-2007:  Don't make this synchronized, since that
    // would reverse the synchronization order inside of closeAllocation,
    // leading to deadlock
    
    // implement FarragoSession
    public void kill()
    {
        closeAllocation();
        wasKilled = true;
    }

    // implement FarragoSession
    public void cancel()
    {
        for (Long id : sessionInfo.getExecutingStmtIds()) {
            FarragoSessionExecutingStmtInfo info =
                sessionInfo.getExecutingStmtInfo(id);
            info.getStmtContext().cancel();
        }
    }
    
    // implement FarragoAllocation
    public void closeAllocation()
    {
        synchronized (FarragoDbSingleton.class) {
            synchronized (this) {
                super.closeAllocation();
                if (isClone || isClosed()) {
                    return;
                }
                if (isTxnInProgress()) {
                    if (isAutoCommit) {
                        commitImpl();
                    } else {
                        // NOTE jvs 10-May-2005: Technically, we're
                        // supposed to throw an invalid state
                        // exception here.  However, it's very
                        // unlikely that the caller is going to handle
                        // it properly, so instead we roll back.  If
                        // they wanted their changes committed, they
                        // should have said so.
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
        }
    }

    // implement FarragoSession
    public synchronized void commit()
    {
        if (isAutoCommit) {
            throw FarragoResource.instance().SessionNoCommitInAutocommit.ex();
        }
        commitImpl();
    }

    // implement FarragoSession
    public synchronized FarragoSessionSavepoint newSavepoint(String name)
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
    public synchronized void releaseSavepoint(FarragoSessionSavepoint savepoint)
    {
        int iSavepoint = validateSavepoint(savepoint);
        releaseSavepoint(iSavepoint);
    }

    // implement FarragoSession
    public synchronized FarragoSessionAnalyzedSql analyzeSql(
        String sql,
        RelDataTypeFactory typeFactory,
        RelDataType paramRowType,
        boolean optimize)
    {
        FarragoSessionAnalyzedSql analyzedSql = getAnalysisBlock(typeFactory);
        analyzedSql.optimized = optimize;
        analyzedSql.paramRowType = paramRowType;
        FarragoSessionExecutableStmt stmt =
            prepare(
                null,
                sql,
                null,
                false,
                analyzedSql);
        assert (stmt == null);
        if (typeFactory != null) {
            // Have to copy types into the caller's factory since
            // analysis uses a private factory.
            if (analyzedSql.paramRowType != null) {
                analyzedSql.paramRowType =
                    typeFactory.copyType(
                        analyzedSql.paramRowType);
            }
            if (analyzedSql.resultType != null) {
                analyzedSql.resultType =
                    typeFactory.copyType(
                        analyzedSql.resultType);
            }
        }
        return analyzedSql;
    }

    public FarragoSessionAnalyzedSql getAnalysisBlock(
        RelDataTypeFactory typeFactory)
    {
        return new FarragoSessionAnalyzedSql();
    }

    public FarragoDatabase getDatabase()
    {
        return database;
    }

    public FarragoSessionIndexMap getSessionIndexMap()
    {
        return sessionIndexMap;
    }

    public synchronized void setSessionIndexMap(
        FarragoSessionIndexMap sessionIndexMap)
    {
        this.sessionIndexMap = sessionIndexMap;
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
        if (fennelTxnContext != null) {
            fennelTxnContext.commit();
        }
        onEndOfTransaction(FarragoSessionTxnEnd.COMMIT);
        sessionIndexMap.onCommit();
    }

    void rollbackImpl()
    {
        tracer.info("rollback");
        if (fennelTxnContext != null) {
            fennelTxnContext.rollback();
        }
        onEndOfTransaction(FarragoSessionTxnEnd.ROLLBACK);
    }

    private void onEndOfTransaction(
        FarragoSessionTxnEnd eot)
    {
        if (txnId != null) {
            getTxnMgr().endTxn(txnId, eot);
            txnId = null;
        }
        savepointList.clear();
        Iterator iter = txnCodeCache.values().iterator();
        while (iter.hasNext()) {
            // REVIEW jvs 26-Nov-2006:  for pinned ExecStreamGraphs
            // (and maybe other statement-related resources) can
            // we verify that they are no longer in use?
            FarragoAllocation alloc = (FarragoAllocation) iter.next();
            alloc.closeAllocation();
        }
        txnCodeCache.clear();
    }

    // implement FarragoSession
    public synchronized void rollback(FarragoSessionSavepoint savepoint)
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
    public synchronized Collection<RefObject> executeLurqlQuery(
        String lurql,
        Map<String,?> argMap)
    {
        // TODO jvs 24-May-2005:  query cache
        Connection connection = null;
        try {
            if (connectionSource != null) {
                connection = connectionSource.newConnection();
            }
            JmiQueryProcessor queryProcessor =
                getPersonality().newJmiQueryProcessor("LURQL");
            JmiPreparedQuery query =
                queryProcessor.prepare(
                    getRepos().getModelView(),
                    lurql);
            return query.execute(connection, argMap);
        } catch (JmiQueryException ex) {
            throw FarragoResource.instance().SessionInternalQueryFailed.ex(ex);
        } finally {
            Util.squelchConnection(connection);
        }
    }

    // implement FarragoSession
    public synchronized void setOptRuleDescExclusionFilter(
        Pattern exclusionFilter)
    {
        optRuleDescExclusionFilter = exclusionFilter;
    }

    // implement FarragoSession
    public Pattern getOptRuleDescExclusionFilter()
    {
        return optRuleDescExclusionFilter;
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
            throw FarragoResource.instance().SessionNoSavepointInAutocommit
            .ex();
        }
        FennelSvptHandle fennelSvptHandle = fennelTxnContext.newSavepoint();
        FarragoDbSavepoint newSavepoint =
            new FarragoDbSavepoint(nextSavepointId++,
                name,
                fennelSvptHandle,
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
                    savepoint.getId());
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
            throw FarragoResource.instance().SessionInvalidSavepointName.ex(
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
        FarragoDbStmtContextBase stmtContext,
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionAnalyzedSql analyzedSql)
    {
        tracer.info(sql);

        EigenbaseTimingTracer timingTracer =
            new EigenbaseTimingTracer(
                sqlTimingTracer,
                "begin prepare");

        // TODO jvs 20-Jan-2007: Replace this big mutex with a
        // readers/writers lock.
        synchronized (database.DDL_LOCK) {
            FarragoReposTxnContext reposTxnContext = repos.newTxnContext();

            boolean [] pRollback = new boolean[1];
            pRollback[0] = true;
            FarragoSessionStmtValidator stmtValidator = newStmtValidator();
            stmtValidator.setTimingTracer(timingTracer);
            
            // Pass the repos txn context to the statement validator so
            // the parser can access it and start the appropriate type of
            // repository transaction (for DDL vs not DDL)
            stmtValidator.setReposTxnContext(reposTxnContext);
            
            FarragoSessionExecutableStmt stmt = null;
            try {
                stmt =
                    prepareImpl(sql,
                        owner,
                        isExecDirect,
                        analyzedSql,
                        stmtContext,
                        stmtValidator,
                        reposTxnContext,
                        pRollback);

                // NOTE jvs 17-Mar-2006:  We have to do this here
                // rather than in FarragoDbStmtContext.finishPrepare
                // to ensure that's there's no window in between
                // when we release the mutex and lock the objects;
                // otherwise a DROP might slip in and yank them out
                // from under us.
                if ((stmt != null) && (stmtContext != null)) {
                    stmtContext.lockObjectsInUse(stmt);
                }
            } finally {
                if (stmtValidator != null) {
                    stmtValidator.closeAllocation();
                }
                
                // MDR doesn't allow rollback on read-only txns
                if (pRollback[0] && !reposTxnContext.isReadTxnInProgress()) {
                    tracer.fine("rolling back DDL");
                    reposTxnContext.rollback();
                } else {
                    reposTxnContext.commit();
                }
            }
            timingTracer.traceTime("end prepare");
            return stmt;
        }
    }

    private FarragoSessionExecutableStmt prepareImpl(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionAnalyzedSql analyzedSql,
        FarragoSessionStmtContext stmtContext,
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

        Object parsedObj =
            parser.parseSqlText(
                stmtValidator,
                ddlValidator,
                sql,
                expectStatement);

        stmtValidator.getTimingTracer().traceTime("end parse");

        if (parsedObj instanceof SqlNode) {
            SqlNode sqlNode = (SqlNode) parsedObj;
            pRollback[0] = false;
            ddlValidator.closeAllocation();
            ddlValidator = null;
            personality.validate(stmtValidator, sqlNode);
            FarragoSessionExecutableStmt stmt =
                database.prepareStmt(
                    stmtContext,
                    stmtValidator,
                    sqlNode,
                    owner,
                    analyzedSql);
            if (isExecDirect
                && (stmt.getDynamicParamRowType().getFieldList().size() > 0)) {
                owner.closeAllocation();
                throw FarragoResource.instance()
                .SessionNoExecuteImmediateParameters.ex(sql);
            }
            return stmt;
        }

        FarragoSessionDdlStmt ddlStmt = (FarragoSessionDdlStmt) parsedObj;

        validateDdl(ddlValidator, reposTxnContext, ddlStmt);

        stmtValidator.getTimingTracer().traceTime("end DDL validation");

        if (!isExecDirect) {
            return null;
        }

        executeDdl(ddlValidator, reposTxnContext, ddlStmt);

        stmtValidator.getTimingTracer().traceTime("end DDL execution");

        pRollback[0] = false;
        return null;
    }

    private void validateDdl(
        FarragoSessionDdlValidator ddlValidator,
        FarragoReposTxnContext reposTxnContext,
        FarragoSessionDdlStmt ddlStmt)
    {
        if (ddlStmt.requiresCommit()) {
            // most DDL causes implicit commit of any pending txn
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
        if (ddlStmt.requiresCommit()) {
            // start a Fennel txn to cover any effects on storage
            fennelTxnContext.initiateTxn();
        }

        boolean rollbackFennel = true;
        try {
            ddlValidator.executeStorage();
            ddlStmt.preExecute();
            if (ddlStmt instanceof DdlStmt) {
                ((DdlStmt) ddlStmt).visit(new DdlExecutionVisitor());
            }
            ddlStmt.postExecute();

            tracer.fine("committing DDL");
            reposTxnContext.commit();
            commitImpl();
            rollbackFennel = false;
            ddlStmt.postCommit(ddlValidator);

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
            if (rollbackFennel) {
                rollbackImpl();
            }
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private class DdlExecutionVisitor
        extends DdlVisitor
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
            personality =
                stmt.newPersonality(
                    FarragoDbSession.this,
                    defaultPersonality);
            personality.loadDefaultSessionVariables(sessionVariables);
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

        // implement DdlVisitor
        public void visit(DdlReplaceCatalogStmt stmt)
        {
            shutDownRequested = true;
            catalogDumpRequested = true;
        }
    }
}

// End FarragoDbSession.java

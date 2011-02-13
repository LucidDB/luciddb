/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2003 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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

import javax.jmi.reflect.*;

import javax.security.auth.callback.*;
import javax.security.auth.login.*;
import javax.sql.DataSource;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.core.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.security.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.query.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.util.*;
import net.sf.farrago.namespace.util.FarragoDataWrapperCache;

import org.eigenbase.jmi.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.resgen.*;
import org.eigenbase.resource.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.util.SqlString;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;


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

    protected static final Logger tracer =
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
     * Reference to current transaction ID, or null if none active. We do it
     * this way so that reference is shared across all clones via shallow-copy.
     */
    private TxnIdRef txnIdRef;

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
    private boolean shutdownRequested;
    private boolean reposSessionEnded;
    private boolean wasKilled;

    /**
     * List of savepoints established within current transaction which have not
     * been released or rolled back; order is from earliest to latest.
     */
    private List<FarragoDbSavepoint> savepointList;

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
    private Map<String, FarragoObjectCache.Entry> txnCodeCache;
    private DatabaseMetaData dbMetaData;
    protected FarragoSessionFactory sessionFactory;

    private FarragoSessionPrivilegeMap privilegeMap;

    private FarragoDbSessionInfo sessionInfo;

    private Pattern optRuleDescExclusionFilter;

    private SessionLabel sessionLabel;

    private boolean isLoopback;

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
        txnIdRef = new TxnIdRef();
        sessionLabel = null;

        boolean requireExistingEngine =
            info.getProperty(
                "requireExistingEngine",
                "false").equalsIgnoreCase("true");
        database =
            FarragoDbSingleton.pinReference(
                sessionFactory,
                requireExistingEngine);
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
                    FarragoResource.instance().SessionClientProcessIdNotNumeric
                    .ex(processStr));
            }
        }
        String remoteProtocol = info.getProperty("remoteProtocol", "none");
        FemUser femUser = null;

        final boolean reentrantMdrSession;
        if (MDR_USER_NAME.equals(sessionVariables.sessionUserName)) {
            // This is a reentrant session from MDR.
            repos = database.getSystemRepos();
            reentrantMdrSession = true;
        } else {
            repos = database.getUserRepos();
            reentrantMdrSession = false;
        }

        FarragoReposTxnContext txn = repos.newTxnContext(true);
        txn.beginReadTxn();
        try {
            if (reentrantMdrSession) {
                if (sessionVariables.sessionName == null) {
                    sessionVariables.sessionName = MDR_USER_NAME;
                }
            } else {
                // This is a normal session.
                // Security best practices for failed login attempts:
                // * report only that username/password combination is invalid
                // * use same error for "no such user" and "wrong password"
                // * do not reveal that username exists but password wrong
                femUser = FarragoCatalogUtil.getUserByName(repos, sessionUser);
                if (femUser == null) {
                    throw FarragoResource.instance().SessionLoginFailed.ex(
                        repos.getLocalizedObjectName(sessionUser));
                } else if (
                    database.isJaasAuthenticationEnabled()
                    && (database.shouldAuthenticateLocalConnections()
                        || !remoteProtocol.equals("none")))
                {
                    // authenticate; use same SessionLoginFailed if fails
                    LoginContext lc;
                    CallbackHandler cbh =
                        new FarragoNoninteractiveCallbackHandler(
                            info.getProperty("user", "GUEST"),
                            info.getProperty("password"));
                    try {
                        lc = new LoginContext(
                            "Farrago",
                            null,
                            cbh,
                            database.getJaasConfig());
                        lc.login();
                    } catch (LoginException ex) {
                        throw FarragoResource.instance().SessionLoginFailed.ex(
                            repos.getLocalizedObjectName(sessionUser));
                    }

                    try {
                        lc.logout();
                    } catch (LoginException ex) {
                        throw FarragoResource.instance().SessionLogoutFailed.ex(
                            repos.getLocalizedObjectName(sessionUser));
                    }
                }
                // Regardless of whether JAAS is configured, if the user
                // has a password set in the catalog, enforce it.
                if (femUser.getEncryptedPassword() != null) {
                    String plaintext = info.getProperty("password");
                    String cyphertext;
                    if (plaintext != null) {
                        cyphertext =
                            FarragoUtil.encryptPassword(
                                plaintext,
                                femUser.getPasswordEncryptionAlgorithm());
                    } else {
                        cyphertext = null;
                    }
                    if (!femUser.getEncryptedPassword().equals(cyphertext)) {
                        throw FarragoResource
                            .instance().SessionLoginFailed.ex(
                                repos.getLocalizedObjectName(sessionUser));
                    }
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
                sessionVariables.catalogName =
                    repos.getSelfAsCatalog().getName();
            } else if (defaultNamespace instanceof CwmCatalog) {
                sessionVariables.catalogName = defaultNamespace.getName();
            } else {
                sessionVariables.schemaName = defaultNamespace.getName();
                sessionVariables.catalogName =
                    defaultNamespace.getNamespace().getName();
            }
        } finally {
            txn.commit();
        }

        txnCodeCache = new HashMap<String, FarragoObjectCache.Entry>();

        isAutoCommit = true;

        savepointList = new ArrayList<FarragoDbSavepoint>();

        sessionIndexMap = new FarragoDbSessionIndexMap(this, this, repos);

        personality = sessionFactory.newSessionPersonality(this, null);
        defaultPersonality = personality;
        personality.loadDefaultSessionVariables(sessionVariables);

        // If a session label has been specified, make sure the personality
        // supports snapshot reads and the label is valid
        String labelName = info.getProperty("label", null);
        if (labelName != null) {
            if (!getPersonality().supportsFeature(
                    EigenbaseResource.instance().PersonalitySupportsSnapshots))
            {
                throw EigenbaseResource.instance().PersonalitySupportsSnapshots
                .ex();
            }
            txn.beginReadTxn();
            try {
                FemLabel label =
                    (FemLabel) FarragoCatalogUtil.getModelElementByName(
                        repos.allOfType(FemLabel.class),
                        labelName);
                if (label == null) {
                    throw FarragoResource.instance().InvalidLabelProperty.ex(
                        labelName);
                }
                sessionVariables.set(
                    FarragoDefaultSessionPersonality.LABEL,
                    labelName);
                setSessionLabel(label);
            } finally {
                txn.commit();
            }
        }

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
        return newStmtContext(paramDefFactory, null);
    }

    // implement FarragoSession
    public synchronized FarragoSessionStmtContext newStmtContext(
        FarragoSessionStmtParamDefFactory paramDefFactory,
        FarragoSessionStmtContext rootStmtContext)
    {
        FarragoDbStmtContext stmtContext =
            new FarragoDbStmtContext(
                this,
                paramDefFactory,
                database.getDdlLockManager(),
                rootStmtContext);
        addAllocation(stmtContext);
        return stmtContext;
    }

    // implement FarragoSession
    public synchronized FarragoSessionStmtValidator newStmtValidator()
    {
        return new FarragoStmtValidator(
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
            for (FarragoSessionModelExtension ext : getModelExtensions()) {
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
            clone.allocations = new LinkedList<ClosableAllocation>();
            clone.savepointList = new ArrayList<FarragoDbSavepoint>();
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
    public boolean isClosed()
    {
        // NOTE jvs 18-Nov-2008:  don't mark this method as
        // synchronized; see FRG-294 for why.
        return (database == null);
    }

    // implement FarragoSession
    public synchronized boolean wasKilled()
    {
        return isClosed() && wasKilled;
    }

    // implement FarragoSession
    public boolean isTxnInProgress()
    {
        // NOTE jvs 18-Nov-2008:  don't mark this method as
        // synchronized; see FRG-294 for why.

        // TODO jvs 9-Mar-2006:  Unify txn state.
        if (txnIdRef.txnId != null) {
            return true;
        }

        FennelTxnContext contextToCheck = fennelTxnContext;

        if (contextToCheck == null) {
            return false;
        }
        return contextToCheck.isTxnInProgress();
    }

    // implement FarragoSession
    public synchronized FarragoSessionTxnId getTxnId(boolean createIfNeeded)
    {
        if ((txnIdRef.txnId == null) && createIfNeeded) {
            txnIdRef.txnId = getTxnMgr().beginTxn(this);
        }
        return txnIdRef.txnId;
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

    // implement FarragoSession
    public void disableSubqueryReduction()
    {
        sessionVariables.set(
            FarragoDefaultSessionPersonality.REDUCE_NON_CORRELATED_SUBQUERIES,
            "false");
    }

    // implement FarragoAllocation
    public void closeAllocation()
    {
        if (isClone) {
            // avoid synchronization for reentrant sessions; the
            // reverse ordering can cause deadlock:
            // top-level session close takes FarragoDbSingleton.class lock,
            // then statement context lock,
            // while top-level statement execution holds a lock on the
            // statement context, and then tries to acquire
            // FarragoDbSingleton.class lock
            super.closeAllocation();

            // The following will unlock any session labels set by the
            // reentrant session.
            // NOTE zfong 8/6/08 - In the case of a UDR session, this is
            // currently a no-op because session labels cannot be set
            // inside UDR's.
            setSessionLabel(null);
            return;
        }
        synchronized (FarragoDbSingleton.class) {
            synchronized (this) {
                super.closeAllocation();
                if (isClosed()) {
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
                    // unlock the session label, if it has been set
                    setSessionLabel(null);
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
        analyzedSql.rawString = new SqlString(SqlDialect.EIGENBASE, sql);
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

    private void setSessionLabel(FemLabel label)
    {
        FarragoDdlLockManager ddlLockManager =
            getDatabase().getDdlLockManager();

        // Unlock the current label
        if (sessionLabel != null) {
            ddlLockManager.removeObjectsInUse(this);
            tracer.info("Session label reset to null");
        }

        // If the label is an alias, determine the base label that the
        // alias maps to.  This will make it possible to reset the alias to
        // a new base label even though the original base label is still
        // in-use.
        if (label == null) {
            sessionLabel = null;
        } else {
            FemLabel parentLabel = label.getParentLabel();
            while (parentLabel != null) {
                label = parentLabel;
                parentLabel = label.getParentLabel();
            }
            sessionLabel =
                new SessionLabel(
                    label.getCommitSequenceNumber(),
                    label.getCreationTimestamp());
        }

        if (sessionLabel != null) {
            // Lock the new label
            Set<String> mofId = new HashSet<String>();
            CwmModelElement refObj = (CwmModelElement) label;
            mofId.add(refObj.refMofId());
            ddlLockManager.addObjectsInUse(this, mofId);
            tracer.info(
                "Session label set to \"" + label.getName() + "\"");
        }
    }

    // implement FarragoSession
    public Long getSessionLabelCsn()
    {
        if (sessionLabel == null) {
            return null;
        } else {
            return sessionLabel.getCommitSequenceNumber();
        }
    }

    // implement FarragoSession
    public Timestamp getSessionLabelCreationTimestamp()
    {
        if (sessionLabel == null) {
            return null;
        } else {
            return Timestamp.valueOf(sessionLabel.getCreationTimestamp());
        }
    }

    /**
     * @return true if the session has a label setting and it is not temporarily
     * disabled
     */
    private boolean isSessionLabelEnabled()
    {
        return (sessionLabel != null);
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


    // implement FarragoSession
    public FarragoDataWrapperCache newFarragoDataWrapperCache(
        FarragoAllocationOwner owner,
        FarragoObjectCache sharedCache,
        FarragoRepos repos,
        FennelDbHandle fennelDbHandle,
        DataSource loopbackDataSource)
    {
        return new FarragoDataWrapperCache(
            owner,
            sharedCache,
            getPluginClassLoader(),
            repos,
            fennelDbHandle,
            loopbackDataSource);
    }

    Map<String, FarragoObjectCache.Entry> getTxnCodeCache()
    {
        return txnCodeCache;
    }

    /**
     * Accessor for the fennel Txn Context in this session.  If no transaction
     * is active, this should return null
     * @return the FennelTxnContext active in this DB session
     */
    public FennelTxnContext getFennelTxnContext()
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

        FarragoReposTxnContext txn = repos.newTxnContext(true);
        txn.beginReadTxn();
        try {
            sessionIndexMap.onCommit();
        } finally {
            txn.commit();
        }
    }

    void rollbackImpl()
    {
        tracer.info("rollback");
        if (fennelTxnContext != null) {
            fennelTxnContext.rollback();
        }
        onEndOfTransaction(FarragoSessionTxnEnd.ROLLBACK);
    }

    protected void onEndOfTransaction(
        FarragoSessionTxnEnd eot)
    {
        if (txnIdRef.txnId != null) {
            getTxnMgr().endTxn(txnIdRef.txnId, eot);
            txnIdRef.txnId = null;
        }
        savepointList.clear();
        for (FarragoObjectCache.Entry o : txnCodeCache.values()) {
            // REVIEW jvs 26-Nov-2006:  for pinned ExecStreamGraphs
            // (and maybe other statement-related resources) can
            // we verify that they are no longer in use?
            o.closeAllocation();
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
        Map<String, ?> argMap)
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
            new FarragoDbSavepoint(
                nextSavepointId++,
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
        int iSavepoint = findSavepoint(dbSavepoint);
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
            FarragoDbSavepoint savepoint = savepointList.get(i);
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

    private int findSavepoint(FarragoDbSavepoint savepoint)
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
        FarragoDbSavepoint savepoint = savepointList.get(iSavepoint);
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

        // The local variable is necessary to insure that the repository
        // session can be closed if the statement causes a shutdown.
        final FarragoRepos repos = this.repos;
        repos.beginReposSession();
        reposSessionEnded = false;

        FarragoReposTxnContext reposTxnContext =
            new FarragoReposTxnContext(
                repos,
                false,
                getDatabase().isPartiallyRestored());


        boolean [] pRollback = { true };
        FarragoSessionStmtValidator stmtValidator = newStmtValidator();
        if (stmtContext != null) {
            stmtValidator.setWarningQueue(stmtContext.getWarningQueue());
        }
        stmtValidator.setTimingTracer(timingTracer);

        // Pass the repos txn context to the statement validator so
        // the parser can access it and start the appropriate type of
        // repository transaction (for DDL vs not DDL)
        stmtValidator.setReposTxnContext(reposTxnContext);

        FarragoSessionExecutableStmt stmt = null;
        try {
            stmt =
                prepareImpl(
                    sql,
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
            // when we release the catalog lock and lock the objects;
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
            reposTxnContext.unlockAfterTxn();
            if (!reposSessionEnded) {
                repos.endReposSession();
            }
        }
        timingTracer.traceTime("end prepare");
        return stmt;
    }

    private FarragoSessionExecutableStmt prepareImpl(
        String sql,
        FarragoAllocationOwner owner,
        boolean isExecDirect,
        FarragoSessionAnalyzedSql analyzedSql,
        FarragoDbStmtContextBase stmtContext,
        FarragoSessionStmtValidator stmtValidator,
        FarragoReposTxnContext reposTxnContext,
        boolean [] pRollback)
    {
        // REVIEW: May need to disallow some types of prepared DDL.
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
            FarragoSessionExecutableStmt stmt =
                database.prepareStmt(
                    stmtContext,
                    stmtValidator,
                    sqlNode,
                    owner,
                    analyzedSql);
            if (isExecDirect) {
                if (stmt.getDynamicParamRowType().getFieldList().size() > 0) {
                    owner.closeAllocation();
                    throw FarragoResource.instance()
                    .SessionNoExecuteImmediateParameters.ex(sql);
                }

                // DML statements are disallowed if a session label is set.
                // For CALL statements, the contents of the UDP determines
                // whether the call can be executed.
                if (stmt.isDml()
                    && (stmt.getTableModOp() != null)
                    && isSessionLabelEnabled())
                {
                    owner.closeAllocation();
                    throw FarragoResource.instance().ReadOnlySession.ex();
                }
            }
            return stmt;
        }

        FarragoSessionDdlStmt ddlStmt = (FarragoSessionDdlStmt) parsedObj;
        ddlStmt.setRootStmtContext(stmtContext);

        // DDL statements are disallowed if a session label is set.  The
        // exceptions are the SET statements that only impact the current
        // session.
        if (isSessionLabelEnabled()
            && !((ddlStmt instanceof DdlSetContextStmt)
                || (ddlStmt instanceof DdlSetSessionParamStmt)
                || (ddlStmt instanceof DdlSetSessionImplementationStmt)))
        {
            throw FarragoResource.instance().ReadOnlySession.ex();
        }

        // If !isExecDirect and we don't need to validate on prepare, then
        // we only need to parse the statement
        if (!isExecDirect
            && !stmtValidator.getSession().getSessionVariables().getBoolean(
                FarragoDefaultSessionPersonality.VALIDATE_DDL_ON_PREPARE))
        {
            if (ddlStmt.requiresCommit()) {
                commitImpl();
            }
            return null;
        }

        if (ddlStmt.runsAsDml()) {
            markTableInUse(stmtContext, ddlStmt);
        }

        validateDdl(ddlValidator, stmtContext, reposTxnContext, ddlStmt);

        stmtValidator.getTimingTracer().traceTime("end DDL validation");

        // Now that we've validated, we shouldn't continue with execution
        // when !isExecDirect
        if (!isExecDirect) {
            if (ddlStmt.requiresCommit()) {
                commitImpl();
            }
            return null;
        }

        executeDdl(ddlValidator, reposTxnContext, ddlStmt);

        stmtValidator.getTimingTracer().traceTime("end DDL execution");

        pRollback[0] = false;
        return null;
    }

    private void validateDdl(
        FarragoSessionDdlValidator ddlValidator,
        FarragoDbStmtContextBase stmtContext,
        FarragoReposTxnContext reposTxnContext,
        FarragoSessionDdlStmt ddlStmt)
    {
        if (ddlStmt.requiresCommit()) {
            // most DDL causes implicit commit of any pending txn
            commitImpl();
        }
        tracer.fine("validating DDL");
        if (ddlStmt.runsAsDml()) {
            accessTargetTable(stmtContext, ddlStmt);
        }
        if (ddlStmt.requiresCommit()) {
            // start a Fennel txn to cover any effects on storage
            fennelTxnContext.initiateTxn();
        }
        boolean rollbackFennel = true;
        try {
            ddlValidator.validate(ddlStmt);
            rollbackFennel = false;
        } finally {
            if (rollbackFennel) {
                rollbackImpl();
            }
        }
    }

    /**
     * Marks the target table for a DDL statement as in-use. This allows us to
     * subsequently release the MDR repository lock acquired at the start of
     * query preparation. Marking the table as in-use prevents it from being
     * dropped while the repository is unlocked. The lock is released in {@link
     * DdlExecutionVisitor} during the execution of some types of long-running
     * DdlStmt.
     *
     * @param stmtContext context of the DDL statement
     * @param ddlStmt the DDL statement
     */
    private void markTableInUse(
        FarragoDbStmtContextBase stmtContext,
        FarragoSessionDdlStmt ddlStmt)
    {
        // Mark the table as in use, then unlock the repository
        CwmModelElement table = ddlStmt.getModelElement();
        stmtContext.lockObjectInUse(table.refMofId());
    }

    /**
     * Accesses the target table of a DDL statement for write to prevent
     * concurrent DML on the same table.
     *
     * @param stmtContext context of the DDL statement
     * @param ddlStmt the DDL statement
     */
    private void accessTargetTable(
        FarragoDbStmtContextBase stmtContext,
        FarragoSessionDdlStmt ddlStmt)
    {
        // Try to lock the target table
        List<String> names = new ArrayList<String>(3);
        CwmModelElement table = ddlStmt.getModelElement();
        names.add(table.getName());
        for (
            CwmNamespace ns = table.getNamespace();
            ns != null;
            ns = ns.getNamespace())
        {
            names.add(ns.getName());
        }
        Collections.reverse(names);

        boolean success = false;
        try {
            stmtContext.accessTable(
                names,
                TableAccessMap.Mode.READWRITE_ACCESS);
            success = true;
        } finally {
            if (!success) {
                // Abort the txn that was implicitly started to acquire the
                // table lock, if we couldn't acquire the lock
                stmtContext.getSession().endTransactionIfAuto(false);
            }
        }
    }

    private void executeDdl(
        FarragoSessionDdlValidator ddlValidator,
        FarragoReposTxnContext reposTxnContext,
        FarragoSessionDdlStmt ddlStmt)
    {
        tracer.fine("updating storage");

        boolean rollbackFennel = true;
        try {
            ddlValidator.executeStorage();
            ddlStmt.preExecute();
            if (ddlStmt instanceof DdlStmt) {
                ((DdlStmt) ddlStmt).visit(
                    new DdlExecutionVisitor(reposTxnContext, ddlValidator));
            }
            ddlStmt.postExecute();

            tracer.fine("committing DDL");
            reposTxnContext.commit();
            commitImpl();
            rollbackFennel = false;
            ddlStmt.postCommit(ddlValidator);

            if (shutDownRequested) {
                repos.endReposSession();
                reposSessionEnded = true;

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

    /**
     * Turns on a flag indicating whether a shutdown request has been made.
     *
     * @param val whether to set the flag to true or false
     */
    public void setShutdownRequest(boolean val)
    {
        shutdownRequested = val;
    }

    /**
     * @return true if a shutdown has been requested
     */
    public boolean shutdownRequested()
    {
        return shutdownRequested;
    }

    // implement FarragoSession
    public void setLoopback()
    {
        isLoopback = true;
    }

    // implement FarragoSession
    public boolean isLoopback()
    {
        return isLoopback;
    }

    // implement FarragoSession
    public boolean isReentrantAlterTableRebuild()
    {
        return (getSessionIndexMap().getReloadTable() != null)
            && !isReentrantAlterTableAddColumn();
    }

    // implement FarragoSession
    public boolean isReentrantAlterTableAddColumn()
    {
        return (getSessionIndexMap().getOldTableStructure() != null);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class DdlExecutionVisitor
        extends DdlVisitor
    {
        private final FarragoReposTxnContext reposTxnContext;
        private final FarragoSessionDdlValidator ddlValidator;

        private DdlExecutionVisitor(
            FarragoReposTxnContext reposTxnContext,
            FarragoSessionDdlValidator ddlValidator)
        {
            this.reposTxnContext = reposTxnContext;
            this.ddlValidator = ddlValidator;
        }

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
        public void visit(DdlSetRoleStmt stmt)
        {
            SqlIdentifier id = stmt.getRoleName();
            sessionVariables.currentRoleName = id.getSimple();
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

        // implement DdlVisitor
        public void visit(DdlDeallocateOldStmt stmt)
        {
            database.deallocateOld();
        }

        // implement DdlVisitor
        public void visit(DdlMultipleTransactionStmt stmt)
        {
            Util.permAssert(
                reposTxnContext.isTxnInProgress(),
                "must be in repos txn");

            boolean readOnly = !stmt.completeRequiresWriteTxn();
            boolean needRecovery = false;

            FarragoSession session = ddlValidator.newReentrantSession();
            try {
                stmt.prepForExecuteUnlocked(ddlValidator, session);

                reposTxnContext.commit();
                reposTxnContext.unlockAfterTxn();

                needRecovery = true;
                stmt.executeUnlocked(ddlValidator, session);
                needRecovery = false;

                reposTxnContext.beginLockedTxn(readOnly);
                stmt.completeAfterExecuteUnlocked(
                    ddlValidator,
                    session,
                    true);
            } finally {
                if (needRecovery) {
                    reposTxnContext.beginLockedTxn(readOnly);
                    stmt.completeAfterExecuteUnlocked(
                        ddlValidator,
                        session,
                        false);
                    reposTxnContext.commit();
                }
                ddlValidator.releaseReentrantSession(session);
            }
        }

        // implement DdlVisitor
        public void visit(DdlSetSessionParamStmt stmt)
        {
            if (stmt.getParamName().equals(
                    FarragoDefaultSessionPersonality.LABEL))
            {
                setSessionLabel(stmt.getLabelParamValue());
            }
        }
    }

    private static class TxnIdRef
    {
        FarragoSessionTxnId txnId;
    }

    private static class SessionLabel
    {
        Long csn;
        String timestamp;

        SessionLabel(Long csn, String timestamp)
        {
            this.csn = csn;
            this.timestamp = timestamp;
        }

        Long getCommitSequenceNumber()
        {
            return csn;
        }

        String getCreationTimestamp()
        {
            return timestamp;
        }
    }
}

// End FarragoDbSession.java

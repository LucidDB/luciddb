/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2003-2007 Disruptive Tech
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
package net.sf.farrago.db;

import com.sun.security.auth.login.*;

import java.io.*;

import java.net.*;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import javax.security.auth.login.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.defimpl.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.ojrex.*;
import net.sf.farrago.plugin.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.oj.rex.*;
import org.eigenbase.rel.*;
import org.eigenbase.reltype.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.sql.validate.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;
import org.eigenbase.util.property.*;
import org.eigenbase.resource.EigenbaseResource;

import org.netbeans.mdr.handlers.*;


/**
 * FarragoDatabase is a top-level singleton representing an instance of a
 * Farrago database engine.
 *
 * <p>NOTE jvs 14-Dec-2005: FarragoDatabase inherits from FarragoDbSingleton for
 * backwards compatibility. This tie may eventually be severed so that multiple
 * instances of FarragoDatabase can be created in the same JVM.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDatabase
    extends FarragoDbSingleton
{
    //~ Instance fields --------------------------------------------------------

    private FarragoRepos systemRepos;
    private FarragoRepos userRepos;
    private FennelDbHandle fennelDbHandle;
    private OJRexImplementorTable ojRexImplementorTable;
    protected FarragoSessionFactory sessionFactory;
    private FarragoPluginClassLoader pluginClassLoader;
    private List<FarragoSessionModelExtension> modelExtensions;
    private FarragoDdlLockManager ddlLockManager;
    private FarragoSessionTxnMgr txnMgr;
    private Configuration authenticationConfig;
    private boolean authenticateLocalConnections = false;

    /**
     * Cache of all sorts of stuff; see <a
     * href="http://wiki.eigenbase.org/FarragoCodeCache">the design docs</a>.
     */
    private FarragoObjectCache codeCache;

    /**
     * File containing trace configuration.
     */
    private File traceConfigFile;

    /**
     * Provides unique identifiers for sessions and statements.
     */
    private AtomicLong uniqueId = new AtomicLong(1);

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a <code>FarragoDatabase</code>.
     *
     * @param sessionFactory factory for various database-level objects
     * @param init whether to initialize the system catalog (the first time the
     * database is started)
     */
    public FarragoDatabase(
        FarragoSessionFactory sessionFactory,
        boolean init)
    {
        if (instance == null) {
            instance = this;
        }
        try {
            FarragoCompoundAllocation startOfWorldAllocation =
                new FarragoCompoundAllocation();
            this.addAllocation(startOfWorldAllocation);

            StringProperty prop = FarragoProperties.instance().traceConfigFile;
            String loggingConfigFile = prop.get();
            if (loggingConfigFile == null) {
                throw FarragoResource.instance().MissingHomeProperty.ex(
                    prop.getPath());
            }
            traceConfigFile = new File(loggingConfigFile);

            dumpTraceConfig();

            this.sessionFactory = sessionFactory;

            // Tell MDR about our plugin ClassLoader so that it can find
            // extension model JMI interfaces in plugin jars.
            pluginClassLoader = new FarragoPluginClassLoader();
            BaseObjectHandler.setClassLoaderProvider(
                new ClassLoaderProvider() {
                    public ClassLoader getClassLoader()
                    {
                        return pluginClassLoader;
                    }

                    public Class defineClass(
                        String className,
                        byte [] classfile)
                    {
                        return null;
                    }
                });

            // Load all model plugin URL's early so that MDR won't try to
            // generate its own bytecode for JMI interfaces.
            loadBootUrls();

            systemRepos = sessionFactory.newRepos(this, false);
            userRepos = systemRepos;
            if (init) {
                FarragoCatalogInit.createSystemObjects(systemRepos);
            }

            // REVIEW:  system/user configuration
            FemFarragoConfig currentConfig = systemRepos.getCurrentConfig();

            tracer.config(
                "java.class.path = "
                + System.getProperty("java.class.path"));

            tracer.config(
                "java.library.path = "
                + System.getProperty("java.library.path"));

            if (systemRepos.isFennelEnabled()) {
                FarragoReposTxnContext txn = systemRepos.newTxnContext();
                try {
                    txn.beginWriteTxn();
                    loadFennel(
                        startOfWorldAllocation,
                        sessionFactory.newFennelCmdExecutor(),
                        init);
                    txn.commit();
                } finally {
                    txn.rollback();
                }
            } else {
                tracer.config("Fennel support disabled");
            }

            long codeCacheMaxBytes = getCodeCacheMaxBytes(currentConfig);
            codeCache =
                new FarragoObjectCache(
                    this,
                    codeCacheMaxBytes,
                    new FarragoLruVictimPolicy());

            ojRexImplementorTable =
                new FarragoOJRexImplementorTable(
                    SqlStdOperatorTable.instance());

            // Create instances of plugin model extensions for shared use
            // by all sessions.
            loadModelPlugins();

            // REVIEW:  sequencing from this point on
            if (currentConfig.isUserCatalogEnabled()) {
                userRepos = sessionFactory.newRepos(this, true);
                if (userRepos.getSelfAsCatalog() == null) {
                    // REVIEW:  request this explicitly?
                    FarragoCatalogInit.createSystemObjects(userRepos);
                }

                // During shutdown, we want to reverse this process, making
                // userRepos revert to systemRepos.  ReposSwitcher takes
                // care of this before userRepos gets closed.
                addAllocation(new ReposSwitcher());
            }

            // Start up timer.  This comes last so that the first thing we do
            // in close is to cancel it, avoiding races with other shutdown
            // activity.
            Timer timer = new Timer("Farrago Watchdog Timer");
            new FarragoTimerAllocation(this, timer);
            timer.schedule(new WatchdogTask(),
                1000,
                1000);

            if (currentConfig.getCheckpointInterval() > 0) {
                long checkpointIntervalMillis =
                    currentConfig.getCheckpointInterval();
                checkpointIntervalMillis *= 1000;
                timer.schedule(
                    new CheckpointTask(),
                    checkpointIntervalMillis,
                    checkpointIntervalMillis);
            }

            ddlLockManager = new FarragoDdlLockManager();
            txnMgr = sessionFactory.newTxnMgr();
            sessionFactory.specializedInitialization(this);

            File jaasConfigFile =
                new File(
                    FarragoProperties.instance().homeDir.get(),
                    "plugin/jaas.config");
            if (jaasConfigFile.exists()) {
                System.setProperty(
                    "java.security.auth.login.config",
                    jaasConfigFile.getPath());
                authenticationConfig = new ConfigFile();
            }
        } catch (Throwable ex) {
            tracer.throwing("FarragoDatabase", "<init>", ex);
            close(true);
            throw FarragoResource.instance().DatabaseLoadFailed.ex(ex);
        }
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * @return the shared code cache for this database
     */
    public FarragoObjectCache getCodeCache()
    {
        return codeCache;
    }

    /**
     * @return the shared data wrapper cache for this database
     */
    public FarragoObjectCache getDataWrapperCache()
    {
        // data wrapper caching is actually unified with the code cache
        return codeCache;
    }

    /**
     * @return ClassLoader for loading plugins
     */
    public FarragoPluginClassLoader getPluginClassLoader()
    {
        return pluginClassLoader;
    }

    /**
     * @return list of installed {@link FarragoSessionModelExtension} instances
     */
    public List<FarragoSessionModelExtension> getModelExtensions()
    {
        return modelExtensions;
    }

    /**
     * @return transaction manager for this database
     */
    public FarragoSessionTxnMgr getTxnMgr()
    {
        return txnMgr;
    }

    private File getBootUrlFile()
    {
        return new File(
            FarragoProperties.instance().getCatalogDir(),
            "FarragoBootUrls.lst");
    }

    private void loadBootUrls()
    {
        FileReader fileReader;
        try {
            fileReader = new FileReader(getBootUrlFile());
        } catch (FileNotFoundException ex) {
            // if file doesn't exist, it's safe to assume that there
            // are no model plugins yet
            return;
        }
        LineNumberReader lineReader = new LineNumberReader(fileReader);
        try {
            for (;;) {
                String line = lineReader.readLine();
                if (line == null) {
                    break;
                }
                URL url =
                    new URL(
                        FarragoProperties.instance().expandProperties(line));
                pluginClassLoader.addPluginUrl(url);
            }
        } catch (Throwable ex) {
            throw FarragoResource.instance().CatalogBootUrlReadFailed.ex(ex);
        }
    }

    void saveBootUrl(String url)
    {
        // append
        FileWriter fileWriter = null;
        try {
            fileWriter =
                new FileWriter(
                    getBootUrlFile(),
                    true);
            PrintWriter pw = new PrintWriter(fileWriter);
            pw.println(url);
            pw.close();
            fileWriter.close();
        } catch (Throwable ex) {
            throw FarragoResource.instance().CatalogBootUrlUpdateFailed.ex(ex);
        } finally {
            Util.squelchWriter(fileWriter);
        }
    }

    private void loadModelPlugins()
    {
        List<ResourceBundle> resourceBundles = new ArrayList<ResourceBundle>();
        sessionFactory.defineResourceBundles(resourceBundles);

        modelExtensions = new ArrayList<FarragoSessionModelExtension>();
        Collection<FemJar> allJars = systemRepos.allOfClass(FemJar.class);
        for (FemJar jar : allJars) {
            if (jar.isModelExtension()) {
                FarragoSessionModelExtension modelExtension =
                    sessionFactory.newModelExtension(
                        pluginClassLoader,
                        jar);
                modelExtensions.add(modelExtension);
                modelExtension.defineResourceBundles(resourceBundles);
            }
        }

        // add repository localization for model extensions
        systemRepos.addResourceBundles(resourceBundles);
    }

    public void close(boolean suppressExcns)
    {
        try {
            // This will close (in reverse order) all the FarragoAllocations
            // opened by the constructor.
            synchronized (this) {
                closeAllocation();
            }
            assertNoFennelHandles();
        } catch (Throwable ex) {
            warnOnClose(ex, suppressExcns);
        }

        fennelDbHandle = null;
        systemRepos = null;
        userRepos = null;
    }

    private void warnOnClose(
        Throwable ex,
        boolean suppressExcns)
    {
        tracer.warning(
            "Caught " + ex.getClass().getName()
            + " during database shutdown:" + ex.getMessage());
        if (!suppressExcns) {
            tracer.log(Level.SEVERE, "warnOnClose", ex);
            tracer.throwing("FarragoDatabase", "warnOnClose", ex);
            throw Util.newInternal(ex);
        }
    }

    private void dumpTraceConfig()
    {
        try {
            FileReader fileReader = new FileReader(traceConfigFile);
            StringWriter stringWriter = new StringWriter();
            FarragoUtil.copyFromReaderToWriter(fileReader, stringWriter);
            tracer.config(stringWriter.toString());
        } catch (IOException ex) {
            tracer.severe(
                "Caught IOException while dumping trace configuration:  "
                + ex.getMessage());
        }
    }

    private void assertNoFennelHandles()
    {
        assert systemRepos != null : "FarragoDatabase.systemRepos is "
            + "null: server has probably already been started";
        if (!systemRepos.isFennelEnabled()) {
            return;
        }
        int n = FennelStorage.getHandleCount();
        assert (n == 0) : "FennelStorage.getHandleCount() == " + n;
    }

    private void loadFennel(
        FarragoCompoundAllocation startOfWorldAllocation,
        FennelCmdExecutor cmdExecutor,
        boolean init)
    {
        tracer.fine("Loading Fennel");
        assertNoFennelHandles();
        FemCmdOpenDatabase cmd = systemRepos.newFemCmdOpenDatabase();
        FemFennelConfig fennelConfig =
            systemRepos.getCurrentConfig().getFennelConfig();
        SortedMap<String, Object> configMap =
            JmiUtil.getAttributeValues(fennelConfig);

        // Filter out null values.
        Iterator<Map.Entry<String,Object>> configMapIter =
            configMap.entrySet().iterator();
        while (configMapIter.hasNext()) {
            Map.Entry<String,Object> entry = configMapIter.next();
            if (entry.getValue() == null) {
                configMapIter.remove();
            }
        }

        // Copy config into a properties object, then tell the session mgr
        // about them. Note that some of the properties may be non-Strings.
        Properties properties = new Properties();
        properties.putAll(configMap);
        sessionFactory.applyFennelExtensionParameters(properties);

        // The applyFennelExtensionParameters method may have modified the
        // properties, so copy them back.
        for (
            Map.Entry<String, String> entry : Util.toMap(properties).entrySet())
        {
            configMap.put(entry.getKey(), entry.getValue());
        }

        for (Map.Entry<String, Object> entry : configMap.entrySet()) {
            String expandedValue =
                FarragoProperties.instance().expandProperties(
                    entry.getValue().toString());

            FemDatabaseParam param = systemRepos.newFemDatabaseParam();
            param.setName(entry.getKey());
            param.setValue(expandedValue);
            cmd.getParams().add(param);
        }

        // databaseDir is set dynamically, allowing the catalog
        // to be moved
        FemDatabaseParam param1 = systemRepos.newFemDatabaseParam();
        param1.setName("databaseDir");
        param1.setValue(
            FarragoProperties.instance().getCatalogDir().getAbsolutePath());
        cmd.getParams().add(param1);

        for (FemDatabaseParam param : cmd.getParams()) {
            // REVIEW:  use Fennel tracer instead?
            tracer.config(
                "Fennel parameter " + param.getName() + "="
                + param.getValue());
        }

        cmd.setCreateDatabase(init);

        NativeTrace.createInstance("net.sf.fennel.");

        fennelDbHandle =
            new FennelDbHandle(systemRepos,
                this,
                cmdExecutor,
                cmd);

        tracer.config("Fennel successfully loaded");
    }

    /**
     * @return shared OpenJava implementation table for SQL operators
     */
    public OJRexImplementorTable getOJRexImplementorTable()
    {
        return ojRexImplementorTable;
    }

    /**
     * @return system repos for this database
     */
    public FarragoRepos getSystemRepos()
    {
        return systemRepos;
    }

    /**
     * @return user repos for this database
     */
    public FarragoRepos getUserRepos()
    {
        return userRepos;
    }

    /**
     * @return true if there is a JAAS configuration entry for application
     * "Farrago".
     */
    public boolean isAuthenticationEnabled()
    {
        if (authenticationConfig == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     * @return the JAAS authentication configuration.
     */
    public Configuration getAuthenticationConfig()
    {
        return authenticationConfig;
    }

    /**
     * @return the Fennel database handle associated with this database
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    /**
     * @return the DDL lock manager associated with this database
     */
    public FarragoDdlLockManager getDdlLockManager()
    {
        return ddlLockManager;
    }

    /**
     * Gets a unique identifier: never 0.
     *
     * @return next unique identifier
     */
    public long getUniqueId()
    {
        return uniqueId.incrementAndGet();
    }

    // REVIEW mberkowitz 28-Mar-06: Is it better for the FarragoDatabase
    // to save a map (id -> FarragoSessionInfo)
    // and a map (id -> FarragoSessionExecutingStmtInfo)?

    /**
     * look up session info by session id.
     *
     * @param id
     *
     * @return FarragoSessionInfo
     */
    public FarragoSessionInfo findSessionInfo(long id)
    {
        for (FarragoSession s : getSessions(this)) {
            FarragoSessionInfo info = s.getSessionInfo();
            if (info.getId() == id) {
                return info;
            }
        }
        return null;
    }

    /**
     * Looks up executing statement info by statement id.
     *
     * @param id
     *
     * @return FarragoSessionExecutingStmtInfo
     */
    public FarragoSessionExecutingStmtInfo findExecutingStmtInfo(long id)
    {
        for (FarragoSession s : getSessions(this)) {
            FarragoSessionExecutingStmtInfo info =
                s.getSessionInfo().getExecutingStmtInfo(id);
            if (info != null) {
                return info;
            }
        }
        return null;
    }

    /**
     * Kills a session.
     *
     * @param id session identifier
     * @param cancelOnly if true, just cancel current execution; if false,
     * destroy session
     */
    public void killSession(long id, boolean cancelOnly)
        throws Throwable
    {
        tracer.info("killSession " + id);
        FarragoSessionInfo info = findSessionInfo(id);
        if (info == null) {
            throw FarragoResource.instance().SessionNotFound.ex(id);
        }
        FarragoDbSession target = (FarragoDbSession) info.getSession();
        if (target.isClosed()) {
            tracer.info("killSession " + id + ": already closed");
            return;
        }
        if (cancelOnly) {
            target.cancel();
        } else {
            target.kill();
        }
    }

    private void kill(FarragoSessionExecutingStmtInfo info, boolean cancelOnly)
        throws Throwable
    {
        FarragoSessionStmtContext stmt = info.getStmtContext();
        if (stmt == null) {
            Long id = info.getId();
            tracer.info("killExecutingStmt " + id + ": statement not found");
            throw new Throwable("executing statement not found: " + id); // i18n
        }
        if (tracer.isLoggable(Level.INFO)) {
            tracer.info(
                "killStatement " + info.getId()
                + "(session " + stmt.getSession().getSessionInfo().getId()
                + "), "
                + stmt.getSql());
        }
        if (cancelOnly) {
            stmt.cancel();
        } else {
            stmt.closeAllocation();
        }
    }

    /**
     * Kill an executing statement: cancel it and deallocate it.
     *
     * @param id statement id
     * @param cancelOnly if true, just cancel current execution; if false,
     * destroy statement
     */
    public void killExecutingStmt(long id, boolean cancelOnly)
        throws Throwable
    {
        tracer.info("killExecutingStmt " + id);
        FarragoSessionExecutingStmtInfo info = findExecutingStmtInfo(id);
        if (info == null) {
            tracer.info("killExecutingStmt " + id + ": statement not found");
            throw new Throwable("executing statement not found: " + id); // i18n
        }
        kill(info, cancelOnly);
    }

    /**
     * Kills all statements that are executing SQL that matches a given pattern,
     * but does not match a second pattern. Not an error if none match.
     *
     * @param match pattern to match. Null string matches nothing, to be safe.
     * @param nomatch pattern not to match
     * @param cancelOnly if true, just cancel current execution; if false,
     * destroy statement
     *
     * @return count of killed statements.
     */
    public int killExecutingStmtMatching(
        String match,
        String nomatch,
        boolean cancelOnly)
        throws Throwable
    {
        int ct = 0;
        tracer.info(
            "killExecutingStmtMatching " + match + " but not " + nomatch);

        // scan all statements
        if (match.length() > 0) {
            for (FarragoSession sess : getSessions(this)) {
                FarragoSessionInfo sessInfo = sess.getSessionInfo();
                for (Long id : sessInfo.getExecutingStmtIds()) {
                    FarragoSessionExecutingStmtInfo info =
                        sessInfo.getExecutingStmtInfo(id);
                    if (info.getSql().contains(nomatch)) {
                        continue;
                    }
                    if (info.getSql().contains(match)) {
                        kill(info, cancelOnly);
                        ct++;
                    }
                }
            }
        }
        tracer.info("killed " + ct + " statements");
        return ct;
    }

    /**
     * Prepares an SQL expression; uses a cached implementation if available,
     * otherwise caches the one generated here.
     *
     * @param stmtContext embracing stmt context
     * @param stmtValidator generic stmt validator
     * @param sqlNode the parsed form of the statement
     * @param owner the FarragoAllocationOwner which will be responsible for the
     * returned stmt
     * @param analyzedSql receives information about a prepared expression
     *
     * @return statement implementation, or null when analyzedSql is non-null
     */
    public FarragoSessionExecutableStmt prepareStmt(
        FarragoSessionStmtContext stmtContext,
        FarragoSessionStmtValidator stmtValidator,
        SqlNode sqlNode,
        FarragoAllocationOwner owner,
        FarragoSessionAnalyzedSql analyzedSql)
    {
        final FarragoSessionPreparingStmt stmt =
            stmtValidator.getSession().getPersonality().newPreparingStmt(
                stmtContext,
                stmtValidator);
        return prepareStmtImpl(stmt, sqlNode, owner, analyzedSql);
    }

    /**
     * Implements a logical or physical query plan but does not execute it.
     *
     * @param prep the FarragoSessionPreparingStmt that is managing the query.
     * @param rootRel root of query plan (relational expression)
     * @param sqlKind SqlKind for the relational expression: only
     * SqlKind.Explain and SqlKind.Dml are special cases.
     * @param logical true for a logical query plan (still needs to be
     * optimized), false for a physical plan.
     * @param owner the FarragoAllocationOwner which will be responsible for the
     * returned stmt
     *
     * @return statement implementation
     */
    public FarragoSessionExecutableStmt implementStmt(
        FarragoSessionPreparingStmt prep,
        RelNode rootRel,
        SqlKind sqlKind,
        boolean logical,
        FarragoAllocationOwner owner)
    {
        try {
            FarragoSessionExecutableStmt executable =
                prep.implement(rootRel, sqlKind, logical);
            owner.addAllocation(executable);
            return executable;
        } finally {
            prep.closeAllocation();
        }
    }

    private FarragoSessionExecutableStmt prepareStmtImpl(
        final FarragoSessionPreparingStmt stmt,
        final SqlNode sqlNode,
        FarragoAllocationOwner owner,
        FarragoSessionAnalyzedSql analyzedSql)
    {
        final EigenbaseTimingTracer timingTracer =
            stmt.getStmtValidator().getTimingTracer();
        final FarragoRepos stmtRepos = stmt.getRepos();

        // REVIEW jvs 27-Aug-2005:  what are the security implications of
        // EXPLAIN PLAN?

        // It would be silly to cache EXPLAIN PLAN results, so deal with them
        // directly. Also check for whether statement caching is turned off
        // for the session before continuing.
        boolean cacheStatements =
            stmt.getSession().getSessionVariables().getBoolean(
                FarragoDefaultSessionPersonality.CACHE_STATEMENTS);
        if (sqlNode.isA(SqlKind.Explain) || (cacheStatements == false)) {
            FarragoSessionExecutableStmt executableStmt =
                stmt.prepare(sqlNode, sqlNode);
            owner.addAllocation(executableStmt);
            return executableStmt;
        }

        // Use unparsed validated SQL as cache key.  This eliminates trivial
        // differences such as whitespace and implicit qualifiers.
        SqlValidator sqlValidator = stmt.getSqlValidator();
        final SqlNode validatedSqlNode;
        if ((analyzedSql != null) && (analyzedSql.paramRowType != null)) {
            Map<String, RelDataType> nameToTypeMap =
                new HashMap<String, RelDataType>();
            for (
                RelDataTypeField field
                : analyzedSql.paramRowType.getFieldList())
            {
                nameToTypeMap.put(
                    field.getName(),
                    field.getType());
            }
            validatedSqlNode =
                sqlValidator.validateParameterizedExpression(
                    sqlNode,
                    nameToTypeMap);
        } else {
            validatedSqlNode = sqlValidator.validate(sqlNode);
        }

        stmt.postValidate(validatedSqlNode);

        timingTracer.traceTime("end validation");

        SqlDialect sqlDialect =
            new SqlDialect(stmt.getSession().getDatabaseMetaData());
        final String sql = validatedSqlNode.toSqlString(sqlDialect);

        if (analyzedSql != null) {
            if (validatedSqlNode instanceof SqlSelect) {
                // assume we're validating a view
                SqlSelect select = (SqlSelect) validatedSqlNode;
                if (select.getOrderList() != null) {
                    analyzedSql.hasTopLevelOrderBy = true;
                }
            }

            // Need to force preparation so we can dig out required info, so
            // don't use cache.  Also, don't need to go all the way with
            // stmt implementation either; can stop after validation, which
            // provides needed metadata.  (In fact, we CAN'T go much further,
            // because if a view is being created as part of a CREATE SCHEMA
            // statement, some of the tables it depends on may not have
            // storage defined yet.)
            analyzedSql.canonicalString = sql;
            stmt.analyzeSql(validatedSqlNode, analyzedSql);

            timingTracer.traceTime("end analyzeSql");

            return null;
        }

        FarragoObjectCache.Entry cacheEntry;
        FarragoObjectCache.CachedObjectFactory stmtFactory =
            new FarragoObjectCache.CachedObjectFactory() {
                public void initializeEntry(
                    Object key,
                    FarragoObjectCache.UninitializedEntry entry)
                {
                    timingTracer.traceTime("code cache miss");

                    assert (key.equals(sql));
                    FarragoSessionExecutableStmt executableStmt =
                        stmt.prepare(validatedSqlNode, sqlNode);
                    long memUsage =
                        FarragoUtil.getStringMemoryUsage(sql)
                        + executableStmt.getMemoryUsage();
                    entry.initialize(
                        executableStmt,
                        memUsage,
                        stmt.mayCacheImplementation());
                }
                
                public boolean isStale(Object value)
                {
                    FarragoSessionExecutableStmt executableStmt =
                        (FarragoSessionExecutableStmt) value;
                    return isExecutableStmtStale(
                        stmtRepos,
                        executableStmt);
                }
            };

        // sharing of executable statements depends on session personality;
        // default for vanilla Farrago personality is that statements
        // are sharable
        final boolean sharable = 
            stmt.getSession().getPersonality().supportsFeature(
                EigenbaseResource.instance().SharedStatementPlans);

        // prepare the statement, caching the results in codeCache
        cacheEntry = codeCache.pin(sql, stmtFactory, !sharable);
        FarragoSessionExecutableStmt executableStmt =
            (FarragoSessionExecutableStmt) cacheEntry.getValue();
        owner.addAllocation(cacheEntry);
        return executableStmt;
    }

    private boolean isExecutableStmtStale(
        FarragoRepos repos,
        FarragoSessionExecutableStmt stmt)
    {
        for (String mofid : stmt.getReferencedObjectIds()) {
            RefBaseObject obj = repos.getMdrRepos().getByMofId(mofid);
            if (obj == null) {
                // the object was deleted
                return true;
            }
            if (obj instanceof FemAnnotatedElement) {
                String cachedModTime = stmt.getReferencedObjectModTime(mofid);
                if (cachedModTime == null) {
                    continue;
                }
                FemAnnotatedElement annotated = (FemAnnotatedElement) obj;
                String lastModTime = annotated.getModificationTimestamp();
                if (!cachedModTime.equals(lastModTime)) {
                    // the object was modified
                    return true;
                }
            }
        }
        return false;
    }

    public void updateSystemParameter(DdlSetSystemParamStmt ddlStmt)
    {
        // TODO:  something cleaner
        boolean setCodeCacheSize = false;

        String paramName = ddlStmt.getParamName();

        if (paramName.equals("calcVirtualMachine")) {
            // sanity check
            if (!userRepos.isFennelEnabled()) {
                CalcVirtualMachine vm =
                    userRepos.getCurrentConfig().getCalcVirtualMachine();
                if (vm.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
                    throw FarragoResource.instance().ValidatorCalcUnavailable
                    .ex();
                }
            }

            // when this parameter changes, we need to clear the code cache,
            // since cached plans may be based on the old setting
            codeCache.setMaxBytes(0);

            // this makes sure that we reset the cache to the correct size
            // below
            setCodeCacheSize = true;
        }

        if (paramName.equals("codeCacheMaxBytes")) {
            setCodeCacheSize = true;
        }

        if (setCodeCacheSize) {
            codeCache.setMaxBytes(
                getCodeCacheMaxBytes(systemRepos.getCurrentConfig()));
        }

        if (paramName.equals("cachePagesInit")
            || paramName.equals("expectedConcurrentStatements")
            || paramName.equals("cacheReservePercentage"))
        {
            executeFennelSetParam(paramName, ddlStmt.getParamValue());
        }
    }

    private long getCodeCacheMaxBytes(FemFarragoConfig config)
    {
        long codeCacheMaxBytes = config.getCodeCacheMaxBytes();
        if (codeCacheMaxBytes == -1) {
            codeCacheMaxBytes = Long.MAX_VALUE;
        }
        return codeCacheMaxBytes;
    }

    private void executeFennelSetParam(String paramName, SqlLiteral paramValue)
    {
        if (!systemRepos.isFennelEnabled()) {
            return;
        }

        FemCmdSetParam cmd = systemRepos.newFemCmdSetParam();
        cmd.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
        FemDatabaseParam param = systemRepos.newFemDatabaseParam();
        param.setName(paramName);
        param.setValue(paramValue.toString());
        cmd.setParam(param);
        fennelDbHandle.executeCmd(cmd);
    }

    public void requestCheckpoint(
        boolean fuzzy,
        boolean async)
    {
        if (!systemRepos.isFennelEnabled()) {
            return;
        }

        FemCmdCheckpoint cmd = systemRepos.newFemCmdCheckpoint();
        cmd.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
        cmd.setFuzzy(fuzzy);
        cmd.setAsync(async);
        fennelDbHandle.executeCmd(cmd);
    }

    public void deallocateOld()
    {
        if (!systemRepos.isFennelEnabled()) {
            return;
        }
        FemCmdAlterSystemDeallocate cmd =
            systemRepos.newFemCmdAlterSystemDeallocate();
        cmd.setDbHandle(fennelDbHandle.getFemDbHandle(systemRepos));
        fennelDbHandle.executeCmd(cmd);
    }

    public static FarragoSessionFactory newSessionFactory()
    {
        String libraryName =
            FarragoProperties.instance().defaultSessionFactoryLibraryName.get();
        try {
            FarragoPluginClassLoader classLoader =
                new FarragoPluginClassLoader();
            Class c =
                classLoader.loadClassFromLibraryManifest(
                    libraryName,
                    "SessionFactoryClassName");
            return (FarragoSessionFactory) classLoader.newPluginInstance(c);
        } catch (Throwable ex) {
            throw FarragoResource.instance().PluginInitFailed.ex(
                libraryName,
                ex);
        }
    }

    /**
     * Main entry point which creates a new Farrago database.
     *
     * @param args ignored
     */
    public static void main(String [] args)
    {
        FarragoDatabase database =
            new FarragoDatabase(
                FarragoDatabase.newSessionFactory(),
                true);
        database.close(false);
    }

    public boolean isAuthenticateLocalConnections()
    {
        return authenticateLocalConnections;
    }

    /**
     * Turns on/off authentication for in-process jdbc sessions. NOTE: If set to
     * off, the origin of the jdbc connection is determined by the
     * remoteProtocol attribute of the jdbc URL.
     *
     * @param authenticateLocalConnections
     */
    public void setAuthenticateLocalConnections(
        boolean authenticateLocalConnections)
    {
        this.authenticateLocalConnections = authenticateLocalConnections;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * 1 Hz task for background activities. Currently all it does is re-read the
     * trace configuration file whenever it changes.
     */
    private class WatchdogTask
        extends FarragoTimerTask
    {
        private long prevTraceConfigTimestamp;

        WatchdogTask()
        {
            super(tracer);
            prevTraceConfigTimestamp = traceConfigFile.lastModified();
        }

        // implement FarragoTimerTask
        protected void runTimer()
        {
            checkTraceConfig();
        }

        private void checkTraceConfig()
        {
            long traceConfigTimestamp = traceConfigFile.lastModified();
            if (traceConfigTimestamp == 0) {
                return;
            }
            if (traceConfigTimestamp > prevTraceConfigTimestamp) {
                prevTraceConfigTimestamp = traceConfigTimestamp;
                tracer.config("Reading modified trace configuration file");
                try {
                    LogManager.getLogManager().readConfiguration();
                } catch (IOException ex) {
                    // REVIEW:  do more?  There's a good chance this will end
                    // up in /dev/null.
                    tracer.severe(
                        "Caught IOException while updating "
                        + "trace configuration:  " + ex.getMessage());
                }
                dumpTraceConfig();
            }
        }
    }

    private class CheckpointTask
        extends FarragoTimerTask
    {
        CheckpointTask()
        {
            super(tracer);
        }
        
        // implement FarragoTimerTask
        public void runTimer()
        {
            requestCheckpoint(true, true);
        }
    }

    private class ReposSwitcher
        implements FarragoAllocation
    {
        public void closeAllocation()
        {
            userRepos = systemRepos;
        }
    }
}

// End FarragoDatabase.java

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
import net.sf.farrago.util.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.ojrex.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.util.*;
import net.sf.saffron.oj.rex.*;
import net.sf.saffron.rel.SaffronRel;

import openjava.tools.DebugOut;

import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

/**
 * FarragoDatabase is a top-level singleton representing an instance of a
 * Farrago database engine.  It is reference-counted to allow it to be shared
 * in a library environment such as the directly embedded JDBC driver.
 * Note that all synchronization is done at the class level, not the
 * object level.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoDatabase
    extends FarragoCompoundAllocation
{
    private static final Logger tracer = FarragoTrace.getDatabaseTracer();

    /**
     * Reference count.
     */
    private static int nReferences;

    /**
     * Singleton instance, or null when nReferences == 0.
     */
    private static FarragoDatabase instance;

    private FarragoCatalog systemCatalog;

    private FarragoCatalog userCatalog;

    private FennelDbHandle fennelDbHandle;

    private OJRexImplementorTable ojRexImplementorTable;

    /**
     * Cache of all sorts of stuff.
     */
    private FarragoObjectCache codeCache;

    /**
     * Cache of FarragoMedDataWrappers.
     */
    private FarragoObjectCache dataWrapperCache;

    /**
     * File containing trace configuration.
     */
    private File traceConfigFile;

    /**
     * Establish a database reference.  If this is the first reference, the
     * database will be loaded first; otherwise, the existing database is
     * reused with an increased reference count.
     *
     * @param sessionFactory factory for various database-level objects
     *
     * @return loaded database
     */
    public static synchronized FarragoDatabase pinReference(
        FarragoSessionFactory sessionFactory)
    {
        tracer.info("connect");

        // Do this first for reentrancy.
        ++nReferences;
        if (nReferences == 1) {
            assert(instance == null);
            boolean success = false;
            try {
                FarragoDatabase newDb = new FarragoDatabase(
                    sessionFactory,false);
                assert(newDb == instance);
                success = true;
            } finally {
                if (!success) {
                    nReferences = 0;
                    instance = null;
                }
            }
        }
        return instance;
    }

    static synchronized void addSession(
        FarragoDatabase db,
        FarragoDbSession session)
    {
        assert(db == instance);
        db.addAllocation(session);
    }

    static synchronized void disconnectSession(FarragoDbSession session)
    {
        tracer.info("disconnect");

        FarragoDatabase db = session.getDatabase();

        assert(nReferences > 0);
        assert(db == instance);

        db.forgetAllocation(session);

        nReferences--;
    }

    /**
     * Conditionally shut down the database depending on the number
     * of references.
     *
     * @param groundReferences threshold for shutdown; if actual number
     * of sessions is greater than this, no shutdown takes place
     *
     * @return whether shutdown took place
     */
    public static synchronized boolean shutdownConditional(
        int groundReferences)
    {
        assert(instance != null);
        tracer.fine("ground reference count = " + groundReferences);
        tracer.fine("actual reference count = " + nReferences);
        if (nReferences <= groundReferences) {
            shutdown();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Shut down the database, killing any running sessions.
     */
    public static synchronized void shutdown()
    {
        tracer.info("shutdown");
        assert(instance != null);
        try {
            instance.close(false);
        } finally {
            instance = null;
            nReferences = 0;
        }
    }

    public static boolean isReferenced()
    {
        if (nReferences > 0) {
            assert(instance != null);
            return true;
        } else {
            assert(instance == null);
            return false;
        }
    }

    /**
     * .
     *
     * @return the shared code cache for this database
     */
    public FarragoObjectCache getCodeCache()
    {
        return codeCache;
    }

    /**
     * .
     *
     * @return the shared data wrapper cache for this database
     */
    public FarragoObjectCache getDataWrapperCache()
    {
        return dataWrapperCache;
    }

    /**
     * Creates a <code>FarragoDatabase</code>.
     *
     * @param sessionFactory factory for various database-level objects
     * @param init whether to initialize the system catalog (the first time
     *     the database is started)
     */
    private FarragoDatabase(FarragoSessionFactory sessionFactory,boolean init)
    {
        instance = this;
        try {
            final String prop = "java.util.logging.config.file";
            String loggingConfigFile =
                    System.getProperties().getProperty(prop);
            if (loggingConfigFile == null) {
                throw FarragoResource.instance().newMissingHomeProperty(prop);
            }
            traceConfigFile = new File(loggingConfigFile);

            dumpTraceConfig();

            systemCatalog = sessionFactory.newCatalog(this,false);
            userCatalog = systemCatalog;
            if (init) {
                systemCatalog.createSystemObjects();
            }

            // REVIEW:  system/user configuration
            FemFarragoConfig currentConfig = systemCatalog.getCurrentConfig();

            tracer.config(
                "java.class.path = "
                + System.getProperty("java.class.path"));

            tracer.config(
                "java.library.path = "
                + System.getProperty("java.library.path"));

            if (systemCatalog.isFennelEnabled()) {
                systemCatalog.beginReposTxn(true);
                try {
                    loadFennel(sessionFactory.newFennelCmdExecutor(),init);
                } finally {
                    systemCatalog.endReposTxn(false);
                }
            } else {
                tracer.config("Fennel support disabled");
            }

            integrateSaffronTracing();

            long codeCacheMaxBytes = getCodeCacheMaxBytes(currentConfig);
            codeCache = new FarragoObjectCache(this,codeCacheMaxBytes);

            // TODO:  parameter for cache size limit
            dataWrapperCache = new FarragoObjectCache(this,Long.MAX_VALUE);

            ojRexImplementorTable =
                new FarragoOJRexImplementorTable(SqlOperatorTable.std());

            // REVIEW:  sequencing from this point on

            if (currentConfig.isUserCatalogEnabled()) {
                userCatalog = new FarragoCatalog(this,true);
                if (userCatalog.getSelfAsCwmCatalog() == null) {
                    // REVIEW:  request this explicitly?
                    userCatalog.createSystemObjects();
                }
                // During shutdown, we want to reverse this process, making
                // userCatalog revert to systemCatalog.  CatalogSwitcher takes
                // care of this before userCatalog gets closed.
                addAllocation(new CatalogSwitcher());
            }

            // Start up timer.  This comes last so that the first thing we do
            // in close is to cancel it, avoiding races with other shutdown
            // activity.
            Timer timer = new Timer();
            new FarragoTimerAllocation(this,timer);
            timer.schedule(new WatchdogTask(),1000,1000);

            if (currentConfig.getCheckpointInterval() > 0) {
                long checkpointIntervalMillis =
                    currentConfig.getCheckpointInterval();
                checkpointIntervalMillis *= 1000;
                timer.schedule(
                    new CheckpointTask(),
                    checkpointIntervalMillis,
                    checkpointIntervalMillis);
            }
        } catch (Throwable ex) {
            tracer.throwing("FarragoDatabase","<init>",ex);
            close(true);
            throw FarragoResource.instance().newDatabaseLoadFailed(ex);
        }
    }

    private void close(boolean suppressExcns)
    {
        try {
            // This will close (in reverse order) all the FarragoAllocations
            // opened by the constructor.
            closeAllocation();
            assertNoFennelHandles();
        } catch (Throwable ex) {
            warnOnClose(ex,suppressExcns);
        }

        fennelDbHandle = null;
        systemCatalog = null;
        userCatalog = null;
    }

    private void warnOnClose(Throwable ex,boolean suppressExcns)
    {
        tracer.warning(
            "Caught " + ex.getClass().getName() + " during database shutdown:"
            + ex.getMessage());
        if (!suppressExcns) {
            tracer.throwing("FarragoDatabase","warnOnClose",ex);
            throw Util.newInternal(ex);
        }
    }

    private void dumpTraceConfig()
    {
        try {
            FileReader fileReader = new FileReader(traceConfigFile);
            StringWriter stringWriter = new StringWriter();
            FarragoUtil.copyFromReaderToWriter(fileReader,stringWriter);
            tracer.config(stringWriter.toString());
        } catch (IOException ex) {
            tracer.severe(
                "Caught IOException while dumping trace configuration:  "
                + ex.getMessage());
        }
    }

    // TODO jvs 4-June-2004:  eliminate this once all Saffron tracing
    // has been rewritten to use java.util.logging
    private void integrateSaffronTracing()
    {
        Logger saffronTrace = Logger.getLogger("net.sf.farrago.saffron");
        if (saffronTrace.isLoggable(Level.FINE)) {
            DebugOut.setDebugLevel(3);
            DebugOut.setDebugOut(
                new LoggingPrintStream(saffronTrace,Level.FINE));
        } else {
            DebugOut.setDebugOut(
                new LoggingPrintStream(saffronTrace,Level.OFF));
        }
    }

    private void assertNoFennelHandles()
    {
        assert systemCatalog != null : "FarragoDatabase.systemCatalog is " +
                "null: server has probably already been started";
        if (!systemCatalog.isFennelEnabled()) {
            return;
        }
        int n = FennelStorage.getHandleCount();
        assert(n == 0);
    }

    private void loadFennel(FennelCmdExecutor cmdExecutor,boolean init)
    {
        tracer.fine("Loading Fennel");
        assertNoFennelHandles();
        FemCmdOpenDatabase cmd =
            systemCatalog.newFemCmdOpenDatabase();
        FemFennelConfig fennelConfig =
            systemCatalog.getCurrentConfig().getFennelConfig();
        Map attributeMap = JmiUtil.getAttributeValues(fennelConfig);
        FemDatabaseParam param;
        Iterator iter = attributeMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();

            String expandedValue = 
                FarragoProperties.instance().expandProperties(
                    entry.getValue().toString());

            param = systemCatalog.newFemDatabaseParam();
            param.setName(entry.getKey().toString());
            param.setValue(expandedValue);
            cmd.getParams().add(param);
        }

        // databaseDir is set dynamically, allowing the catalog
        // to be moved
        param = systemCatalog.newFemDatabaseParam();
        param.setName("databaseDir");
        param.setValue(
            FarragoProperties.instance().getCatalogDir().getAbsolutePath());
        cmd.getParams().add(param);

        iter = cmd.getParams().iterator();
        while (iter.hasNext()) {
            param = (FemDatabaseParam) iter.next();
            // REVIEW:  use Fennel tracer instead?
            tracer.config(
                "Fennel parameter " + param.getName() + "="
                + param.getValue());
        }

        cmd.setCreateDatabase(init);

        NativeTrace nativeTrace =
            new NativeTrace("net.sf.fennel.");

        FennelJavaHandle hNativeTrace =
            FennelDbHandle.allocateNewObjectHandle(this,nativeTrace);
        cmd.setJavaTraceHandle(hNativeTrace.getLongHandle());
        fennelDbHandle = new FennelDbHandle(
            systemCatalog,systemCatalog,this,cmdExecutor,cmd);

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
     * .
     *
     * @return system catalog for this database
     */
    public FarragoCatalog getSystemCatalog()
    {
        return systemCatalog;
    }

    /**
     * .
     *
     * @return user catalog for this database
     */
    public FarragoCatalog getUserCatalog()
    {
        return userCatalog;
    }

    /**
     * .
     *
     * @return the Fennel database handle associated with this database
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelDbHandle;
    }

    /**
     * Prepare a query or DML statement; use a cached implementation if
     * available, otherwise cache the one generated here.
     *
     * @param stmtValidator generic stmt validator
     *
     * @param sqlNode the parsed form of the statement
     *
     * @param owner the FarragoAllocationOwner which will be responsible for
     * the returned stmt
     *
     * @param viewInfo receives information about a prepared view definition
     *
     * @return statement implementation, or null when viewInfo is non-null
     */
    public FarragoSessionExecutableStmt prepareStmt(
        FarragoSessionStmtValidator stmtValidator,
        SqlNode sqlNode,
        FarragoAllocationOwner owner,
        FarragoSessionViewInfo viewInfo)
    {
        final FarragoPreparingStmt stmt =
            new FarragoPreparingStmt(stmtValidator);
        return prepareStmtImpl(
            stmt,sqlNode,owner,viewInfo);
    }


    /**
     * Implement a logical or physical query plan but do not execute it.
     * @param prep the FarragoSessionPreparingStmt that is managing the query.
     * @param rootRel root of query plan (saffron relational expression)
     * @param sqlKind SqlKind for the relational expression: only
     *   SqlKind.Explain and SqlKind.Dml are special cases.
     * @param logical true for a logical query plan (still needs to be
     *   optimized), false for a physical plan.
     * @param owner the FarragoAllocationOwner which will be responsible for
     * the returned stmt
     * @return statement implementation
     */
    public FarragoSessionExecutableStmt implementStmt(
        FarragoSessionPreparingStmt prep,
        SaffronRel rootRel, SqlKind sqlKind, boolean logical,
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
        final FarragoPreparingStmt stmt,
        SqlNode sqlNode,
        FarragoAllocationOwner owner,
        FarragoSessionViewInfo viewInfo)
    {
        // It would be silly to cache EXPLAIN PLAN results, so deal with them
        // directly.
        if (sqlNode.isA(SqlKind.Explain)) {
            FarragoSessionExecutableStmt executableStmt =
                stmt.prepare(sqlNode);
            owner.addAllocation(executableStmt);
            return executableStmt;
        }

        // Use unparsed validated SQL as cache key.  This eliminates trivial
        // differences such as whitespace and implicit qualifiers.

        final SqlNode validatedSqlNode = stmt.validate(sqlNode);

        SqlDialect sqlDialect = new SqlDialect(
            stmt.getSession().getDatabaseMetaData());
        final String sql = validatedSqlNode.toSqlString(sqlDialect);

        if (viewInfo != null) {
            SqlSelect select = (SqlSelect) validatedSqlNode;
            if (select.getOrderList() != null) {
                throw
                    FarragoResource.instance().newValidatorInvalidViewOrderBy();
            }

            // Need to force preparation so we can dig out required info, so
            // don't use cache.  Also, don't need to go all the way with
            // stmt implementation either; can stop after translation, which
            // provides needed metadata.  (In fact, we can't go much further,
            // because if this view is being created as part of a CREATE SCHEMA
            // statement, some of the tables it depends on may not have
            // storage defined yet.)
            viewInfo.validatedSql = sql;
            stmt.prepareViewInfo(validatedSqlNode,viewInfo);
            return null;
        }

        FarragoObjectCache.CachedObjectFactory stmtFactory = new
            FarragoObjectCache.CachedObjectFactory()
            {
                public void initializeEntry(
                    Object key,
                    FarragoObjectCache.UninitializedEntry entry)
                {
                    assert(key.equals(sql));
                    FarragoSessionExecutableStmt executableStmt =
                        stmt.prepare(validatedSqlNode);
                    long memUsage = FarragoUtil.getStringMemoryUsage(sql)
                        + executableStmt.getMemoryUsage();
                    entry.initialize(executableStmt,memUsage);
                }
            };

        FarragoObjectCache.Entry cacheEntry;
        FarragoSessionExecutableStmt executableStmt;

        do {
            cacheEntry = codeCache.pin(sql,stmtFactory,false);
            executableStmt = 
                (FarragoSessionExecutableStmt) cacheEntry.getValue();

            if (isStale(stmt.getCatalog(),executableStmt)) {
                // TODO jvs 17-July-2004: Need DDL-vs-query concurrency control
                // here.  FarragoRuntimeContext needs to acquire DDL-locks on
                // referenced objects so that they cannot be modified/dropped
                // for the duration of execution.
                cacheEntry.closeAllocation();
                codeCache.discard(sql);
                cacheEntry = null;
                executableStmt = null;
            }
        } while (executableStmt == null);
        owner.addAllocation(cacheEntry);
        return executableStmt;
    }

    private boolean isStale(
        FarragoCatalog catalog,
        FarragoSessionExecutableStmt stmt)
    {
        Iterator idIter = stmt.getReferencedObjectIds().iterator();
        while (idIter.hasNext()) {
            String mofid = (String) idIter.next();
            RefBaseObject obj =
                catalog.getRepository().getByMofId(mofid);
            if (obj == null) {
                // TODO jvs 17-July-2004:  Once we support ALTER TABLE, this
                // won't be good enough.  In addition to checking that the
                // object still exists, we'll need to verify that its version
                // number is the same as it was at the time stmt was prepared.
                return true;
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
            if (!userCatalog.isFennelEnabled()) {
                CalcVirtualMachine vm = 
                    userCatalog.getCurrentConfig().getCalcVirtualMachine();
                if (vm.equals(CalcVirtualMachineEnum.CALCVM_FENNEL)) {
                    throw FarragoResource.instance().
                        newValidatorCalcUnavailable();
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
                getCodeCacheMaxBytes(systemCatalog.getCurrentConfig()));
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

    public void requestCheckpoint(boolean fuzzy,boolean async)
    {
        if (!systemCatalog.isFennelEnabled()) {
            return;
        }

        systemCatalog.beginTransientTxn();
        try {
            FemCmdCheckpoint cmd = systemCatalog.newFemCmdCheckpoint();
            cmd.setDbHandle(fennelDbHandle.getFemDbHandle(systemCatalog));
            cmd.setFuzzy(fuzzy);
            cmd.setAsync(async);
            fennelDbHandle.executeCmd(cmd);
        } finally {
            systemCatalog.endTransientTxn();
        }
    }

    /**
     * Main entry point which creates a new Farrago database.
     *
     * @param args ignored
     */
    public static void main(String [] args)
    {
        FarragoDatabase database = new FarragoDatabase(
            new FarragoDbSessionFactory(),
            true);
        database.close(false);
    }

    /**
     * 1 Hz task for background activities.  Currently all it does is re-read
     * the trace configuration file whenever it changes.
     */
    private class WatchdogTask extends TimerTask
    {
        private long prevTraceConfigTimestamp;

        WatchdogTask()
        {
            prevTraceConfigTimestamp = traceConfigFile.lastModified();
        }

        // implement Runnable
        public void run()
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
                        + "trace configuration:  "
                        + ex.getMessage());
                }
                dumpTraceConfig();
            }
        }
    }

    private class CheckpointTask extends TimerTask
    {
        // implement Runnable
        public void run()
        {
            requestCheckpoint(true,true);
        }
    }

    private class CatalogSwitcher implements FarragoAllocation
    {
        public void closeAllocation()
        {
            userCatalog = systemCatalog;
        }
    }
}

// End FarragoDatabase.java

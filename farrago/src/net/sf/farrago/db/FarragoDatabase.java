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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.config.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.query.*;
import net.sf.farrago.session.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.ddl.*;
import net.sf.farrago.namespace.*;

import net.sf.saffron.sql.*;
import net.sf.saffron.util.*;

import openjava.tools.DebugOut;

import java.sql.*;
import java.io.*;
import java.util.*;
import java.util.logging.*;

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
    private static Logger tracer =
        TraceUtil.getClassTrace(FarragoDatabase.class);
    
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
        
        assert(db.nReferences > 0);
        assert(db == instance);

        db.forgetAllocation(session);
        
        db.nReferences--;
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
        tracer.fine("actual reference count = " + instance.nReferences);
        if (instance.nReferences <= groundReferences) {
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
                systemCatalog.getRepository().beginTrans(true);
                try {
                    loadFennel(sessionFactory.newFennelCmdExecutor(),init);
                } finally {
                    systemCatalog.getRepository().endTrans(false);
                }
            } else {
                tracer.config("Fennel support disabled");
            }
            
            integrateSaffronTracing();

            long codeCacheMaxBytes = currentConfig.getCodeCacheMaxBytes();
            if (codeCacheMaxBytes == -1) {
                codeCacheMaxBytes = Long.MAX_VALUE;
            }
            codeCache = new FarragoObjectCache(this,codeCacheMaxBytes);

            // TODO:  parameter for cache size limit
            dataWrapperCache = new FarragoObjectCache(this,Long.MAX_VALUE);

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
        Iterator iter = attributeMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            FemDatabaseParam param =
                systemCatalog.newFemDatabaseParam();
            param.setName(entry.getKey().toString());
            param.setValue(entry.getValue().toString());
            cmd.getParams().add(param);

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
     * @param session FarragoSession requesting preparation
     *
     * @param catalog catalog to use for metadata lookup
     *
     * @param sqlNode the parsed form of the statement
     *
     * @param owner the FarragoAllocationOwner which will be responsible for
     * the returned stmt
     *
     * @param connectionDefaults defaults for unqualified object references
     *
     * @param indexMap FarragoIndexMap to use for index access
     *
     * @param viewInfo receives information about a prepared view definition
     *
     * @return statement implementation, or null when viewInfo is non-null
     */
    public FarragoExecutableStmt prepareStmt(
        FarragoSession session,
        FarragoCatalog catalog,
        SqlNode sqlNode,
        FarragoAllocationOwner owner,
        FarragoConnectionDefaults connectionDefaults,
        FarragoIndexMap indexMap,
        FarragoSessionViewInfo viewInfo)
    {
        final FarragoPreparingStmt stmt = new FarragoPreparingStmt(
            catalog,
            fennelDbHandle,
            session,
            codeCache,
            dataWrapperCache,
            indexMap);
        try {
            return prepareStmtImpl(
                session,stmt,sqlNode,owner,connectionDefaults,
                viewInfo);
        } finally {
            stmt.closeAllocation();
        }
    }
    
        
    private FarragoExecutableStmt prepareStmtImpl(
        FarragoSession session,
        final FarragoPreparingStmt stmt,
        SqlNode sqlNode,
        FarragoAllocationOwner owner,
        FarragoConnectionDefaults connectionDefaults,
        FarragoSessionViewInfo viewInfo)
    {
        // It would be silly to cache EXPLAIN PLAN results, so deal with them
        // directly.
        if (sqlNode.isA(SqlKind.Explain)) {
            FarragoExecutableStmt executableStmt = stmt.implement(sqlNode);
            owner.addAllocation(executableStmt);
            return executableStmt;
        }

        // Use unparsed validated SQL as cache key.  This eliminates trivial
        // differences such as whitespace and implicit qualifiers.
        
        final SqlNode validatedSqlNode = stmt.validate(sqlNode);
        
        SqlDialect sqlDialect = new SqlDialect(session.getDatabaseMetaData());
        final String sql = validatedSqlNode.toString(sqlDialect);
        
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
                    FarragoExecutableStmt executableStmt =
                        stmt.implement(validatedSqlNode);
                    long memUsage = FarragoUtil.getStringMemoryUsage(sql)
                        + executableStmt.getMemoryUsage();
                    entry.initialize(executableStmt,memUsage);
                }
            };
        
        FarragoObjectCache.Entry cacheEntry =
            codeCache.pin(sql,stmtFactory,false);
        owner.addAllocation(cacheEntry);
        return (FarragoExecutableStmt) cacheEntry.getValue();
    }

    public void updateSystemParameter(DdlSetSystemParamStmt ddlStmt)
    {
        // TODO:  something cleaner
        String paramName = ddlStmt.getParamName();
        if (paramName.equals("codeCacheMaxBytes")) {
            codeCache.setMaxBytes(
                systemCatalog.getCurrentConfig().getCodeCacheMaxBytes());
        }
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

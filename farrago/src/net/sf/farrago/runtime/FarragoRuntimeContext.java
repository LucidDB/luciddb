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
package net.sf.farrago.runtime;

import java.nio.*;

import java.sql.*;
import java.sql.Date;

import java.util.*;
import java.util.logging.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.trace.*;
import org.eigenbase.util.*;
import org.eigenbase.enki.mdr.*;
import org.eigenbase.jmi.JmiObjUtil;


/**
 * FarragoRuntimeContext defines runtime support routines needed by generated
 * code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRuntimeContext
    extends FarragoCompoundAllocation
    implements FarragoSessionRuntimeContext,
        RelOptConnection,
        FennelJavaStreamMap,
        FennelJavaErrorTarget
{
    //~ Static fields/initializers ---------------------------------------------

    protected static final Logger tracer =
        FarragoTrace.getRuntimeContextTracer();

    private static final ThreadLocal<List<FarragoUdrInvocationFrame>>
        threadInvocationStack =
            new ThreadLocal<List<FarragoUdrInvocationFrame>>();

    //~ Instance fields --------------------------------------------------------

    private final FarragoSession session;
    private final FarragoRepos repos;
    protected final FarragoObjectCache codeCache;
    private final Map<String, FarragoObjectCache.Entry> txnCodeCache;
    private final FennelTxnContext fennelTxnContext;
    private final FarragoWarningQueue warningQueue;
    protected final Object cursorMonitor;
    private boolean cursorActive;

    /**
     * Maps stream id to the corresponding java object.
     */
    private final Map<Integer, FennelJavaHandle> streamIdToHandleMap =
        new HashMap<Integer, FennelJavaHandle>();
    private final Object [] dynamicParamValues;

    protected FennelStreamGraph streamGraph;

    /**
     * responsible for closing the FennelStreamGraph
     */
    protected final FarragoCompoundAllocation streamOwner;

    private final FarragoSessionIndexMap indexMap;
    private final FarragoSessionVariables sessionVariables;
    private final FarragoDataWrapperCache dataWrapperCache;
    private final FarragoStreamFactoryProvider streamFactoryProvider;
    private final boolean isDml;
    private long currentTime;
    private boolean isCanceled;
    protected boolean isClosed;
    private ClassLoader statementClassLoader;
    private Map<String, RelDataType> resultSetTypeMap;
    protected long stmtId;

    private NativeRuntimeContext nativeContext;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoRuntimeContext.
     *
     * @param params constructor params
     */
    public FarragoRuntimeContext(FarragoSessionRuntimeParams params)
    {
        this.session = params.session;
        this.repos = params.repos;
        this.codeCache = params.codeCache;
        this.txnCodeCache = params.txnCodeCache;
        this.fennelTxnContext = params.fennelTxnContext;
        this.indexMap = params.indexMap;
        this.dynamicParamValues = params.dynamicParamValues;
        this.sessionVariables = params.sessionVariables;
        this.streamFactoryProvider = params.streamFactoryProvider;
        this.isDml = params.isDml;
        this.resultSetTypeMap = params.resultSetTypeMap;
        this.stmtId = params.stmtId;
        this.currentTime = params.currentTime;

        if (params.warningQueue == null) {
            params.warningQueue = new FarragoWarningQueue();
        }
        this.warningQueue = params.warningQueue;

        cursorMonitor = new Object();

        dataWrapperCache =
            new FarragoDataWrapperCache(
                this,
                params.sharedDataWrapperCache,
                session.getPluginClassLoader(),
                params.repos,
                params.fennelTxnContext.getFennelDbHandle(),
                new FarragoSessionDataSource(session));

        streamOwner = new StreamOwner();
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionRuntimeContext
    public FarragoWarningQueue getWarningQueue()
    {
        return warningQueue;
    }

    /**
     * Returns the stream graph.
     */
    protected FennelStreamGraph getStreamGraph()
    {
        return streamGraph;
    }

    // implement RelOptConnection
    public RelOptSchema getRelOptSchema()
    {
        throw new AssertionError();
    }

    // TODO jvs 13-Aug-2008:  introduce an abstract base
    // SynchronizedCompoundClosableAllocation and inherit from that

    // override CompoundClosableAllocation
    public synchronized void closeAllocation()
    {
        if (isClosed) {
            return;
        }
        isClosed = true;

        isCanceled = true;

        // Override CompoundClosableAllocation behavior, because we
        // need special synchronization to account for the fact
        // that FarragoJavaUdxIterator instances may be adding themselves
        // concurrently during this shutdown.  Question:  is it possible
        // for one to leak due to a race?
        for (;;) {
            ClosableAllocation allocation;
            synchronized (allocations) {
                if (allocations.isEmpty()) {
                    break;
                }
                allocation = allocations.remove(allocations.size() - 1);
            }
            allocation.closeAllocation();
        }
        
        // make sure all streams get closed BEFORE they are deallocated
        streamOwner.closeAllocation();
        if (!isDml) {
            // For queries, this is called when the cursor is closed.
            session.endTransactionIfAuto(true);
        }
        statementClassLoader = null;

        // FRG-253: nullify this, so that once we release its pinned entry from
        // the cache, we don't try to abort it after someone else starts to
        // reuse it!
        streamGraph = null;
    }

    // override CompoundClosableAllocation
    public void addAllocation(ClosableAllocation allocation)
    {
        synchronized (allocations) {
            super.addAllocation(allocation);
        }
    }
    
    // override CompoundClosableAllocation
    public boolean forgetAllocation(ClosableAllocation allocation)
    {
        synchronized (allocations) {
            return super.forgetAllocation(allocation);
        }
    }
    
    // override CompoundClosableAllocation
    public boolean hasAllocations()
    {
        synchronized (allocations) {
            return !allocations.isEmpty();
        }
    }
    
    // implement RelOptConnection
    public Object contentsAsArray(
        String qualifier,
        String tableName)
    {
        throw new AssertionError();
    }

    /**
     * Gets an object needed to support the implementation of foreign data
     * access.
     *
     * @param serverMofId MOFID of foreign server being accessed
     * @param param server-specific runtime parameter
     *
     * @return server-specific runtime support object
     */
    public Object getDataServerRuntimeSupport(
        String serverMofId,
        Object param)
    {
        EnkiMDRepository mdrRepos = repos.getEnkiMdrRepos();
        mdrRepos.beginSession();
        mdrRepos.beginTrans(false);
        try {
            FemDataServer femServer =
                (FemDataServer) mdrRepos.getByMofId(serverMofId);
    
            FarragoMedDataServer server =
                dataWrapperCache.loadServerFromCatalog(femServer);
            try {
                Object obj = server.getRuntimeSupport(param);
                if (obj instanceof FarragoAllocation) {
                    addAllocation((FarragoAllocation) obj);
                }
                return obj;
            } catch (Throwable ex) {
                throw FarragoResource.instance().DataServerRuntimeFailed.ex(ex);
            }
        } finally {
            mdrRepos.endTrans();
            mdrRepos.endSession();
        }
    }

    /**
     * Gets the MofId for a RefBaseObject, or null if the object is null. This
     * is called at execution from code generated by MdrTable.
     *
     * @param refObj RefBaseObject for which to get the MofId
     *
     * @return MofId or null
     */
    public String getRefMofId(RefBaseObject refObj)
    {
        if (refObj == null) {
            return null;
        } else {
            return refObj.refMofId();
        }
    }

    /**
     * Gets the value bound to a dynamic parameter.
     *
     * @param paramIndex 0-based index of parameter
     *
     * @return bound value
     */
    public Object getDynamicParamValue(int paramIndex)
    {
        return dynamicParamValues[paramIndex];
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable USER.
     */
    public String getContextVariable_USER()
    {
        return sessionVariables.currentUserName;
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_USER.
     */
    public String getContextVariable_CURRENT_USER()
    {
        return sessionVariables.currentUserName;
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable SYSTEM_USER.
     */
    public String getContextVariable_SYSTEM_USER()
    {
        return sessionVariables.systemUserName;
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable SESSION_USER.
     */
    public String getContextVariable_SESSION_USER()
    {
        return sessionVariables.sessionUserName;
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_ROLE.
     */
    public String getContextVariable_CURRENT_ROLE()
    {
        return sessionVariables.currentRoleName;
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_PATH.
     *
     * @sql.99 Part 2 Section 6.3 General Rule 10
     */
    public String getContextVariable_CURRENT_PATH()
    {
        return sessionVariables.getFormattedSchemaSearchPath(
            session.getDatabaseMetaData());
    }

    protected long getCurrentTime()
    {
        // NOTE jvs 25-Sept-2004:  per SQL standard, the same time
        // is used for all references within the same statement.
        if (currentTime == 0) {
            // internally, we use a psuedo local time
            currentTime = System.currentTimeMillis();
        }
        return currentTime;
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_DATE.
     */
    public Date getContextVariable_CURRENT_DATE()
    {
        return new Date(getCurrentTime());
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_TIME.
     */
    public Time getContextVariable_CURRENT_TIME()
    {
        // Strip off the milliseconds in the time value since CURRENT_TIME
        // doesn't return that portion of the time
        long currTime = getCurrentTime();
        return new Time(currTime - (currTime % 1000));
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_TIMESTAMP.
     */
    public Timestamp getContextVariable_CURRENT_TIMESTAMP()
    {
        return new Timestamp(getCurrentTime());
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable LOCALTIME.
     */
    public Time getContextVariable_LOCALTIME()
    {
        return getContextVariable_CURRENT_TIME();
    }

    /**
     * Called from generated code.
     *
     * @return the value of context variable LOCALTIMESTAMP.
     */
    public Timestamp getContextVariable_LOCALTIMESTAMP()
    {
        return getContextVariable_CURRENT_TIMESTAMP();
    }

    // REVIEW jvs 22-Mar-2006:  JavaPullTupleStream is no longer
    // used within Farrago, since everything is handled via transforms
    // instead.  Is it still used outside of Farrago?

    /**
     * Associates a stream id with a java stream object, so that it can be
     * retrieved by a native method later.
     */
    protected void registerJavaStream(int streamId, Object stream)
    {
        registerJavaStream(streamId, stream, this);
    }

    /**
     * Associates a stream id with a java stream object, so that it can be
     * retrieved by a native method later. Binds the stream to a specific owner
     * (that will eventually close it).
     */
    protected void registerJavaStream(
        int streamId,
        Object stream,
        FarragoCompoundAllocation streamOwner)
    {
        streamIdToHandleMap.put(
            new Integer(streamId),
            FennelDbHandle.allocateNewObjectHandle(streamOwner, stream));
    }

    /**
     * Stupid helper for code generated by {@link
     * net.sf.farrago.query.FennelDoubleRel}.
     *
     * @param dummy1 a dummy
     * @param dummy2 another dummy
     *
     * @return yet another dummy
     */
    public Object dummyPair(
        Object dummy1,
        Object dummy2)
    {
        assert (dummy1 == null);
        assert (dummy2 == null);
        return null;
    }

    /**
     * Stupid helper for code generated by {@link
     * net.sf.farrago.query.FennelMultipleRel}.
     *
     * @param dummyArray array of dummies
     *
     * @return yet another dummy
     */
    public Object dummyArray(
        Object [] dummyArray)
    {
        for (int i = 0; i < dummyArray.length; i++) {
            assert (dummyArray[i] == null);
        }
        return null;
    }

    // implement FarragoSessionRuntimeContext
    public void loadFennelPlan(final String xmiFennelPlan)
    {
        assert (streamGraph == null);

        FarragoObjectCache.CachedObjectFactory streamFactory =
            new FarragoObjectCache.CachedObjectFactory() {
                public void initializeEntry(
                    Object key,
                    FarragoObjectCache.UninitializedEntry entry)
                {
                    assert (key.equals(xmiFennelPlan));
                    streamGraph = prepareStreamGraph(xmiFennelPlan);

                    long memUsage =
                        FarragoUtil.getFennelMemoryUsage(xmiFennelPlan);
                    entry.initialize(streamGraph, memUsage, true);
                }
                
                public boolean isStale(Object value)
                {
                    return false;
                }
            };

        FarragoObjectCache.Entry cacheEntry = null;
        if (txnCodeCache != null) {
            cacheEntry = txnCodeCache.get(xmiFennelPlan);
        }
        if (cacheEntry == null) {
            cacheEntry = codeCache.pin(xmiFennelPlan, streamFactory, true);
        }

        if (txnCodeCache == null) {
            addAllocation(cacheEntry);
        } else {
            txnCodeCache.put(xmiFennelPlan, cacheEntry);
        }

        if (streamGraph == null) {
            streamGraph = (FennelStreamGraph) cacheEntry.getValue();
            streamOwner.addAllocation(streamGraph);
        }
    }

    // implement FarragoSessionRuntimeContext
    public void openStreams()
    {
        assert (streamGraph != null);
        streamGraph.open(fennelTxnContext, this, this);
    }

    // implement FarragoSessionRuntimeContext
    public FennelStreamGraph getFennelStreamGraph()
    {
        return streamGraph;
    }

    /**
     * Creates a FennelTupleIter for executing a plan represented as XML. This
     * is called at execution from code generated by FennelToIteratorConverter.
     *
     * @param tupleReader object providing FennelTupleReader implementation
     * @param streamName name of stream from which to read (globally unique)
     * @param streamId id of stream from which to read (unique within statement)
     * @param dummies a dummy parameter to give non-Fennel children a place to
     * generate code
     *
     * @return tuple iterator
     */
    public TupleIter newFennelTupleIter(
        FennelTupleReader tupleReader,
        String streamName,
        int streamId,
        Object dummies)
    {
        assert (dummies == null);
        assert (streamGraph != null);

        FarragoReposTxnContext txn = repos.newTxnContext(true);
        txn.beginReadTxn();
        try {
            FennelStreamHandle streamHandle = getStreamHandle(streamName, true);
    
            return new FennelTupleIter(
                tupleReader,
                streamGraph,
                streamHandle,
                repos.getCurrentConfig().getFennelConfig().getCachePageSize());
        } finally {
            txn.commit();
        }
    }

    /**
     * Creates a FennelTupleIter for executing a plan represented as XML. This
     * is called at execution in Fennel's JavaTransformExecStream from code
     * generated by FennelToIteratorConverter.
     *
     * <p>Note: The semantics of streamName and streamId differ from {@link
     * #newFennelTupleIter(FennelTupleReader, String, int, Object)}.
     *
     * @param tupleReader object providing FennelTupleReader implementation
     * @param streamName name of the JavaExecTransformStream we're reading on
     * behalf of
     * @param inputStreamName the global name of a stream to read
     * @param dummies a dummy parameter to give non-Fennel children a place to
     * generate code
     *
     * @return tuple iterator
     */
    public TupleIter newFennelTransformTupleIter(
        FennelTupleReader tupleReader,
        String streamName,
        String inputStreamName,
        FarragoTransform.InputBinding [] inputBindings,
        Object dummies)
    {
        assert (dummies == null);
        assert (streamGraph != null);
        assert (inputBindings != null);

        FarragoReposTxnContext txn = repos.newTxnContext(true);
        txn.beginReadTxn();
        try {
            FennelStreamHandle streamHandle = 
                getStreamHandle(streamName, false);
    
            FennelStreamHandle inputStreamHandle =
                getStreamHandle(inputStreamName, true);
    
            FarragoTransform.InputBinding inputBinding = null;
            for (FarragoTransform.InputBinding binding : inputBindings) {
                // The binding's input stream name may be a buffer adapter 
                // created to handle provisioning of buffers.  It's name will
                // be the stream name we're looking for plus some additional
                // information.
                if (binding.getInputStreamName().startsWith(inputStreamName)) {
                    inputBinding = binding;
                    break;
                }
            }
            assert (inputBinding != null);
    
            return new FennelTransformTupleIter(
                tupleReader,
                streamGraph,
                streamHandle,
                inputStreamHandle,
                inputBinding.getOrdinal(),
                repos.getCurrentConfig().getFennelConfig().getCachePageSize());
        }
        finally {
            txn.commit();
        }
    }

    // implement FarragoSessionRuntimeContext
    public FennelStreamHandle getStreamHandle(
        String globalStreamName,
        boolean isInput)
    {
        return streamGraph.findStream(repos, globalStreamName, isInput);
    }

    protected FennelStreamGraph prepareStreamGraph(String xmiFennelPlan)
    {
        boolean success = false;
        FennelStreamGraph newStreamGraph = null;
        repos.beginReposSession();
        try {
            Collection<RefBaseObject> collection =
                JmiObjUtil.importFromXmiString(
                    repos.getTransientFarragoPackage(),
                    xmiFennelPlan);
            assert (collection.size() == 1);
            FemCmdPrepareExecutionStreamGraph cmd =
                (FemCmdPrepareExecutionStreamGraph) collection.iterator()
                .next();

            newStreamGraph = fennelTxnContext.newStreamGraph(streamOwner);
            streamFactoryProvider.registerStreamFactories(
                newStreamGraph.getLongHandle());
            cmd.setStreamGraphHandle(newStreamGraph.getStreamGraphHandle());
            fennelTxnContext.getFennelDbHandle().executeCmd(cmd);
            success = true;
            return newStreamGraph;
        } catch (RuntimeException e) {
            // When these occur, it can cause the ensuing
            // FennelStorage.tupleStreamGraphClose() to crash the JVM.
            tracer.log(Level.SEVERE, "stream preparation exception", e);
            throw e;
        } catch (Error e) {
            // When these occur, it can cause the ensuing
            // FennelStorage.tupleStreamGraphClose() to crash the JVM.
            tracer.log(Level.SEVERE, "stream preparation error", e);
            throw e;
        } finally {
            if (!success) {
                if (newStreamGraph != null) {
                    newStreamGraph.closeAllocation();
                }
            }
            repos.endReposSession();
        }
    }

    // implement FennelJavaStreamMap
    public long getJavaStreamHandle(int streamId)
    {
        FennelJavaHandle handle = streamIdToHandleMap.get(streamId);
        assert handle != null : "No handle for stream #" + streamId;
        return handle.getLongHandle();
    }

    // implement FennelJavaStreamMap
    public long getIndexRoot(long pageOwnerId)
    {
        FemLocalIndex index = indexMap.getIndexById(pageOwnerId);
        return indexMap.getIndexRoot(index);
    }

    /**
     * @return handle to Fennel database being accessed
     */
    public FennelDbHandle getFennelDbHandle()
    {
        return fennelTxnContext.getFennelDbHandle();
    }

    // implement FarragoSessionRuntimeContext
    public FarragoRepos getRepos()
    {
        return repos;
    }

    /**
     * Called when a nullable value is cast to a NOT NULL type.
     *
     * @param targetName target expression
     * @param nullableValue source value
     */
    public void checkNotNull(String targetName, NullableValue nullableValue)
    {
        if (nullableValue.isNull()) {
            throw FarragoResource.instance().NullNotAllowed.ex(
                targetName);
        }
    }

    /**
     * Called when a nullable value is cast to a NOT NULL type.
     *
     * @param targetName target expression
     * @param obj source value
     */
    public void checkNotNull(String targetName, Object obj)
    {
        if (null == obj) {
            throw FarragoResource.instance().NullNotAllowed.ex(
                targetName);
        }
    }

    // implement FarragoSessionRuntimeContext
    public void pushRoutineInvocation(
        FarragoSessionUdrContext udrContext,
        boolean allowSql)
    {
        // TODO jvs 19-Jan-2005: set system properties sqlj.defaultconnection
        // and sqlj.runtime per SQL:2003 13:12.1.2.  Also other
        // context stuff.

        FarragoUdrInvocationFrame frame = new FarragoUdrInvocationFrame();
        frame.context = this;
        frame.allowSql = allowSql;
        udrContext.setSession(session);
        frame.udrContext = udrContext;

        List<FarragoUdrInvocationFrame> stack = getInvocationStack();
        stack.add(frame);
    }

    // implement FarragoSessionRuntimeContext
    public void popRoutineInvocation()
    {
        // TODO jvs 19-Jan-2005:  see corresponding comment in
        // pushRoutineInvocation.

        List<FarragoUdrInvocationFrame> stack = getInvocationStack();
        assert (!stack.isEmpty());
        FarragoUdrInvocationFrame frame = stack.remove(stack.size() - 1);
        assert (frame.context == this);
        if (frame.connection != null) {
            try {
                frame.connection.close();
            } catch (SQLException ex) {
                // TODO jvs 19-Jan-2005:  standard mechanism for tracing
                // swallowed exceptions
            } finally {
                EnkiMDRepository mdrRepos = 
                    frame.context.getRepos().getEnkiMdrRepos();
                mdrRepos.reattachSession(frame.reposSession);
            }
        }
    }

    // implement FarragoSessionRuntimeContext
    public void cancel()
    {
        synchronized (this) {
            // be sure only one thread tries to close the FennelStreamGraph
            if (isCanceled) {
                return;
            }

            // set isCanceled before aborting streamGraph to ensure
            // that flag is set when ResultSet sees END_OF_DATA
            isCanceled = true;
        }

        FennelStreamGraph streamGraphToAbort = streamGraph;
        if (streamGraphToAbort != null) {
            streamGraphToAbort.abort();
        }
    }

    // implement FarragoSessionRuntimeContext
    public void checkCancel()
    {
        if (isCanceled) {
            throw FarragoResource.instance().ExecutionAborted.ex();
        }
    }

    // implement FarragoSessionRuntimeContext
    public void setCursorState(boolean active)
    {
        synchronized (cursorMonitor) {
            if (active) {
                // check before fetch
                checkCancel();
            }
            cursorActive = active;
            if (!cursorActive) {
                cursorMonitor.notifyAll();
            }
        }
    }

    // implement FarragoSessionRuntimeContext
    public void waitForCursor()
    {
        synchronized (cursorMonitor) {
            try {
                while (cursorActive) {
                    cursorMonitor.wait();
                }
            } catch (InterruptedException ex) {
                throw Util.newInternal(ex);
            }
        }
    }

    // implement FarragoSessionRuntimeContext
    public RuntimeException handleRoutineInvocationException(
        Throwable ex,
        String methodName)
    {
        // TODO jvs 19-Jan-2005:  special SQLSTATE handling defined
        // in SQL:2003-13-15.1
        return FarragoResource.instance().RoutineInvocationException.ex(
            methodName,
            ex);
    }

    private static List<FarragoUdrInvocationFrame> getInvocationStack()
    {
        List<FarragoUdrInvocationFrame> stack = threadInvocationStack.get();
        if (stack == null) {
            stack = new ArrayList<FarragoUdrInvocationFrame>();
            threadInvocationStack.set(stack);
        }
        return stack;
    }

    static FarragoUdrInvocationFrame getUdrInvocationFrame()
    {
        List<FarragoUdrInvocationFrame> stack = threadInvocationStack.get();
        if ((stack == null) || (stack.isEmpty())) {
            throw new IllegalStateException("No UDR executing.");
        }
        FarragoUdrInvocationFrame frame = peekStackFrame(stack);
        return frame;
    }

    private static FarragoUdrInvocationFrame peekStackFrame(
        List<FarragoUdrInvocationFrame> stack)
    {
        assert (!stack.isEmpty());
        return stack.get(stack.size() - 1);
    }

    /**
     * Creates a new default connection attached to the session of the current
     * thread.
     */
    public static Connection newConnection()
    {
        List<FarragoUdrInvocationFrame> stack = getInvocationStack();
        if (stack.isEmpty()) {
            throw FarragoResource.instance().NoDefaultConnection.ex();
        }

        FarragoUdrInvocationFrame frame = peekStackFrame(stack);

        if (!frame.allowSql) {
            throw FarragoResource.instance().NoDefaultConnection.ex();
        }
        
        if (frame.connection == null) {
            FarragoSessionConnectionSource connectionSource =
                frame.context.session.getConnectionSource();
            frame.connection = connectionSource.newConnection();
            // TODO jvs 19-Jan-2005:  we're also supposed to make
            // sure the new connection has autocommit turned off.  Need
            // to do that without disturbing the session.  And could
            // enforce READS/MODIFIES SQL DATA access.

            EnkiMDRepository mdrRepos = 
                frame.context.getRepos().getEnkiMdrRepos();
            frame.reposSession = mdrRepos.detachSession();
        }

        // NOTE jvs 19-Jan-2005:  We automatically close the
        // connection in popRoutineInvocation, which is guaranteed
        // to be called because we generate it in a finally block.  So
        // there's no need to track the connection as an allocation.

        return frame.connection;
    }

    public void setStatementClassLoader(ClassLoader statementClassLoader)
    {
        this.statementClassLoader = statementClassLoader;
    }

    public Class statementClassForName(String statementClassName)
    {
        try {
            return Class.forName(
                statementClassName,
                true,
                statementClassLoader);
        } catch (ClassNotFoundException e) {
            tracer.log(
                Level.SEVERE,
                "Could not load statement class: " + statementClassName,
                e);
            return null;
        }
    }

    // implement FarragoSessionRuntimeContext
    public RelDataType getRowTypeForResultSet(String resultSetName)
    {
        return resultSetTypeMap.get(resultSetName);
    }

    public FarragoSequenceAccessor getSequenceAccessor(String mofId)
    {
        return repos.getSequenceAccessor(mofId);
    }

    // implement FarragoSessionRuntimeContext
    public FarragoSession getSession()
    {
        return session;
    }

    /**
     * Handles an exception encountered by a stream while processing an input
     * row. The default implementation logs exceptions to the server trace file.
     *
     * @param row the input row object
     * @param ex the exception encountered
     * @param columnIndex index for the output column being calculated when an
     * exception was encountered, or zero if the filter condition was being
     * evaluated. This parameter may be -1 if not appropriate for the exception
     * @param tag an error handling tag specific to the runtime context. This
     * parameter may also be null
     *
     * @return the status of the error handler. While the default implementation
     * returns null, Farrago extensions may return more informative values, such
     * as TupleIter.NoDataReason.
     */
    public Object handleRowError(
        SyntheticObject row,
        RuntimeException ex,
        int columnIndex,
        String tag)
    {
        return handleRowError(row, ex, columnIndex, tag, false);
    }

    /**
     * Handles a runtime exception, but allows warnings as well.
     *
     * @see #handleRowError(SyntheticObject, RuntimeException, int, String)
     */
    public Object handleRowError(
        SyntheticObject row,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning)
    {
        return handleRowErrorHelper(
            row.toString(),
            ex,
            columnIndex,
            tag,
            isWarning);
    }

    /**
     * Handles a runtime exception based on an array of column values rather
     * than on a SyntheticObject
     */
    public Object handleRowError(
        String [] columnNames,
        Object [] columnValues,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning)
    {
        return handleRowErrorHelper(
            Util.flatArrayToString(columnValues),
            ex,
            columnIndex,
            tag,
            isWarning);
    }

    /**
     * Handles runtime exception; if errorCode is non-null, exceptions are
     * deferred until all errors on a row are processed; not currently
     * implemented
     */
    public Object handleRowError(
        String [] columnNames,
        Object [] columnValues,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning,
        String errorCode,
        String columnName)
    {
        throw Util.needToImplement(this);
    }

    /**
     * Handles exception for row errors with deferred exceptions; not currently
     * implemented
     */
    public void handleRowErrorCompletion(RuntimeException ex, String tag)
    {
        throw Util.needToImplement(this);
    }

    /**
     * Helper for various handleRowError methods
     */
    private EigenbaseException handleRowErrorHelper(
        String row,
        RuntimeException ex,
        int columnIndex,
        String tag,
        boolean isWarning)
    {
        EigenbaseTrace.getStatementTracer().log(
            Level.WARNING,
            "Row level exception",
            makeRowError(ex, row, columnIndex, null));
        return null;
    }

    /**
     * Makes a row error based on conventions for column index
     *
     * @param ex the runtime exception encountered
     * @param row string representing the row on which an exception occurred
     * @param index index of the column being processed at the time the
     * exception was encountered, or 0 for an error processing a conditional
     * expression, or -1 for a non-specific error
     * @param field optional column name, used in constructing the error message
     * when columnIndex > 0
     *
     * @return a non-nested exception summarizing the row error
     */
    protected EigenbaseException makeRowError(
        RuntimeException ex,
        String row,
        int index,
        String field)
    {
        FarragoResource resource = FarragoResource.instance();
        String msgs = Util.getMessages(ex);

        if (index < 0) {
            return resource.JavaRowError.ex(row, msgs, ex);
        } else if (index == 0) {
            return resource.JavaCalcConditionError.ex(row, msgs, ex);
        } else {
            String fieldName =
                (field != null) ? field : Integer.toString(index);
            return resource.JavaCalcError.ex(
                fieldName,
                row,
                msgs,
                ex);
        }
    }

    // implement FennelJavaErrorTarget
    public Object handleRowError(
        String source,
        boolean isWarning,
        String msg,
        ByteBuffer byteBuffer,
        int index)
    {
        if (nativeContext == null) {
            nativeContext = new NativeRuntimeContext(this);
        }
        return nativeContext.handleRowError(
            source,
            isWarning,
            msg,
            byteBuffer,
            index);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Inner class for taking care of closing streams without deallocating them.
     */
    private static class StreamOwner
        extends FarragoCompoundAllocation
    {
        public void closeAllocation()
        {
            // traverse in reverse order
            ListIterator iter = allocations.listIterator(allocations.size());
            while (iter.hasPrevious()) {
                FennelStreamGraph streamGraph =
                    (FennelStreamGraph) iter.previous();
                streamGraph.close();
            }
            allocations.clear();
        }
    }
}

// End FarragoRuntimeContext.java

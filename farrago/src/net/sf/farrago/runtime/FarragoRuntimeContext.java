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
package net.sf.farrago.runtime;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.sql.*;

import javax.jmi.reflect.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.session.*;
import net.sf.farrago.trace.FarragoTrace;
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.util.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.pretty.SqlPrettyWriter;
import org.eigenbase.runtime.RestartableIterator;
import org.eigenbase.runtime.TupleIter;

import java.sql.Date;

/**
 * FarragoRuntimeContext defines runtime support routines needed by generated
 * code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRuntimeContext extends FarragoCompoundAllocation
    implements FarragoSessionRuntimeContext,
        RelOptConnection,
        FennelJavaStreamMap
{
    protected static final Logger tracer = 
        FarragoTrace.getRuntimeContextTracer();

    private static final ThreadLocal threadInvocationStack = new ThreadLocal();

    //~ Instance fields -------------------------------------------------------

    private final FarragoSession session;
    private final FarragoRepos repos;
    private final FarragoObjectCache codeCache;
    private final Map txnCodeCache;
    private final FennelTxnContext fennelTxnContext;
    
    /**
     * Maps stream id ({@link Integer}) to the corresponding java object
     * ({@link FennelJavaHandle}).
     */
    private final Map streamIdToHandleMap = new HashMap();
    private final Object [] dynamicParamValues;
    private final FarragoCompoundAllocation streamOwner;
    private final FarragoSessionIndexMap indexMap;
    private final FarragoSessionVariables sessionVariables;
    private final FarragoDataWrapperCache dataWrapperCache;
    private final FarragoStreamFactoryProvider streamFactoryProvider;
    private final boolean isDml;
    private FennelStreamGraph streamGraph;
    private long currentTime;
    private boolean isCanceled;
    private ClassLoader statementClassLoader;

    //~ Constructors ----------------------------------------------------------

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

    //~ Methods ---------------------------------------------------------------

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

    // override FarragoCompoundAllocation
    public void closeAllocation()
    {
        isCanceled = true;
        // make sure all streams get closed BEFORE they are deallocated
        streamOwner.closeAllocation();
        if (!isDml) {
            // For queries, this is called when the cursor is closed. 
            session.endTransactionIfAuto(true);
        }
        statementClassLoader = null;
        super.closeAllocation();
    }

    // implement RelOptConnection
    public Object contentsAsArray(
        String qualifier,
        String tableName)
    {
        throw new AssertionError();
    }

    /**
     * Gets an object needed to support the implementation of foreign
     * data access.
     *
     * @param serverMofId MOFID of foreign server being accessed
     *
     * @param param server-specific runtime parameter
     *
     * @return server-specific runtime support object
     */
    public Object getDataServerRuntimeSupport(
        String serverMofId,
        Object param)
    {
        FemDataServer femServer =
            (FemDataServer) repos.getMdrRepos().getByMofId(serverMofId);

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
    }

    /**
     * Gets the MofId for a RefBaseObject, or null if the object is null.  This
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
     * @sql.99 Part 2 Section 6.3 General Rule 10
     *
     * @return the value of context variable CURRENT_PATH.
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
        return new Time(getCurrentTime());
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

    /**
     * Creates a {@link JavaPullTupleStream} (for feeding the results of an
     * {@link Iterator} to Fennel) and stores it in a handle.
     *
     * <p>This is called at execution time from code generated by
     * {@link net.sf.farrago.query.IteratorToFennelConverter}.
     *
     * @param streamId ID under which to register the stream (unique within
     *   statement); if -1, do not register
     * @param tupleWriter FennelTupleWriter for marshalling tuples
     * @param iter row producer
     *
     * @return dummy object
     */
    public Object newJavaTupleStream(
        int streamId,
        FennelTupleWriter tupleWriter,
        RestartableIterator iter)
    {
        assert(!CallingConvention.ENABLE_NEW_ITER);
        JavaPullTupleStream stream = new JavaPullTupleStream(tupleWriter, iter);
        if (streamId != -1) {
            registerJavaStream(streamId, stream);
        }
        return null;
    }

    /**
     * Creates a {@link JavaPullTupleStream} (for feeding the results of an
     * {@link TupleIter} to Fennel) and stores it in a handle.
     *
     * <p>This is called at execution time from code generated by
     * {@link net.sf.farrago.query.IteratorToFennelConverter}.
     *
     * @param streamId ID under which to register the stream (unique within
     *   statement); if -1, do not register
     * @param tupleWriter FennelTupleWriter for marshalling tuples
     * @param tupleIter row producer
     *
     * @return dummy object
     */
    public Object newJavaTupleStream(
        int streamId,
        FennelTupleWriter tupleWriter,
        TupleIter tupleIter)
    {
        assert(CallingConvention.ENABLE_NEW_ITER);
        JavaPullTupleStream stream = 
            new JavaPullTupleStream(tupleWriter, tupleIter);
        if (streamId != -1) {
            registerJavaStream(streamId, stream);
        }
        return null;
    }

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
        int streamId, Object stream, FarragoCompoundAllocation streamOwner)
    {
        streamIdToHandleMap.put(
            new Integer(streamId),
            FennelDbHandle.allocateNewObjectHandle(streamOwner, stream));
    }

    /**
     * Stupid helper for code generated by
     * {@link net.sf.farrago.query.FennelDoubleRel}.
     *
     * @param dummy1 a dummy
     * @param dummy2 another dummy
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
     * Stupid helper for code generated by
     * {@link net.sf.farrago.query.FennelMultipleRel}.
     *
     * @param dummyArray array of dummies
     * @return yet another dummy
     */
    public Object dummyArray(
        Object[] dummyArray)
    {
        for (int i=0; i < dummyArray.length; i++) {
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

                    // TODO:  proper memory accounting
                    long memUsage =
                        FarragoUtil.getStringMemoryUsage(xmiFennelPlan);
                    entry.initialize(streamGraph, memUsage);
                }
            };

        FarragoObjectCache.Entry cacheEntry = null;
        if (txnCodeCache != null) {
            cacheEntry =
                (FarragoObjectCache.Entry) txnCodeCache.get(xmiFennelPlan);
        }
        if (cacheEntry == null) {
            // NOTE jvs 15-July-2004:  to avoid deadlock, grab the catalog
            // lock BEFORE we pin the cache entry (this matches the
            // order used by statement preparation)
            repos.beginTransientTxn();
            try {
                cacheEntry = codeCache.pin(xmiFennelPlan, streamFactory, true);
            } finally {
                repos.endTransientTxn();
            }
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
        streamGraph.open(fennelTxnContext, this);
    }

    // implement FarragoSessionRuntimeContext
    public FennelStreamGraph getFennelStreamGraph()
    {
        return streamGraph;
    }

    /**
     * Creates a FennelIterator for executing a plan represented as XML.  This
     * is called at execution from code generated by
     * FennelToIteratorConverter.
     *
     * @param tupleReader object providing FennelTupleReader implementation
     * @param streamName name of stream from which to read (globally unique)
     * @param streamId id of stream from which to read (unique within
     *        statement)
     * @param dummies a dummy parameter to give non-Fennel children a place to
     *        generate code
     *
     * @return iterator
     */
    public RestartableIterator newFennelIterator(
        FennelTupleReader tupleReader,
        String streamName,
        int streamId,
        Object dummies)
    {
        assert (dummies == null);
        assert (streamGraph != null);
        assert(!CallingConvention.ENABLE_NEW_ITER);

        FennelStreamHandle streamHandle = getStreamHandle(streamName, true);

        return new FennelIterator(
            tupleReader,
            streamGraph,
            streamHandle,
            repos.getCurrentConfig().getFennelConfig().getCachePageSize());
    }

    /**
     * Creates a FennelTupleIter for executing a plan represented as XML.  This
     * is called at execution from code generated by
     * FennelToIteratorConverter.
     *
     * @param tupleReader object providing FennelTupleReader implementation
     * @param streamName name of stream from which to read (globally unique)
     * @param streamId id of stream from which to read (unique within
     *        statement)
     * @param dummies a dummy parameter to give non-Fennel children a place to
     *        generate code
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
        assert(CallingConvention.ENABLE_NEW_ITER);

        FennelStreamHandle streamHandle = getStreamHandle(streamName, true);

        return new FennelTupleIter(
            tupleReader,
            streamGraph,
            streamHandle,
            repos.getCurrentConfig().getFennelConfig().getCachePageSize());
    }
    
    /**
     * Creates a FennelTupleIter for executing a plan represented as XML.  This
     * is called at execution in Fennel's JavaTransformExecStream from code 
     * generated by FennelToIteratorConverter.
     * 
     * <p>Note: The semantics of streamName and streamId differ from
     * {@link #newFennelTupleIter(FennelTupleReader, String, int, Object)}.
     *
     * @param tupleReader object providing FennelTupleReader implementation
     * @param streamName name of the JavaExecTransformStream we're reading
     *                   on behalf of
     * @param inputStreamName the global name of a stream to read 
     * @param dummies a dummy parameter to give non-Fennel children a place to
     *        generate code
     *
     * @return tuple iterator
     */
    public TupleIter newFennelTransformTupleIter(
        FennelTupleReader tupleReader,
        String streamName,
        String inputStreamName,
        FarragoTransform.InputBinding[] inputBindings,
        Object dummies)
    {
        assert (dummies == null);
        assert (streamGraph != null);
        assert (inputBindings != null);
        assert (CallingConvention.ENABLE_NEW_ITER);

        FennelStreamHandle streamHandle = getStreamHandle(streamName, false);

        FennelStreamHandle inputStreamHandle = 
            getStreamHandle(inputStreamName, true);

        FarragoTransform.InputBinding inputBinding = null;
        for(FarragoTransform.InputBinding binding: inputBindings) {
            // The binding's input stream name may be a buffer adapter created
            // to handle provisioning of buffers.  It's name will be the
            // stream name we're looking for plus some addition information.
            if (binding.getInputStreamName().startsWith(inputStreamName)) {
                inputBinding = binding;
                break;
            }
        }
        assert(inputBinding != null);
        
        return new FennelTransformTupleIter(
            tupleReader,
            streamGraph,
            streamHandle,
            inputStreamHandle,
            inputBinding.getOrdinal(),
            repos.getCurrentConfig().getFennelConfig().getCachePageSize());
    }

    protected FennelStreamHandle getStreamHandle(String globalStreamName, boolean isInput)
    {
        repos.beginReposTxn(true);
        try {
            return streamGraph.findStream(repos, globalStreamName, isInput);
        } finally {
            repos.endReposTxn(false);
        }
    }

    private FennelStreamGraph prepareStreamGraph(String xmiFennelPlan)
    {
        boolean success = false;
        FennelStreamGraph newStreamGraph = null;
        try {
            Collection collection =
                JmiUtil.importFromXmiString(repos.getTransientFarragoPackage(),
                    xmiFennelPlan);
            assert (collection.size() == 1);
            FemCmdPrepareExecutionStreamGraph cmd =
                (FemCmdPrepareExecutionStreamGraph) collection.iterator().next();

            newStreamGraph = fennelTxnContext.newStreamGraph(streamOwner);
            streamFactoryProvider.registerStreamFactories(
                newStreamGraph.getLongHandle());
            cmd.setStreamGraphHandle(newStreamGraph.getStreamGraphHandle());
            fennelTxnContext.getFennelDbHandle().executeCmd(cmd);
            success = true;
            return newStreamGraph;
        } catch(RuntimeException e) {
            // When these occur, it can cause the ensuing 
            // FennelStorage.tupleStreamGraphClose() to crash the JVM.
            tracer.log(Level.SEVERE, "stream preparation exception", e);
            throw e;
        } catch(Error e) {
            // When these occur, it can cause the ensuing 
            // FennelStorage.tupleStreamGraphClose() to crash the JVM.
            tracer.log(Level.SEVERE, "stream preparation error", e);
            throw e;
        } finally {
            if (!success) {
                newStreamGraph.closeAllocation();
            }
        }
    }

    // implement FennelJavaStreamMap
    public long getJavaStreamHandle(int streamId)
    {
        FennelJavaHandle handle =
            (FennelJavaHandle) streamIdToHandleMap.get(new Integer(streamId));
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

    /**
     * Called when a nullable value is cast to a NOT NULL type.
     *
     * @param targetName target expression
     *
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
     *
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
        frame.udrContext = udrContext;

        List stack = getInvocationStack();
        stack.add(frame);
    }

    // implement FarragoSessionRuntimeContext
    public void popRoutineInvocation()
    {
        // TODO jvs 19-Jan-2005:  see corresponding comment in
        // pushRoutineInvocation.

        List stack = getInvocationStack();
        assert(!stack.isEmpty());
        FarragoUdrInvocationFrame frame = (FarragoUdrInvocationFrame)
            stack.remove(stack.size() - 1);
        assert(frame.context == this);
        if (frame.connection != null) {
            try {
                frame.connection.close();
            } catch (SQLException ex) {
                // TODO jvs 19-Jan-2005:  standard mechanism for tracing
                // swallowed exceptions
            }
        }
    }

    // implement FarragoSessionRuntimeContext
    public void cancel()
    {
        if (streamGraph != null) {
            streamGraph.abort();
        }
        isCanceled = true;
    }

    // implement FarragoSessionRuntimeContext
    public void checkCancel()
    {
        if (isCanceled) {
            throw FarragoResource.instance().ExecutionAborted.ex();
        }
    }
    
    // implement FarragoSessionRuntimeContext
    public RuntimeException handleRoutineInvocationException(
        Throwable ex, String methodName)
    {
        // TODO jvs 19-Jan-2005:  special SQLSTATE handling defined
        // in SQL:2003-13-15.1
        return FarragoResource.instance().RoutineInvocationException.ex(
            methodName, ex);
    }

    private static List getInvocationStack()
    {
        List stack = (List) threadInvocationStack.get();
        if (stack == null) {
            stack = new ArrayList();
            threadInvocationStack.set(stack);
        }
        return stack;
    }

    static FarragoUdrInvocationFrame getUdrInvocationFrame()
    {
        List stack = (List) threadInvocationStack.get();
        if ((stack == null) || (stack.isEmpty())) {
            throw new IllegalStateException("No UDR executing.");
        }
        FarragoUdrInvocationFrame frame = peekStackFrame(stack);
        return frame;
    }

    private static FarragoUdrInvocationFrame peekStackFrame(List stack)
    {
        assert(!stack.isEmpty());
        return (FarragoUdrInvocationFrame) stack.get(stack.size() - 1);
    }

    /**
     * Creates a new default connection attached to the session of the current
     * thread.
     */
    public static Connection newConnection()
    {
        List stack = getInvocationStack();
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
                statementClassName, true, statementClassLoader);
        } catch(ClassNotFoundException e) {
            tracer.log(
                Level.SEVERE, 
                "Could not load statement class: " + statementClassName, 
                e);
            return null;
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Inner class for taking care of closing streams without deallocating
     * them.
     */
    private static class StreamOwner extends FarragoCompoundAllocation
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

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
package net.sf.farrago.runtime;

import java.util.*;
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
import net.sf.farrago.type.*;
import net.sf.farrago.type.runtime.*;
import net.sf.farrago.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.util.*;
import org.eigenbase.sql.*;

import java.sql.Date;
import java.io.*;

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
    private FennelStreamGraph streamGraph;
    private long currentTime;

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

        dataWrapperCache =
            new FarragoDataWrapperCache(
                this,
                params.sharedDataWrapperCache,
                session.getPluginClassLoader(),
                params.repos,
                params.fennelTxnContext.getFennelDbHandle());

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
        // make sure all streams get closed BEFORE they are deallocated
        streamOwner.closeAllocation();
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
            throw FarragoResource.instance().newDataServerRuntimeFailed(ex);
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
        // TODO jvs 25-Sept-2004:  once supported
        return "";
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
        // The SQL standard is very precise about the formatting
        SqlDialect dialect = new SqlDialect(session.getDatabaseMetaData());
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        SqlWriter sqlWriter = new SqlWriter(dialect, pw);
        Iterator iter = sessionVariables.schemaSearchPath.iterator();
        while (iter.hasNext()) {
            SqlIdentifier id = (SqlIdentifier) iter.next();
            id.unparse(sqlWriter, 0, 0);
            if (iter.hasNext()) {
                sqlWriter.print(',');
            }
        }
        pw.close();
        return sw.toString();
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
     * Creates a {@link JavaTupleStream} (for feeding the results of an
     * {@link Iterator} to Fennel) and stores it in a handle.
     *
     * <p>This is called at execution time from code generated by
     * {@link net.sf.farrago.query.IteratorToFennelConverter}.
     *
     * @param streamId ID for stream (unique within statement)
     * @param tupleWriter FennelTupleWriter for marshalling tuples
     * @param iter row producer
     *
     * @return dummy object
     */
    public Object newJavaTupleStream(
        int streamId,
        FennelTupleWriter tupleWriter,
        Iterator iter)
    {
        JavaTupleStream stream = new JavaTupleStream(tupleWriter, iter);
        registerJavaStream(streamId, stream);
        return null;
    }

    /**
     * Associates a stream id with a java stream object, so that it can be
     * retrieved by a native method later.
     */
    protected void registerJavaStream(int streamId, Object stream)
    {
        streamIdToHandleMap.put(
            new Integer(streamId),
            FennelDbHandle.allocateNewObjectHandle(this, stream));
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
    public Iterator newFennelIterator(
        FennelTupleReader tupleReader,
        String streamName,
        int streamId,
        Object dummies)
    {
        assert (dummies == null);
        assert (streamGraph != null);

        FennelStreamHandle streamHandle = getStreamHandle(streamName);

        return new FennelIterator(
            tupleReader,
            streamGraph,
            streamHandle,
            repos.getCurrentConfig().getFennelConfig().getCachePageSize());
    }

    protected FennelStreamHandle getStreamHandle(String globalStreamName)
    {
        repos.beginReposTxn(true);
        try {
            return streamGraph.findStream(repos, globalStreamName);
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
     * @param nullableValue source value
     */
    public void checkNotNull(NullableValue nullableValue)
    {
        if (nullableValue.isNull()) {
            throw FarragoResource.instance().newNullNotAllowed();
        }
    }

    /**
    * Called when a nullable value is cast to a NOT NULL type.
    *
    * @param obj source value
    */
    public void checkNotNull(Object obj)
    {
        if (null == obj) {
            throw FarragoResource.instance().newNullNotAllowed();
        }
    }

    // implement FarragoSessionRuntimeContext
    public void pushRoutineInvocation(boolean allowSql)
    {
        // TODO jvs 19-Jan-2005: set system properties sqlj.defaultconnection
        // and sqlj.runtime per SQL:2003 13:12.1.2.  Also other
        // context stuff.

        RoutineInvocationFrame frame = new RoutineInvocationFrame();
        frame.context = this;
        frame.allowSql = allowSql;

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
        RoutineInvocationFrame frame = (RoutineInvocationFrame)
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
    public RuntimeException handleRoutineInvocationException(
        Throwable ex, String methodName)
    {
        // TODO jvs 19-Jan-2005:  special SQLSTATE handling defined
        // in SQL:2003-13-15.1
        return FarragoResource.instance().newRoutineInvocationException(
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

    /**
     * Creates a new default connection attached to the session of the current
     * thread.
     */
    public static Connection newConnection()
    {
        List stack = getInvocationStack();
        if (stack.isEmpty()) {
            throw FarragoResource.instance().newNoDefaultConnection();
        }

        RoutineInvocationFrame frame =
            (RoutineInvocationFrame) stack.get(stack.size() - 1);

        if (!frame.allowSql) {
            throw FarragoResource.instance().newNoDefaultConnection();
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

    /**
     * Inner class for entries on the routine invocation stack.
     */
    private static class RoutineInvocationFrame
    {
        FarragoRuntimeContext context;

        boolean allowSql;

        Connection connection;
    }
}


// End FarragoRuntimeContext.java

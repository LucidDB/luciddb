/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
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

package net.sf.farrago.runtime;

import net.sf.saffron.core.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;
import net.sf.farrago.resource.*;
import net.sf.farrago.namespace.*;
import net.sf.farrago.namespace.util.*;

import net.sf.saffron.util.*;

import java.util.*;
import javax.jmi.reflect.*;

/**
 * FarragoRuntimeContext defines runtime support routines needed by generated
 * code.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FarragoRuntimeContext
    extends FarragoCompoundAllocation
    implements SaffronConnection, FennelJavaStreamMap
{
    private FarragoCatalog catalog;

    private FarragoObjectCache codeCache;

    private Map txnCodeCache;

    private FennelTxnContext fennelTxnContext;

    private Map streamIdToHandleMap = new HashMap();

    private Object [] dynamicParamValues;

    private FarragoCompoundAllocation streamOwner;

    private FarragoIndexMap indexMap;

    private FarragoConnectionDefaults connectionDefaults;

    private FarragoDataWrapperCache dataWrapperCache;
    
    /**
     * Create a new FarragoRuntimeContext.
     *
     * @param catalog catalog storing object definitions
     *
     * @param codeCache cache for Fennel tuple streams
     *
     * @param txnCodeCache txn-private cache for Fennel tuple streams,
     * or null if streams don't need to be pinned by txn
     *
     * @param fennelTxnContext Fennel context for transactions
     *
     * @param indexMap map of indexes which might be accessed
     *
     * @param dynamicParamValues array of values bound to dynamic
     * parameters by position
     *
     * @param connectionDefaults context-dependent settings
     *
     * @param sharedDataWrapperCache FarragoObjectCache to use for
     * caching FarragoMedDataWrapper instances
     */
    public FarragoRuntimeContext(
        FarragoCatalog catalog,
        FarragoObjectCache codeCache,
        Map txnCodeCache,
        FennelTxnContext fennelTxnContext,
        FarragoIndexMap indexMap,
        Object [] dynamicParamValues,
        FarragoConnectionDefaults connectionDefaults,
        FarragoObjectCache sharedDataWrapperCache)
    {
        this.catalog = catalog;
        this.codeCache = codeCache;
        this.txnCodeCache = txnCodeCache;
        this.fennelTxnContext = fennelTxnContext;
        this.indexMap = indexMap;
        this.dynamicParamValues = dynamicParamValues;
        this.connectionDefaults = connectionDefaults;

        dataWrapperCache = new FarragoDataWrapperCache(
            this,
            sharedDataWrapperCache,
            catalog);

        streamOwner = new StreamOwner();
    }

    // implement SaffronConnection
    public SaffronSchema getSaffronSchema()
    {
        assert(false);
        return null;
    }

    // override FarragoCompoundAllocation
    public void closeAllocation()
    {
        // make sure all streams get closed BEFORE they are deallocated
        streamOwner.closeAllocation();
        super.closeAllocation();
    }
    
    // implement SaffronConnection
    public Object contentsAsArray(String qualifier,String tableName)
    {
        assert(false);
        return null;
    }
    
    /**
     * Get an object needed to support the implementation of foreign
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
        FemDataServerImpl femServer = (FemDataServerImpl)
            catalog.getRepository().getByMofId(serverMofId);

        FarragoMedDataServer server = 
            femServer.loadFromCache(dataWrapperCache);
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
     * Get the MofId for a RefBaseObject, or null if the object is null.  This
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
     * Get the value bound to a dynamic parameter.
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
        return connectionDefaults.currentUserName;
    }
    
    /**
     * Called from generated code.
     *
     * @return the value of context variable CURRENT_USER.
     */
    public String getContextVariable_CURRENT_USER()
    {
        return connectionDefaults.currentUserName;
    }
    
    /**
     * Called from generated code.
     *
     * @return the value of context variable SYSTEM_USER.
     */
    public String getContextVariable_SYSTEM_USER()
    {
        return connectionDefaults.systemUserName;
    }
    
    /**
     * Called from generated code.
     *
     * @return the value of context variable SESSION_USER.
     */
    public String getContextVariable_SESSION_USER()
    {
        return connectionDefaults.sessionUserName;
    }
    
    /**
     * Create a JavaTupleStream (for feeding the results of an Iterator to
     * Fennel) and store it in a handle.  This is called at execution from
     * code generated by IteratorToFennelConverter.
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
        if (!catalog.isFennelEnabled()) {
            return null;
        }

        JavaTupleStream stream = new JavaTupleStream(tupleWriter,iter);

        streamIdToHandleMap.put(
            new Integer(streamId),
            getFennelDbHandle().allocateNewObjectHandle(
                this,stream));
                
        return null;
    }

    /**
     * Stupid helper for code generated by FennelDoubleRel.
     *
     * @param dummy1 a dummy
     * @param dummy2 another dummy
     * @return yet another dummy
     */
    public Object dummyPair(Object dummy1,Object dummy2)
    {
        assert(dummy1 == null);
        assert(dummy2 == null);
        return null;
    }

    /**
     * Create a FennelIterator for executing a plan represented as XML.  This
     * is called at execution from code generated by
     * FennelToIteratorConverter.
     *
     * @param tupleReader object providing FennelTupleReader implementation
     * @param xmiString XMI string representation of plan
     * @param dummies a dummy parameter to give non-Fennel children a place to
     *        generate code
     *
     * @return iterator
     */
    public Iterator newFennelIterator(
        FennelTupleReader tupleReader,
        final String xmiString,
        Object dummies)
    {
        if (!catalog.isFennelEnabled()) {
            return Collections.EMPTY_LIST.iterator();
        }

        assert (dummies == null);

        FarragoObjectCache.CachedObjectFactory streamFactory = new
            FarragoObjectCache.CachedObjectFactory()
            {
                public void initializeEntry(
                    Object key,
                    FarragoObjectCache.UninitializedEntry entry)
                {
                    assert(key.equals(xmiString));
                    FennelStreamHandle hStream = prepareTupleStream(xmiString);
                    // TODO:  proper memory accounting
                    long memUsage = FarragoUtil.getStringMemoryUsage(xmiString);
                    entry.initialize(hStream,memUsage);
                }
            };

        FarragoObjectCache.Entry cacheEntry = null;
        if (txnCodeCache != null) {
            cacheEntry = (FarragoObjectCache.Entry)
                txnCodeCache.get(xmiString);
        }
        if (cacheEntry == null) {
            cacheEntry =
                codeCache.pin(xmiString,streamFactory,true);
        }

        if (txnCodeCache == null) {
            addAllocation(cacheEntry);
        } else {
            txnCodeCache.put(xmiString,cacheEntry);
        }
        
        FennelStreamHandle hStream =
            (FennelStreamHandle) cacheEntry.getValue();

        // NOTE:  This will cause hStream to be added twice for the uncached
        // case, but this isn't a problem since it's legal to close
        // a stream more than once.
        streamOwner.addAllocation(hStream);

        hStream.open(fennelTxnContext.getTxnHandle(),this);
        return new FennelIterator(
            tupleReader,
            hStream,
            catalog.getCurrentConfig().getFennelConfig().getCachePageSize());
    }

    private FennelStreamHandle prepareTupleStream(String xmiString)
    {
        // need a repository txn for import
        catalog.getRepository().beginTrans(true);
        try {
            Collection collection = JmiUtil.importFromXmiString(
                catalog.farragoPackage,xmiString);
            assert (collection.size() == 1);
            FemCmdPrepareExecutionStreamGraph cmd =
                (FemCmdPrepareExecutionStreamGraph)
                collection.iterator().next();

            Iterator streamIter = cmd.getStreamDefs().iterator();
            while (streamIter.hasNext()) {
                setCacheQuotas((FemTupleStreamDef) streamIter.next());
            }
            cmd.setTxnHandle(fennelTxnContext.getTxnHandle());
            // REVIEW:  here's a potential window for leaks;
            // if an excn is thrown before this stream gets cached,
            // the stream will be closed but not deallocated
            return getFennelDbHandle().prepareTupleStream(
                streamOwner,cmd);
        } finally {
            catalog.getRepository().endTrans();
        }
    }

    // implement FennelJavaStreamMap
    public long getJavaStreamHandle(int streamId)
    {
        FennelJavaHandle handle = (FennelJavaHandle)
            streamIdToHandleMap.get(new Integer(streamId));
        assert(handle != null);
        return handle.getLongHandle();
    }

    // implement FennelJavaStreamMap
    public long getIndexRoot(long pageOwnerId)
    {
        CwmSqlindex index = indexMap.getIndexById(pageOwnerId);
        return indexMap.getIndexRoot(index);
    }
    
    /**
     * .
     *
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
    public static void checkNotNull(NullableValue nullableValue)
    {
        if (nullableValue.isNull()) {
            throw FarragoResource.instance().newNullNotAllowed();
        }
    }

    private void setCacheQuotas(FemTupleStreamDef streamDef)
    {
        assert (streamDef.getCachePageMin() <= streamDef.getCachePageMax());

        // TODO:  set quotas based on current cache state; for now just set to
        // minimum for testing
        streamDef.setCachePageQuota(streamDef.getCachePageMin());
    }

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
                FennelStreamHandle hStream = (FennelStreamHandle)
                    iter.previous();
                hStream.close();
            }
            allocations.clear();
        }
    }
}

// End FarragoRuntimeContext.java

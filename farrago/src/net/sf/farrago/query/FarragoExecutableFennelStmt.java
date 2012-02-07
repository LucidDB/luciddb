/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.query;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.runtime.*;
import org.eigenbase.util.*;


/**
 * FarragoExecutableFennelStmt implements FarragoSessionExecutableStmt by
 * executing a pure Fennel statement that requires no compiled Java classes.
 *
 * <p>NOTE: be sure to read superclass warnings before modifying this class.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FarragoExecutableFennelStmt
    extends FarragoExecutableStmtImpl
{
    //~ Instance fields --------------------------------------------------------

    protected final RelDataType rowType;
    protected final List<List<String>> fieldOrigins;
    protected final String xmiFennelPlan;
    private final Map<String, String> referencedObjectTimestampMap;
    private final String streamName;
    private final Map<String, RelDataType> resultSetTypeMap;

    //~ Constructors -----------------------------------------------------------

    FarragoExecutableFennelStmt(
        RelDataType preparedRowType,
        List<List<String>> fieldOrigins,
        RelDataType dynamicParamRowType,
        String xmiFennelPlan,
        String streamName,
        boolean isDml,
        TableModificationRel.Operation tableModOp,
        Map<String, String> referencedObjectTimestampMap,
        TableAccessMap tableAccessMap,
        Map<String, RelDataType> typeMap)
    {
        super(dynamicParamRowType, isDml, tableModOp, tableAccessMap);

        this.fieldOrigins = fieldOrigins;
        this.xmiFennelPlan = xmiFennelPlan;
        this.streamName = streamName;
        this.referencedObjectTimestampMap = referencedObjectTimestampMap;
        this.resultSetTypeMap = typeMap;
        this.rowType = preparedRowType;

        assert fieldOrigins == null
           || fieldOrigins.size() == rowType.getFieldCount()
            : fieldOrigins + "; " + rowType;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public RelDataType getRowType()
    {
        return rowType;
    }

    // implement FarragoSessionExecutableStmt
    public List<List<String>> getFieldOrigins()
    {
        return fieldOrigins;
    }

    // implement FarragoSessionExecutableStmt
    public Set<String> getReferencedObjectIds()
    {
        return referencedObjectTimestampMap.keySet();
    }

    // implement FarragoSessionExecutableStmt
    public String getReferencedObjectModTime(String mofid)
    {
        return referencedObjectTimestampMap.get(mofid);
    }

    // implement FarragoSessionExecutableStmt
    public ResultSet execute(FarragoSessionRuntimeContext runtimeContext)
    {
        try {
            runtimeContext.loadFennelPlan(xmiFennelPlan);

            FennelTupleDescriptor tupleDesc =
                FennelRelUtil.convertRowTypeToFennelTupleDesc(
                    rowType);
            FennelTupleData tupleData = new FennelTupleData(tupleDesc);
            FennelTupleReader tupleReader =
                new FennelOnlyTupleReader(tupleDesc, tupleData);
            FennelStreamHandle streamHandle;
            int cachePageSize;

            FarragoReposTxnContext txn =
                runtimeContext.getRepos().newTxnContext(true);
            txn.beginReadTxn();
            try {
                streamHandle = runtimeContext.getStreamHandle(streamName, true);
                cachePageSize =
                    runtimeContext.getRepos().getCurrentConfig()
                    .getFennelConfig().getCachePageSize();
            } finally {
                txn.commit();
            }

            TupleIter tupleIter =
                new FennelTupleIter(
                    tupleReader,
                    runtimeContext.getFennelStreamGraph(),
                    streamHandle,
                    cachePageSize);
            FennelOnlyResultSet resultSet =
                new FennelOnlyResultSet(
                    tupleIter,
                    rowType,
                    fieldOrigins,
                    runtimeContext);

            // Finally, it's safe to open all streams.
            runtimeContext.openStreams();

            runtimeContext = null;

            resultSet.setOpened();

            return resultSet;
        } catch (UnsupportedOperationException e) {
            throw Util.newInternal(e);
        } finally {
            if (runtimeContext != null) {
                runtimeContext.closeAllocation();
            }
        }
    }

    // implement FarragoSessionExecutableStmt
    public long getMemoryUsage()
    {
        int xmiSize = FarragoUtil.getStringMemoryUsage(xmiFennelPlan);
        if (tracer.isLoggable(Level.FINE)) {
            tracer.fine("XMI Fennel plan size = " + xmiSize + " bytes");
        }

        // Account for half of the XMI plan here since this cache entry holds
        // a pointer to that plan.  The other half will be accounted for in the
        // object associated with the Fennel XMI entry itself.  That entry may
        // be flushed from the cache while this entry is still in cache.  So,
        // we want to need to account for the memory in both entries.  But, at
        // the same time, we don't want to account for the entire size with
        // both, as that would double count the memory.
        return xmiSize / 2;
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getResultSetTypeMap()
    {
        return resultSetTypeMap;
    }
}

// End FarragoExecutableFennelStmt.java

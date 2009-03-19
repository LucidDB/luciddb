/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
package net.sf.farrago.query;

import java.sql.*;

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.*;
import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
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
    protected final String xmiFennelPlan;
    private final Map<String, String> referencedObjectTimestampMap;
    private final String streamName;
    private final Map<String, RelDataType> resultSetTypeMap;

    //~ Constructors -----------------------------------------------------------

    FarragoExecutableFennelStmt(
        RelDataType preparedRowType,
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

        this.xmiFennelPlan = xmiFennelPlan;
        this.streamName = streamName;
        this.referencedObjectTimestampMap = referencedObjectTimestampMap;
        this.resultSetTypeMap = typeMap;

        rowType = preparedRowType;
    }

    //~ Methods ----------------------------------------------------------------

    // implement FarragoSessionExecutableStmt
    public RelDataType getRowType()
    {
        return rowType;
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
                    runtimeContext,
                    new FarragoResultSetMetaData(rowType));

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

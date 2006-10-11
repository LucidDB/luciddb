/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2006 The Eigenbase Project
// Copyright (C) 2005-2006 Disruptive Tech
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
package net.sf.farrago.query;

import java.sql.*;

import java.util.*;

import net.sf.farrago.fennel.tuple.*;
import net.sf.farrago.runtime.*;
import net.sf.farrago.session.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

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

    private final RelDataType rowType;
    private final String xmiFennelPlan;
    private final String streamName;
    private final Set<String> referencedObjectIds;

    //~ Constructors -----------------------------------------------------------

    FarragoExecutableFennelStmt(
        RelDataType preparedRowType,
        RelDataType dynamicParamRowType,
        String xmiFennelPlan,
        String streamName,
        boolean isDml,
        Set<String> referencedObjectIds,
        TableAccessMap tableAccessMap)
    {
        super(dynamicParamRowType, isDml, tableAccessMap);

        assert(xmiFennelPlan != null);
        this.xmiFennelPlan = xmiFennelPlan;
        this.streamName = streamName;
        this.referencedObjectIds = referencedObjectIds;

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
        return referencedObjectIds;
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
            TupleIter tupleIter =
                new FennelTupleIter(
                    tupleReader,
                    runtimeContext.getFennelStreamGraph(),
                    runtimeContext.getStreamHandle(streamName, true),
                    runtimeContext.getRepos().getCurrentConfig().
                        getFennelConfig().getCachePageSize());
            ResultSet resultSet =
                new FennelOnlyResultSet(
                    tupleIter,
                    rowType,
                    runtimeContext,
                    new FarragoResultSetMetaData(rowType));

            // Finally, it's safe to open all streams.
            runtimeContext.openStreams();

            runtimeContext = null;
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
        return FarragoUtil.getStringMemoryUsage(xmiFennelPlan);
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getResultSetTypeMap()
    {
        return Collections.emptyMap();
    }

    // implement FarragoSessionExecutableStmt
    public Map<String, RelDataType> getIterCalcTypeMap()
    {
        return Collections.emptyMap();
    }
}

// End FarragoExecutableFennelStmt.java

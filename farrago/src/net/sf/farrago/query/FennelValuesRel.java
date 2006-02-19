/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.type.*;
import org.eigenbase.util.*;

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.catalog.*;
import net.sf.farrago.fennel.tuple.*;

import openjava.ptree.*;

import java.util.*;
import java.io.*;
import java.nio.*;
import java.math.*;

import java.util.List;

/**
 * FennelValuesRel represents the Fennel implementation of VALUES where all
 * values are literals.  It corresponds to fennel::ValuesExecStream via {@link
 * ValuesStreamDef}.  It guarantees the order of the tuples it produces, making
 * it usable for such purposes as the search input to an index scan, where
 * order may matter for both performance and correctness.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelValuesRel extends AbstractRelNode implements FennelRel
{
    private final List<List<RexLiteral>> tuples;

    public FennelValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples)
    {
        super(cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION));
        this.rowType = rowType;
        this.tuples = tuples;
    }

    // override Object
    public Object clone()
    {
        // immutable with no children
        return this;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        return rowType;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeTinyCost();
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "tuples" },
            new Object [] { tuples });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemValuesStreamDef streamDef =
            repos.newFemValuesStreamDef();

        FennelTupleDescriptor tupleDesc =
            FennelRelUtil.convertRowTypeToFennelTupleDesc(rowType);
        FennelTupleData tupleData = new FennelTupleData(tupleDesc);

        // TODO jvs 18-Feb-2006:  query Fennel to get alignment and
        // DEBUG_TUPLE_ACCESS?
        FennelTupleAccessor tupleAccessor = new FennelTupleAccessor();
        tupleAccessor.compute(tupleDesc);
        ByteBuffer tupleBuffer = ByteBuffer.allocate(
            tupleAccessor.getMaxByteCount());
        tupleBuffer.order(ByteOrder.nativeOrder());

        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();

        for (List<RexLiteral> tuple : tuples) {
            int i = 0;
            tupleBuffer.clear();
            for (RexLiteral literal : tuple) {
                FennelTupleDatum datum = tupleData.getDatum(i++);
                datum.reset();
                if (!RexLiteral.isNullLiteral(literal)) {
                    // TODO:  handle non-numerics
                    BigDecimal bigDecimal = (BigDecimal) literal.getValue();
                    datum.setLong(bigDecimal.unscaledValue().longValue());
                }
            }
            tupleAccessor.marshal(tupleData, tupleBuffer);
            tupleBuffer.flip();
            byteStream.write(
                tupleBuffer.array(),
                0,
                tupleAccessor.getBufferByteCount(tupleBuffer));
        }

        byte [] tupleBytes = byteStream.toByteArray();
        String base64 = RhBase64.encodeBytes(
            tupleBytes, RhBase64.DONT_BREAK_LINES);

        streamDef.setTupleBytesBase64(base64);

        return streamDef;
    }
    
    // implement FennelRel
    public Object implementFennelChild(FennelRelImplementor implementor)
    {
        return Literal.constantNull();
    }

    public RelFieldCollation[] getCollations()
    {
        // TODO:  if tuples.size() == 1, say it's trivially sorted
        return RelFieldCollation.emptyCollationArray;
    }
}

// End FennelValuesRel.java

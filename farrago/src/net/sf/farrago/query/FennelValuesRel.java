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
import org.eigenbase.rel.metadata.*;
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

    /**
     * Creates a new FennelValuesRel.  Note that tuples passed in become owned
     * by this rel (without a deep copy), so caller must not modify them after
     * this call, otherwise bad things will happen.
     *
     * @param cluster .
     *
     * @param rowType row type for tuples produced by this rel
     *
     * @param tuples 2-dimensional array of tuple values to be produced; outer
     * list contains tuples; each inner list is one tuple; all tuples must be
     * of same length, conforming to rowType
     */
    public FennelValuesRel(
        RelOptCluster cluster,
        RelDataType rowType,
        List<List<RexLiteral>> tuples)
    {
        super(cluster, new RelTraitSet(FENNEL_EXEC_CONVENTION));
        this.rowType = rowType;
        this.tuples = tuples;
        assert(assertRowType());
    }

    /**
     * @return true if all tuples match rowType; otherwise, assert
     * on mismatch
     */
    private boolean assertRowType()
    {
        for (List<RexLiteral> tuple : tuples) {
            RelDataTypeField [] fields = rowType.getFields();
            assert(tuple.size() == fields.length);
            int i = 0;
            for (RexLiteral literal : tuple) {
                RelDataType fieldType = fields[i++].getType();
                // TODO jvs 19-Feb-2006: strengthen this a bit.  For example,
                // overflow, rounding, and truncation must already have been
                // dealt with.
                if (!RexLiteral.isNullLiteral(literal)) {
                    assert(
                        SqlTypeUtil.canAssignFrom(
                            fieldType,
                            literal.getType()));
                }
            }
        }
        return true;
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
        double dRows = RelMetadataQuery.getRowCount(this);
        // CPU is negligible since ValuesExecStream just hands off
        // the entire buffer to its consumer.
        double dCpu = 1;
        double dIo = 0;
        return planner.makeCost(dRows, dCpu, dIo);
    }
    
    // implement RelNode
    public double getRows()
    {
        return tuples.size();
    }

    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        // A little adapter just to get the tuples to come out
        // with curly brackets instead of square brackets.  Plus
        // more whitespace for readability.
        List renderList = new ArrayList();
        for (List<RexLiteral> tuple : tuples) {
            String s = tuple.toString();
            assert(s.startsWith("["));
            assert(s.endsWith("]"));
            renderList.add("{ " + s.substring(1, s.length() - 1) + " }");
        }
        pw.explain(
            this,
            new String [] { "tuples" },
            new Object [] { renderList });
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
        // DEBUG_TUPLE_ACCESS?  And maybe we should always use network
        // byte order in case this plan is going to get shipped
        // somewhere else?
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
                FennelTupleDatum datum = tupleData.getDatum(i);
                RelDataType fieldType = rowType.getFields()[i].getType();
                ++i;
                // Start with a null.
                datum.reset();
                if (RexLiteral.isNullLiteral(literal)) {
                    continue;
                }
                Comparable value = literal.getValue();
                if (value instanceof BigDecimal) {
                    BigDecimal bigDecimal = (BigDecimal) value;
                    switch (fieldType.getSqlTypeName().getOrdinal()) {
                    case SqlTypeName.Real_ordinal:
                        datum.setFloat(bigDecimal.floatValue());
                        break;
                    case SqlTypeName.Float_ordinal:
                    case SqlTypeName.Double_ordinal:
                        datum.setDouble(bigDecimal.doubleValue());
                        break;
                    default:
                        datum.setLong(bigDecimal.unscaledValue().longValue());
                        break;
                    }
                } else if (value instanceof Calendar) {
                    Calendar cal = (Calendar) value;
                    // TODO:  eventually, timezone
                    datum.setLong(cal.getTimeInMillis());
                } else if (value instanceof NlsString) {
                    NlsString nlsString = (NlsString) value;
                    datum.setString(nlsString.getValue());
                } else {
                    assert(value instanceof ByteBuffer);
                    ByteBuffer byteBuffer = (ByteBuffer) value;
                    datum.setBytes(byteBuffer.array());
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

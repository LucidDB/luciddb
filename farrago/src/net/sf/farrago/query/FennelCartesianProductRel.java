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

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;

/**
 * FennelCartesianProductRel represents the Fennel implementation of Cartesian
 * product.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelCartesianProductRel extends FennelPullDoubleRel
{
    /**
     * Creates a new FennelCartesianProductRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param left left input
     * @param right right input
     */
    public FennelCartesianProductRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right)
    {
        super(cluster,left,right);
    }

    // implement Cloneable
    public Object clone()
    {
        return new FennelCartesianProductRel(
            cluster,
            RelOptUtil.clone(left),
            RelOptUtil.clone(right));
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  account for buffering I/O and CPU
        double rowCount = getRows();
        return planner.makeCost(
            rowCount,
            0,
            rowCount*getRowType().getFieldCount());
    }

    // implement RelNode
    public double getRows()
    {
        return left.getRows()*right.getRows();
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left","right" },
            new Object [] {});
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FemCartesianProductStreamDef streamDef =
            getCatalog().newFemCartesianProductStreamDef();

        FemExecutionStreamDef leftInput =
            implementor.visitFennelChild((FennelRel) left);
        streamDef.getInput().add(leftInput);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right);

        // TODO: For now we always buffer the right-hand input.  In most
        // cases, this is good for performance; in some cases it is required
        // for correctness.  The performance part is obvious: we only need to
        // compute the right-hand side once, and since we store only what we
        // need, we may save I/O.  However, there are counterexamples; if the
        // right-hand side is a table scan with no filtering or projection,
        // there's no point buffering it.  So we should produce plan variants
        // with and without buffering and use cost to decide.  However, before
        // we can do that, we have to fix the correctness part.  Namely,
        // any Java implementation in the right-hand side is not restartable
        // since JavaTupleStream doesn't support that yet.
        boolean needBuffer = true;

        if (needBuffer) {
            FemBufferingTupleStreamDef buffer =
                getCatalog().newFemBufferingTupleStreamDef();
            buffer.setInMemory(false);
            buffer.setMultipass(true);

            buffer.getInput().add(rightInput);

            streamDef.getInput().add(buffer);
        } else {
            streamDef.getInput().add(rightInput);
        }

        return streamDef;
    }

    // TODO:  implement getCollations()
}

// End FennelCartesianProductRel.java

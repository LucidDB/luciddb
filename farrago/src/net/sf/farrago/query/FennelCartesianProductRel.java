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

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;

/**
 * FennelCartesianProductRel represents the Fennel implementation of Cartesian
 * product.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelCartesianProductRel extends FennelDoubleRel
{
    /**
     * Creates a new FennelCartesianProductRel object.
     *
     * @param cluster VolcanoCluster for this rel
     * @param left left input
     * @param right right input
     */
    public FennelCartesianProductRel(
        VolcanoCluster cluster,
        SaffronRel left,
        SaffronRel right)
    {
        super(cluster,left,right);
    }

    // implement Cloneable
    public Object clone()
    {
        return new FennelCartesianProductRel(
            cluster,
            OptUtil.clone(left),
            OptUtil.clone(right));
    }

    // implement SaffronRel
    public PlanCost computeSelfCost(SaffronPlanner planner)
    {
        // TODO:  account for buffering I/O and CPU
        double rowCount = getRows();
        return planner.makeCost(
            rowCount,
            0,
            rowCount*getRowType().getFieldCount());
    }

    // implement SaffronRel
    public double getRows()
    {
        return left.getRows()*right.getRows();
    }
    
    // override SaffronRel
    public void explain(PlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left","right" },
            new Object [] {});
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FarragoRelImplementor implementor)
    {
        FemCartesianProductStreamDef streamDef =
            getCatalog().newFemCartesianProductStreamDef();

        // REVIEW: this is to get the buffer provisioning right, but it
        // ought to be accounted for somewhere else, not here
        streamDef.setCachePageMin(1);
        streamDef.setCachePageMax(1);
        
        streamDef.getInput().add(implementor.implementFennelRel(left));
        FemExecutionStreamDef rightInput =
            implementor.implementFennelRel(right);

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

            // Need one page for buffered stream access.  TODO: once
            // BufferingTupleStream supports SpillOutputStream, use it.  Here,
            // we would set the maximum number of pages based on the estimated
            // or known size of the buffered data.  If the optimizer can
            // guarantee a limit, and we can actually get the requested number
            // of pages, use a scratch segment with no backing store.
            // Otherwise, use a SpillOutputStream with as many scratch pages as
            // we can get.
            buffer.setCachePageMin(1);
            buffer.setCachePageMax(1);
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

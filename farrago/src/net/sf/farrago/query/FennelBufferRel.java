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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * FennelBufferRel represents the Fennel implementation of a buffering stream.
 * product.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FennelBufferRel
    extends FennelSingleRel
{
    //~ Instance fields --------------------------------------------------------

    boolean inMemory;
    boolean multiPass;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelBufferRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param child child input
     * @param inMemory true if the buffering needs to done only in memory
     * @param multiPass true if the buffer output will be read multiple times
     */
    public FennelBufferRel(
        RelOptCluster cluster,
        RelNode child,
        boolean inMemory,
        boolean multiPass)
    {
        super(cluster, child);
        this.inMemory = inMemory;
        this.multiPass = multiPass;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelBufferRel clone()
    {
        FennelBufferRel clone =
            new FennelBufferRel(
                getCluster(),
                getChild().clone(),
                inMemory,
                multiPass);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double rowCount = RelMetadataQuery.getRowCount(getChild());

        // NOTE zfong 9/6/06 - For now, I've arbitrarily set the I/O factor
        // to 1/10 the rowcount, which means that there are 10 rows per
        // page
        return planner.makeCost(
            rowCount,
            rowCount,
            rowCount / 10);
    }

    // implement RelNode
    public double getRows()
    {
        return RelMetadataQuery.getRowCount(getChild());
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "child", "inMemory", "multiPass" },
            new Object[] { inMemory, multiPass });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemBufferingTupleStreamDef streamDef =
            repos.newFemBufferingTupleStreamDef();

        FemExecutionStreamDef childInput =
            implementor.visitFennelChild((FennelRel) getChild(), 0);
        implementor.addDataFlowFromProducerToConsumer(
            childInput,
            streamDef);
        streamDef.setInMemory(inMemory);
        streamDef.setMultipass(multiPass);
        return streamDef;
    }
}

// End FennelBufferRel.java

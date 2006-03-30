/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
import org.eigenbase.reltype.*;

/**
 * FennelMergeRel represents the Fennel implementation of UNION ALL.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelMergeRel extends FennelMultipleRel
{
    /**
     * Creates a new FennelMergeRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param inputs array of inputs
     */
    FennelMergeRel(
        RelOptCluster cluster,
        RelNode[] inputs)
    {
        super(cluster, inputs);
    }

    // implement RelNode
    public double getRows()
    {
        return UnionRelBase.estimateRowCount(this);
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        return planner.makeCost(
            RelMetadataQuery.getRowCount(this), 0.0, 0.0);
    }
    
    // implement RelNode
    protected RelDataType deriveRowType()
    {
        // input to merge is supposed to be homogeneous, so just take
        // the first input type
        return inputs[0].getRowType();
    }
    
    // implement RelNode
    public Object clone()
    {
        FennelMergeRel clone =
            new FennelMergeRel(
                getCluster(),
                RelOptUtil.clone(inputs));
        clone.inheritTraitsFrom(this);
        return clone;
    }
    
    // implement RelNode
    public void explain(RelOptPlanWriter pw)
    {
        String[] names = new String[inputs.length];

        for(int i = 0; i < inputs.length; i++) {
            names[i] = "child#" + i;
        }
        pw.explain(
            this, names, new Object[] { });
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        
        FemMergeStreamDef mergeStream =
            repos.newFemMergeStreamDef();
        mergeStream.setSequential(true);

        for (int i = 0; i < inputs.length; i++) {
            FemExecutionStreamDef inputStream =
                implementor.visitFennelChild((FennelRel) inputs[i]);
            implementor.addDataFlowFromProducerToConsumer(
                inputStream,
                mergeStream);
        }

        return mergeStream;
    }
}

// End FennelMergeRel.java

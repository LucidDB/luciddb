/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.disruptivetech.farrago.rel;

import net.sf.farrago.query.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.catalog.FarragoRepos;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.JoinRel;
import org.eigenbase.rel.CorrelatorRel;
import org.eigenbase.reltype.RelDataType;

import java.util.*;

/**
 * FennelPullCorrelatorRel is the relational expression corresponding to a
 * correlation between two streams implemented inside of Fennel.
 *
 * @author Wael Chatila
 * @since Feb 1, 2005
 * @version $Id$
 */
public class FennelPullCorrelatorRel extends FennelDoubleRel
{
    //~ Instance fields -------------------------------------------------------

    protected List correlations;

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a Correlator.
     *
     * @param cluster {@link RelOptCluster} this relational expression
     *        belongs to
     * @param left left input relational expression
     * @param right right  input relational expression
     * @param correlations set of expressions to set as variables each time a
     *        row arrives from the left input
     */
    public FennelPullCorrelatorRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        List correlations)
    {
        super(cluster, left, right);
        this.correlations = correlations;
    }

    //~ Methods ---------------------------------------------------------------

    // implement Cloneable
    public Object clone()
    {
        FennelPullCorrelatorRel clone = new FennelPullCorrelatorRel(
            getCluster(),
            RelOptUtil.clone(left),
            RelOptUtil.clone(right),
            cloneCorrelations());
        clone.inheritTraitsFrom(this);
        return clone;
    }

    public List cloneCorrelations()
    {
        return new ArrayList(correlations);
    }
    
    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left", "right" },
            new Object [] {  });
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        double rowCount = getRows();
        return planner.makeCost(rowCount, 0,
            rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        return left.getRows() * right.getRows();
    }

    protected RelDataType deriveRowType()
    {
        return JoinRel.deriveJoinRowType(
            left, right, JoinRel.JoinType.INNER, getCluster().getTypeFactory());
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCorrelationJoinStreamDef streamDef =
            repos.newFemCorrelationJoinStreamDef();

        for (int i = 0; i < correlations.size(); i++) {
            CorrelatorRel.Correlation correlation =
                (CorrelatorRel.Correlation) correlations.get(i);
            FemCorrelation newFemCorrelation = repos.newFemCorrelation();
            newFemCorrelation.setId(correlation.getId());
            newFemCorrelation.setOffset(correlation.getOffset());
            streamDef.getCorrelations().add(newFemCorrelation);
        }

        FemExecutionStreamDef leftInput =
            implementor.visitFennelChild((FennelRel) left);
        implementor.addDataFlowFromProducerToConsumer(
            leftInput,
            streamDef);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right);
        implementor.addDataFlowFromProducerToConsumer(
            rightInput,
            streamDef);

        return streamDef;
    }
}


// End FennelPullCorrelatorRel.java

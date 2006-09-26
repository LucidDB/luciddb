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

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * FennelCartesianProductRel represents the Fennel implementation of Cartesian
 * product.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelCartesianProductRel
    extends FennelDoubleRel
{

    //~ Instance fields --------------------------------------------------------

    private final JoinRelType joinType;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelCartesianProductRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param left left input
     * @param right right input
     * @param fieldNameList If not null, the row type will have these field
     * names
     */
    public FennelCartesianProductRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        List<String> fieldNameList)
    {
        super(cluster, left, right);
        assert joinType != null;
        this.joinType = joinType;
        this.rowType =
            JoinRel.deriveJoinRowType(
                left.getRowType(),
                right.getRowType(),
                joinType,
                cluster.getTypeFactory(),
                fieldNameList);
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public FennelCartesianProductRel clone()
    {
        FennelCartesianProductRel clone =
            new FennelCartesianProductRel(
                getCluster(),
                left.clone(),
                right.clone(),
                joinType,
                RelOptUtil.getFieldNameList(rowType));
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  account for buffering I/O and CPU
        double rowCount = RelMetadataQuery.getRowCount(this);
        return
            planner.makeCost(rowCount,
                0,
                rowCount * getRowType().getFieldList().size());
    }

    // implement RelNode
    public double getRows()
    {
        return
            RelMetadataQuery.getRowCount(left)
            * RelMetadataQuery.getRowCount(right);
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String[] { "left", "right", "leftouterjoin" },
            new Object[] { isLeftOuter() });
    }

    private boolean isLeftOuter()
    {
        return JoinRelType.LEFT == joinType;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        throw Util.newInternal("row type should have been set already");
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        FarragoRepos repos = FennelRelUtil.getRepos(this);
        FemCartesianProductStreamDef streamDef =
            repos.newFemCartesianProductStreamDef();

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
        streamDef.setLeftOuter(isLeftOuter());
        return streamDef;
    }

    // TODO:  implement getCollations()
}

// End FennelCartesianProductRel.java

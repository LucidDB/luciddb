/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.util.*;


/**
 * LhxJoinRel implements the hash join.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxJoinRel
    extends FennelDoubleRel
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Join type. Currently only inner join is supported.
     */
    private final LhxJoinRelType joinType;

    /**
     * Join key columns from the left.
     */
    List<Integer> leftKeys;

    /**
     * Join key columns from the right.
     */
    List<Integer> rightKeys;

    /**
     * row count on the build side
     */
    long numBuildRows;

    /**
     * cardinality of the build key
     */
    long cndBuildKey;

    /**
     * This LhxJoinRel implements setop, one of the following: intersect
     * (distinct), except (distinct) Setop differs from regular join in its
     * treatment of NULLs and duplicates: NULLs are considered "matching" and
     * duplicates are removed by default(the default setop is setop DISTINCT).
     */
    boolean isSetop;

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
    public LhxJoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        LhxJoinRelType joinType,
        boolean isSetop,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<String> fieldNameList,
        long numBuildRows,
        long cndBuildKey)
    {
        super(cluster, left, right);
        assert joinType != null;
        this.joinType = joinType;
        this.isSetop = isSetop;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        if (joinType == LhxJoinRelType.LEFTSEMI) {
            // intersect is implemented using left semi join
            this.rowType = left.getRowType();
        } else if (joinType == LhxJoinRelType.RIGHTANTI) {
            // except is implemented using right anti join
            this.rowType = right.getRowType();
        } else {
            // regular join
            this.rowType =
                JoinRel.deriveJoinRowType(
                    left.getRowType(),
                    right.getRowType(),
                    joinType.getLogicalJoinType(),
                    cluster.getTypeFactory(),
                    fieldNameList);
        }
        this.numBuildRows = numBuildRows;
        this.cndBuildKey = cndBuildKey;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public Object clone()
    {
        LhxJoinRel clone =
            new LhxJoinRel(
                getCluster(),
                RelOptUtil.clone(left),
                RelOptUtil.clone(right),
                joinType,
                isSetop,
                leftKeys,
                rightKeys,
                RelOptUtil.getFieldNameList(rowType),
                numBuildRows,
                cndBuildKey);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  account for buffering I/O and CPU
        double rowCount = RelMetadataQuery.getRowCount(this);
        double joinSelectivity = 0.1;
        return
            planner.makeCost(rowCount * joinSelectivity,
                0,
                rowCount * getRowType().getFieldList().size()
                * joinSelectivity);
    }

    // implement RelNode
    public double getRows()
    {
        return RelMetadataQuery.getRowCount(this);
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        if (!isSetop) {
            pw.explain(
                this,
                new String[] {
                    "left", "right", "leftKeys", "rightKeys", "joinType"
                },
                new Object[] { leftKeys, rightKeys, joinType });
        } else {
            pw.explain(
                this,
                new String[] {
                    "left", "right", "leftKeys", "rightKeys", "joinType", "setop"
                },
                new Object[] { leftKeys, rightKeys, joinType, isSetop });
        }
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
        FemLhxJoinStreamDef streamDef = repos.newFemLhxJoinStreamDef();

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

        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                this.getRowType()));

        streamDef.setNumBuildRows(numBuildRows);
        streamDef.setCndBuildKeys(cndBuildKey);

        // From join types, derive whether to return matching or non-matching
        // tuples from either side:
        // LeftInner: matching tuples from the left
        // LeftOuter: non-matching tuples from the left
        // RightInner: matching tuples from the right
        // RightOuter: non-matching tuples from the right
        if (joinType == LhxJoinRelType.RIGHTANTI) {
            streamDef.setLeftInner(false);
        } else {
            streamDef.setLeftInner(true);
        }
        if ((joinType == LhxJoinRelType.LEFT)
            || (joinType == LhxJoinRelType.FULL)) {
            streamDef.setLeftOuter(true);
        } else {
            streamDef.setLeftOuter(false);
        }

        if ((joinType == LhxJoinRelType.LEFTSEMI)
            || (joinType == LhxJoinRelType.RIGHTANTI)) {
            streamDef.setRightInner(false);
        } else {
            streamDef.setRightInner(true);
        }

        if ((joinType == LhxJoinRelType.RIGHT)
            || (joinType == LhxJoinRelType.FULL)
            || (joinType == LhxJoinRelType.RIGHTANTI)) {
            streamDef.setRightOuter(true);
        } else {
            streamDef.setRightOuter(false);
        }

        if (isSetop) {
            // set operation
            streamDef.setSetopAll(false);
            streamDef.setSetopDistinct(true);
        } else {
            // regular join
            streamDef.setSetopAll(false);
            streamDef.setSetopDistinct(false);
        }

        streamDef.setLeftKeyProj(
            FennelRelUtil.createTupleProjection(
                repos,
                leftKeys));

        streamDef.setRightKeyProj(
            FennelRelUtil.createTupleProjection(
                repos,
                rightKeys));

        return streamDef;
    }
    
    public LhxJoinRelType getJoinType()
    {
        return joinType;
    }
    
    public RelNode getLeft()
    {
        return left;
    }
    
    public RelNode getRight()
    {
        return right;
    }
    
    public List<Integer> getLeftKeys()
    {
        return leftKeys;
    }
    
    public List<Integer> getRightKeys()
    {
        return rightKeys;
    }
}

// End LhxJoinRel.java

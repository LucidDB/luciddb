/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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
package net.sf.farrago.fennel.rel;

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
     * Whether the join key at a given position matches null values
     */
    List<Integer> filterNulls;

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
     * Creates a new LhxJoinRel object.
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
        List<Integer> filterNulls,
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

        if (filterNulls != null) {
            this.filterNulls = filterNulls;
        } else {
            // if filterNulls is null, it is implied that nulls keys
            // are not matched, i.e. filterNulls is true for every key position
            //
            // note that an empty list means nulls are not filtered for any key
            // position, which means null values are considered matching for
            // all join key positions.
            this.filterNulls = new ArrayList<Integer>();
            for (int i = 0; i < leftKeys.size(); i++) {
                this.filterNulls.add(i);
            }
        }

        if (joinType == LhxJoinRelType.LEFTSEMI) {
            // intersect is implemented using left semi join
            this.rowType = left.getRowType();
        } else if (
            (joinType == LhxJoinRelType.RIGHTANTI)
            || (joinType == LhxJoinRelType.RIGHTSEMI))
        {
            // except is implemented using right anti or right semi join
            this.rowType = right.getRowType();
        } else {
            // regular join
            this.rowType =
                JoinRel.deriveJoinRowType(
                    left.getRowType(),
                    right.getRowType(),
                    joinType.getLogicalJoinType(),
                    cluster.getTypeFactory(),
                    fieldNameList,
                    Collections.<RelDataTypeField>emptyList());
        }
        this.numBuildRows = numBuildRows;
        this.cndBuildKey = cndBuildKey;
    }

    //~ Methods ----------------------------------------------------------------

    // implement Cloneable
    public LhxJoinRel clone()
    {
        LhxJoinRel clone =
            new LhxJoinRel(
                getCluster(),
                left.clone(),
                right.clone(),
                joinType,
                isSetop,
                leftKeys,
                rightKeys,
                filterNulls,
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
        return planner.makeCost(
            rowCount * joinSelectivity,
            0,
            rowCount * getRowType().getFieldList().size()
            * joinSelectivity);
    }

    // implement RelNode
    public double getRows()
    {
        // NOTE jvs 7-Jan-2008: In LucidDB, LoptMetadataProvider takes care of
        // overriding this with a better implementation, but in vanilla
        // Farrago, this default implementation is used.

        double product =
            RelMetadataQuery.getRowCount(getLeft())
            * RelMetadataQuery.getRowCount(getRight())
            * 0.1;

        return product;
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        if (!isSetop) {
            if (filterNulls.size() == leftKeys.size()) {
                pw.explain(
                    this,
                    new String[] {
                        "left", "right", "leftKeys", "rightKeys", "joinType"
                    },
                    new Object[] { leftKeys, rightKeys, joinType });
            } else {
                // only print out filterNulls if not all key positions are
                // included
                pw.explain(
                    this,
                    new String[] {
                        "left", "right", "leftKeys", "rightKeys", "filterNulls",
                        "joinType"
                    },
                    new Object[] {
                        leftKeys, rightKeys, filterNulls, joinType });
            }
        } else {
            pw.explain(
                this,
                new String[] {
                    "left", "right", "leftKeys", "rightKeys", "joinType",
                    "setop"
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
            implementor.visitFennelChild((FennelRel) left, 0);
        implementor.addDataFlowFromProducerToConsumer(
            leftInput,
            streamDef);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right, 1);
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
        if ((joinType == LhxJoinRelType.RIGHTANTI)
            || (joinType == LhxJoinRelType.RIGHTSEMI))
        {
            streamDef.setLeftInner(false);
        } else {
            streamDef.setLeftInner(true);
        }

        if ((joinType == LhxJoinRelType.LEFT)
            || (joinType == LhxJoinRelType.FULL))
        {
            streamDef.setLeftOuter(true);
        } else {
            streamDef.setLeftOuter(false);
        }

        if ((joinType == LhxJoinRelType.LEFTSEMI)
            || (joinType == LhxJoinRelType.RIGHTANTI))
        {
            streamDef.setRightInner(false);
        } else {
            streamDef.setRightInner(true);
        }

        if ((joinType == LhxJoinRelType.RIGHT)
            || (joinType == LhxJoinRelType.FULL)
            || (joinType == LhxJoinRelType.RIGHTANTI))
        {
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

        streamDef.setFilterNullProj(
            FennelRelUtil.createTupleProjection(
                repos,
                filterNulls));

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

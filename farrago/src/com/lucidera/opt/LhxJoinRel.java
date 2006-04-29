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

import net.sf.farrago.catalog.FarragoRepos;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.util.Util;
import org.eigenbase.stat.*;

import java.util.List;

/**
 * LhxJoinRel implements the hash join.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LhxJoinRel extends FennelDoubleRel
{
    /**
     * Join type. Currently only inner join is supported.
     */
    private final JoinRelType joinType;
    /**
     * Join key columns from the left.
     */
    List<Integer> leftKeys;
    /**
     * Join key columns from the right.
     */
    List<Integer> rightKeys;
    
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelCartesianProductRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param left left input
     * @param right right input
     * @param fieldNameList If not null, the row type will have these field
     *                      names
     */
    public LhxJoinRel(
        RelOptCluster cluster,
        RelNode left,
        RelNode right,
        JoinRelType joinType,
        List<Integer> leftKeys,
        List<Integer> rightKeys,
        List<String> fieldNameList)
    {
        super(cluster, left, right);
        assert joinType != null;
        this.joinType = joinType;
        this.leftKeys = leftKeys;
        this.rightKeys = rightKeys;
        this.rowType = JoinRel.deriveJoinRowType(
            left.getRowType(), right.getRowType(), joinType,
            cluster.getTypeFactory(), fieldNameList);
    }

    //~ Methods ---------------------------------------------------------------

    // implement Cloneable
    public Object clone()
    {
        LhxJoinRel clone =
            new LhxJoinRel(
                getCluster(),
                RelOptUtil.clone(left),
                RelOptUtil.clone(right),
                joinType,
                leftKeys,
                rightKeys,
                RelOptUtil.getFieldNameList(rowType));
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    public RelOptCost computeSelfCost(RelOptPlanner planner)
    {
        // TODO:  account for buffering I/O and CPU
        double rowCount = RelMetadataQuery.getRowCount(this);
        double joinSelectivity = 0.1;
        return planner.makeCost(rowCount * joinSelectivity, 0,
            rowCount * getRowType().getFieldList().size() * joinSelectivity);
    }

    // implement RelNode
    public double getRows()
    {
        return RelMetadataQuery.getRowCount(left)
            * RelMetadataQuery.getRowCount(right);
    }

    // override RelNode
    public void explain(RelOptPlanWriter pw)
    {
        pw.explain(
            this,
            new String [] { "left", "right", "leftKeys", "rightKeys", "joinType"},
            new Object [] {leftKeys, rightKeys, joinType});
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
        FemLhxJoinStreamDef streamDef =
            repos.newFemLhxJoinStreamDef();

        FemExecutionStreamDef leftInput =
            implementor.visitFennelChild((FennelRel) left);
        implementor.addDataFlowFromProducerToConsumer(
            leftInput, streamDef);
        FemExecutionStreamDef rightInput =
            implementor.visitFennelChild((FennelRel) right);
        implementor.addDataFlowFromProducerToConsumer(
            rightInput, streamDef);

        streamDef.setOutputDesc(
            FennelRelUtil.createTupleDescriptorFromRowType(
                repos,
                getCluster().getTypeFactory(),
                this.getRowType()));

        Double numRightRows = RelMetadataQuery.getRowCount(right);
        if (numRightRows == null) {
            numRightRows = 10000.0;
        }
        streamDef.setNumBuildRows(numRightRows.intValue());
        
        RelStatSource statSource = RelMetadataQuery.getStatistics(right);

        // Derive cardinality of RHS join keys.
        Double cndKeys = 1.0;
        double correlationFactor = 0.7;
        RelStatColumnStatistics colStat;
        Double cndCol;
        
        for (int i = 0; i < rightKeys.size(); i ++) {
            cndCol = null;
            if (statSource != null) {
                colStat = statSource.getColumnStatistics(rightKeys.get(i), null);
                if (colStat != null) {
                    cndCol = colStat.getCardinality();
                }
            }

            if (cndCol == null) {
                // default to 100 distinct values for a column
                cndCol = 100.0;
            }
            
            cndKeys *= cndCol;
            
            // for each additional key, apply the correlationFactor.
            if (i > 0) {
                cndKeys *= correlationFactor;
            }
            
            // cndKeys can be at most equal to number of rows from the build
            // side.
            if (cndKeys > numRightRows) {
                cndKeys = numRightRows;
                break;
            }
        }

        if (joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL) {
            streamDef.setLeftOuter(true);
        }

        if (joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL) {
            streamDef.setRightOuter(true);
        }

        streamDef.setCndBuildKeys(cndKeys.intValue());

        streamDef.setLeftKeyProj(
            FennelRelUtil.createTupleProjection(
                repos, leftKeys));
        
        streamDef.setRightKeyProj(
            FennelRelUtil.createTupleProjection(
                repos, rightKeys));

        return streamDef;
    }
}

// End LhxJoinRel.java

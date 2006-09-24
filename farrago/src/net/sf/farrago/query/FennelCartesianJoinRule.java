/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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
import org.eigenbase.relopt.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.rex.*;

/**
 * FennelCartesianJoinRule is a rule for converting an INNER JoinRel with no
 * join condition into a FennelCartesianProductRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelCartesianJoinRule
    extends RelOptRule
{

    //~ Constructors -----------------------------------------------------------

    public FennelCartesianJoinRule()
    {
        super(new RelOptRuleOperand(
                JoinRel.class,
                null));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        RelNode leftRel = joinRel.getLeft();
        RelNode rightRel = joinRel.getRight();

        /*
         * Joins that can use CartesianProduct will have only TRUE condition 
         * in JoinRel. Any other join conditions have to be extracted out 
         * already. This implies that only ON TRUE condition is suported for
         * LeftOuterJoins.
         */
        boolean joinTypeFeasible = 
            (joinRel.getJoinType() == JoinRelType.INNER) ||
            (joinRel.getJoinType() == JoinRelType.LEFT);
        
        /*
         * CartesianProduct relies on a post filter to do the join filtering.
         * If the join condition is not extracted to a post filter(and is still
         * in JoinRel), CartesianProduct can not be used.
         */
        boolean joinConditionFeasible =
            joinRel.getCondition().equals(
                joinRel.getCluster().getRexBuilder().makeLiteral(true));
                
        if (!joinTypeFeasible || !joinConditionFeasible) {
            return;
        }

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        // see if it makes sense to buffer the existing RHS; if not, try
        // the LHS, swapping the join operands if it does make sense to buffer
        // the LHS
        boolean swapped = false;
        FennelBufferRel bufRel =
            bufferRight(leftRel, rightRel, joinRel.getTraits());
        if (bufRel != null) {
            rightRel = bufRel;
        } else {
            bufRel = bufferRight(rightRel, leftRel, joinRel.getTraits());
            if (bufRel != null) {
                swapped = true;
                leftRel = rightRel;
                rightRel = bufRel;
            }         
        }

        RelNode fennelLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                leftRel);
        if (fennelLeft == null) {
            return;
        }

        RelNode fennelRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                rightRel);
        if (fennelRight == null) {
            return;
        }      
        
        FennelCartesianProductRel productRel =
            new FennelCartesianProductRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                joinRel.getJoinType(),
                RelOptUtil.getFieldNameList(joinRel.getRowType()));
        
        RelNode newRel;
        if (swapped) {
            // if the join inputs were swapped, create a CalcRel on top of
            // the new cartesian join that reflects the original join
            // projection
            final RexNode [] exps = 
                RelOptUtil.createSwappedJoinExprs(
                    productRel,
                    joinRel,
                    true);
            final RexProgram program =
                RexProgram.create(
                    productRel.getRowType(),
                    exps,
                    null,
                    joinRel.getRowType(),
                    productRel.getCluster().getRexBuilder());
            newRel =
                new CalcRel(
                    productRel.getCluster(),
                    RelOptUtil.clone(joinRel.getTraits()),
                    productRel,
                    joinRel.getRowType(),
                    program,
                    RelCollation.emptyList);
        } else {
            newRel = productRel;
        }
        call.transformTo(newRel);
    }
    
    /**
     * Returns a FennelBufferRel in the case where it makes sense to buffer
     * the RHS into the cartesian product join.  This is done by comparing
     * the cost between the buffered and non-buffered cases.
     * 
     * @param left left hand input into the cartesian join
     * @param right right hand input into the cartesian join
     * @param traits traits of the original join
     * 
     * @return created FennelBufferRel if it makes sense to buffer the RHS
     */
    private FennelBufferRel bufferRight(
        RelNode left,
        RelNode right,
        RelTraitSet traits)
    {
        RelNode fennelInput = 
            mergeTraitsAndConvert(
                traits,
                FennelRel.FENNEL_EXEC_CONVENTION,
                right);
        FennelBufferRel bufRel =
            new FennelBufferRel(right.getCluster(), fennelInput, false, true);

        // if we don't have a rowcount for the LHS, then just go ahead and
        // buffer
        Double nRowsLeft = RelMetadataQuery.getRowCount(left);
        if (nRowsLeft == null) {
            return bufRel;
        }
        
        // Cost without buffering is:
        // getCumulativeCost(LHS) +
        //     getRowCount(LHS) * getCumulativeCost(RHS)
        //
        // Cost with buffering is:
        // getCumulativeCost(LHS) + getCumulativeCost(RHS) +
        //     getRowCount(LHS) * getNonCumulativeCost(buffering) * 3;
        //
        // The times 3 represents the overhead of caching.  The "3"
        // is arbitrary at this point.
        //
        // To decide if buffering makes sense, take the difference between the
        // two costs described above.
        RelOptCost rightCost =
            RelMetadataQuery.getCumulativeCost(right);
        RelOptCost noBufferPlanCost = rightCost.multiplyBy(nRowsLeft);
        
        RelOptCost bufferCost = RelMetadataQuery.getNonCumulativeCost(bufRel);
        bufferCost = bufferCost.multiplyBy(3);
        RelOptCost bufferPlanCost = bufferCost.multiplyBy(nRowsLeft);
        bufferPlanCost = bufferPlanCost.plus(rightCost);
        
        if (bufferPlanCost.isLt(noBufferPlanCost)) {
            return bufRel;
        } else {
            return null;
        }      
    }
}

// End FennelCartesianJoinRule.java

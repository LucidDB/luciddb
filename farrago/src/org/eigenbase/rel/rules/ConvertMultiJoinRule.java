/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;

/**
 * Rule to flatten a tree of {@link JoinRel}s into a single
 * {@link MultiJoinRel} with N inputs.  An input is not flattened if the
 * input is a null generating input in an outer join, i.e., either input in
 * a full outer join, the right hand side of a left outer join, or the left
 * hand side of a right outer join.
 * 
 * <p>Join conditions are also pulled up from the inputs into the topmost
 * {@link MultiJoinRel}, unless the input corresponds to a null generating
 * input in an outer join,
 * 
 * <p>Outer join information is also stored in the {@link MultiJoinRel}.  A
 * boolean flag indicates if the join is a full outer join, and in the case of
 * left and right outer joins, the join type and outer join conditions are
 * stored in arrays in the {@link MultiJoinRel}.  This outer join information
 * is associated with the null generating input in the outer join.  So, in the
 * case of a a left outer join between A and B, the information is associated
 * with B, not A.
 * 
 * <p>Here are examples of the {@link MultiJoinRel}s constructed after this
 * rule has been applied on following join trees.
 * 
 * <pre>
 * A JOIN B -> MJ(A, B)
 * A JOIN B JOIN C -> MJ(A, B, C)
 * A LEFTOUTER B -> MJ(A, B), left outer join on input#1
 * A RIGHTOUTER B -> MJ(A, B), right outer join on input#0
 * A FULLOUTER B -> MJ[full](A, B)
 * A LEFTOUTER (B JOIN C) -> MJ(A, MJ(B, C))), left outer join on input#1 in
 * the outermost MultiJoinRel
 * (A JOIN B) LEFTOUTER C -> MJ(A, B, C), left outer join on input#2
 * A LEFTOUTER (B FULLOUTER C) -> MJ(A, MJ[full](B, C)), left outer join on
 *      input#1 in the outermost MultiJoinRel
 * (A LEFTOUTER B) FULLOUTER (C RIGHTOUTER D) ->
 *      MJ[full](MJ(A, B), MJ(C, D)), left outer join on input #1 in the first
 *      inner MultiJoinRel and right outer join on input#0 in the second inner
 *      MultiJoinRel
 * </pre>
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class ConvertMultiJoinRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public ConvertMultiJoinRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(RelNode.class, null),
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        // if JoinRel has already been converted to a MultiJoinRel,
        // no need to convert it again
        JoinRel origJoinRel = (JoinRel) call.rels[0];
        if (origJoinRel.isMultiJoinDone()) {
            return;
        }

        RelNode left = call.rels[1];
        RelNode right = call.rels[2];

        // combine the children MultiJoinRel inputs into an array of inputs
        // for the new MultiJoinRel
        RelNode [] newInputs = combineInputs(origJoinRel, left, right);

        // combine the outer join information from the left and right
        // inputs, and include the outer join information from the current
        // join, if it's a left/right outer join
        RexNode [] newOuterJoinConds = new RexNode[newInputs.length];
        JoinRelType [] joinTypes = new JoinRelType[newInputs.length];
        combineOuterJoins(
            origJoinRel,
            newInputs,
            left,
            right,
            newOuterJoinConds,
            joinTypes);
        
        // pull up the join filters from the children MultiJoinRels and
        // combine them with the join filter associated with this JoinRel to
        // form the join filter for the new MultiJoinRel
        RexNode newJoinFilter = combineJoinFilters(origJoinRel, left, right);

        RelNode multiJoin =
            new MultiJoinRel(
                origJoinRel.getCluster(),
                newInputs,
                newJoinFilter,
                origJoinRel.getRowType(),
                (origJoinRel.getJoinType() == JoinRelType.FULL),
                newOuterJoinConds,
                joinTypes);            

        call.transformTo(multiJoin);
    }

    /**
     * Combines the inputs into a JoinRel into an array of inputs.
     *
     * @param join original join
     * @param left left input into join
     * @param right right input into join
     *
     * @return combined left and right inputs in an array
     */
    private RelNode [] combineInputs(
        JoinRel joinRel,
        RelNode left,
        RelNode right)
    {
        // leave the null generating sides of an outer join intact; don't
        // pull up those children inputs into the array we're constructing
        int nInputs;
        int nInputsOnLeft;
        MultiJoinRel leftMultiJoin = null;
        JoinRelType joinType = joinRel.getJoinType();
        boolean combineLeft = canCombine(left, joinType.generatesNullsOnLeft());
        if (combineLeft) {
            leftMultiJoin = (MultiJoinRel) left;
            nInputs = left.getInputs().length;
            nInputsOnLeft = nInputs;
        } else {
            nInputs = 1;
            nInputsOnLeft = 1;
        }
        MultiJoinRel rightMultiJoin = null;
        boolean combineRight =
            canCombine(right, joinType.generatesNullsOnRight());
        if (combineRight) {
            rightMultiJoin = (MultiJoinRel) right;
            nInputs += right.getInputs().length;
        } else {
            nInputs += 1;
        }
        
        RelNode [] newInputs = new RelNode[nInputs];
        int i = 0;
        if (combineLeft) {
            for (; i < left.getInputs().length; i++) {
                newInputs[i] = leftMultiJoin.getInput(i);
            }
        } else {
            newInputs[0] = left;
            i = 1;
        }
        if (combineRight) {
            for (; i < nInputs; i++) {
                newInputs[i] = rightMultiJoin.getInput(i - nInputsOnLeft);
            }
        } else {
            newInputs[i] = right;
        }

        return newInputs;
    }

    /**
     * Combines the outer join conditions and join types from the left and right
     * join inputs.  If the join itself is either a left or right outer join,
     * then the join condition corresponding to the join is also set in the
     * position corresponding to the null-generating input into the join.
     * The join type is also set.
     *
     * @param joinRel join rel
     * @param combinedInputs the combined inputs to the join
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     * @param combinedConds the array containing the combined join conditions
     * @param joinTypes the array containing the combined join types
     *
     * @return combined join filters AND'd together
     */
    private RexNode [] combineOuterJoins(
        JoinRel joinRel,
        RelNode [] combinedInputs,
        RelNode left,
        RelNode right,
        RexNode [] combinedConds,
        JoinRelType [] joinTypes)
    {
        JoinRelType joinType = joinRel.getJoinType();
        int nCombinedInputs = combinedInputs.length;
        boolean leftCombined =
            canCombine(left, joinType.generatesNullsOnLeft());
        boolean rightCombined =
            canCombine(right, joinType.generatesNullsOnRight());
        if (joinType == JoinRelType.LEFT) {
            if (leftCombined) {
                copyOuterJoinInfo(
                    (MultiJoinRel) left,
                    combinedConds,
                    joinTypes,
                    0,
                    0,
                    null,
                    null);
            } else {
                joinTypes[0] = JoinRelType.INNER;
            }
            combinedConds[nCombinedInputs - 1] = joinRel.getCondition();
            joinTypes[nCombinedInputs - 1] = joinType;
            
        } else if (joinType == JoinRelType.RIGHT) {
            if (rightCombined) {
                copyOuterJoinInfo(
                    (MultiJoinRel) right,
                    combinedConds,
                    joinTypes,
                    1,
                    left.getRowType().getFieldCount(),
                    right.getRowType().getFields(),
                    joinRel.getRowType().getFields());
            } else {
                joinTypes[nCombinedInputs - 1] = JoinRelType.INNER;
            }
            combinedConds[0] = joinRel.getCondition();
            joinTypes[0] = joinType;
            
        } else {
            int nInputsLeft;
            if (leftCombined) {
                nInputsLeft = left.getInputs().length;
                copyOuterJoinInfo(
                    (MultiJoinRel) left,
                    combinedConds,
                    joinTypes,
                    0,
                    0,
                    null,
                    null);
            } else {
                nInputsLeft = 1;
                joinTypes[0] = JoinRelType.INNER;
            }
            if (rightCombined) {
                copyOuterJoinInfo(
                    (MultiJoinRel) right,
                    combinedConds,
                    joinTypes,
                    nInputsLeft,
                    left.getRowType().getFieldCount(),
                    right.getRowType().getFields(),
                    joinRel.getRowType().getFields());                  
            } else {
                joinTypes[nInputsLeft] = JoinRelType.INNER;
            }
        }
        
        return combinedConds;
    }
        
    /**
     * Copies outer join data from a source MultiJoinRel to a new set of arrays.
     * Also adjusts the conditions to reflect the new position of an input
     * if that input ends up being shifted to the right.
     * 
     * @param multiJoinRel the source MultiJoinRel
     * @param destConds the array where the join conditions will be copied
     * @param destJoinTypes the array where the join types will be copied
     * @param destPos starting position in the array where the copying starts
     * @param adjustmentAmount if > 0, the amount the RexInputRefs in the
     * join conditions need to be adjusted by
     * @param srcFields the source fields that the original join conditions
     * are referencing
     * @param destFields the destination fields that the new join conditions
     * will be referencing
     */
    private void copyOuterJoinInfo(
        MultiJoinRel multiJoinRel,
        RexNode [] destConds,
        JoinRelType [] destJoinTypes,
        int destPos,
        int adjustmentAmount,
        RelDataTypeField [] srcFields,
        RelDataTypeField [] destFields)
    {
        RexNode [] srcConds = multiJoinRel.getOuterJoinConditions();
        JoinRelType [] srcJoinTypes = multiJoinRel.getJoinTypes(); 
        RexBuilder rexBuilder = multiJoinRel.getCluster().getRexBuilder();
        
        int len = srcConds.length;
        System.arraycopy(srcJoinTypes, 0, destJoinTypes, destPos, len);
        
        if (adjustmentAmount == 0) {
            System.arraycopy(srcConds, 0, destConds, 0, len);
        } else {
            int nFields = srcFields.length;
            int [] adjustments = new int[nFields];
            for (int idx = 0; idx < nFields; idx++) {
                adjustments[idx] = adjustmentAmount;
            }
            for (int i = 0; i < len; i++) {
                if (srcConds[i] != null) {
                    destConds[i + destPos] =
                        srcConds[i].accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                srcFields,
                                destFields,
                                adjustments));
                }
            }
        }      
    }

    /**
     * Combines the join filters from the left and right inputs (if they are
     * MultiJoinRels) with the join filter in the joinrel into a single AND'd
     * join filter, unless the inputs correspond to null generating inputs in
     * an outer join
     *
     * @param joinRel join rel
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     *
     * @return combined join filters AND'd together
     */
    private RexNode combineJoinFilters(
        JoinRel joinRel,
        RelNode left,
        RelNode right)
    {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        JoinRelType joinType = joinRel.getJoinType();

        // first need to adjust the RexInputs of the right child, since
        // those need to shift over to the right
        RexNode rightFilter = null;
        if (canCombine(right, joinType.generatesNullsOnRight())) {
            MultiJoinRel multiJoin = (MultiJoinRel) right;
            rightFilter = multiJoin.getJoinFilter();
            if (rightFilter != null) {
                int nFieldsOnLeft = left.getRowType().getFields().length;
                int nFieldsOnRight = multiJoin.getRowType().getFields().length;
                int [] adjustments = new int[nFieldsOnRight];
                for (int i = 0; i < nFieldsOnRight; i++) {
                    adjustments[i] = nFieldsOnLeft;
                }
                rightFilter =
                    rightFilter.accept(
                        new RelOptUtil.RexInputConverter(
                            rexBuilder,
                            multiJoin.getRowType().getFields(),
                            joinRel.getRowType().getFields(),
                            adjustments));
            }
        }

        // AND the join condition if this isn't a left or right outer join;
        // in those cases, the outer join condition is already tracked
        // separately
        RexNode newFilter = null;
        if (joinType != JoinRelType.LEFT && joinType != JoinRelType.RIGHT) {
            newFilter = joinRel.getCondition();
        }
        if (canCombine(left, joinType.generatesNullsOnLeft())) {
            RexNode leftFilter = ((MultiJoinRel) left).getJoinFilter();
            newFilter =
                RelOptUtil.andJoinFilters(
                    rexBuilder,
                    newFilter,
                    leftFilter);
        }
        newFilter =
            RelOptUtil.andJoinFilters(
                rexBuilder,
                newFilter,
                rightFilter);

        return newFilter;
    }
    
    /**
     * @param input input into a join
     * @param nullGenerating true if the input is null generating
     * 
     * @return true if the input can be combined into a parent MultiJoinRel
     */
    private boolean canCombine(RelNode input, boolean nullGenerating)
    {
        return
            (input instanceof MultiJoinRel &&
            !((MultiJoinRel) input).isFullOuterJoin() &&
            !nullGenerating);
    }
}

// End ConvertMultiJoinRule.java

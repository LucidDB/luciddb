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
import org.eigenbase.rex.*;

/**
 * Rule to flatten a tree of JoinRels into a single MultiJoinRel with N inputs.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class ConvertMultiJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------
    
    public ConvertMultiJoinRule(RelOptRuleOperand rule, String id)
    {
        // Currently matches against the following patterns:
        //  - JoinRel(X, Y)
        // where X and Y can be either:
        //  - MultiJoinRel
        //  - LcsRowScanRel
        //  - FilterRel(LcsRowScanRel)
        super(rule);
        description = "ConvertMultiJoinRule: " + id;
    }

    public void onMatch(RelOptRuleCall call)
    {
        // if JoinRel has already been converted to a MultiJoinRel,
        // no need to convert it again     
        JoinRel origJoinRel = (JoinRel) call.rels[0];
        if (origJoinRel.isMultiJoinDone()) {
            return;
        }
       
        // the rel corresponding to the right input varies depending on the
        // pattern matched
        RelNode left = call.rels[1];
        RelNode right;
        if (call.rels[1] instanceof FilterRel) {
            right = call.rels[3];
        } else {
            right = call.rels[2];
        }
        
        // combine the children MultiJoinRel inputs into an array of inputs
        // for the new MultiJoinRel
        RelNode newInputs[] = combineInputs(left, right);
        
        // pull up the join filters from the children MultiJoinRels and
        // combine them with the join filter associated with this JoinRel to
        // form the join filter for the new MultiJoinRel
        RexNode newJoinFilter = combineJoinFilters(origJoinRel, left, right);

        RelNode multiJoin = new MultiJoinRel(
            origJoinRel.getCluster(),
            newInputs, newJoinFilter, origJoinRel.getRowType());
        
        call.transformTo(multiJoin);        
    }

    /**
     * Combines the inputs into a JoinRel into an array of inputs
     * 
     * @param left left input into join
     * @param right right input into join
     * @return combined left and right inputs in an array
     */
    private RelNode[] combineInputs(RelNode left, RelNode right)
    {
        int nInputs;
        int nInputsOnLeft;
        MultiJoinRel leftMultiJoin = null;
        MultiJoinRel rightMultiJoin = null;
        if (left instanceof MultiJoinRel) {
            leftMultiJoin = (MultiJoinRel) left;
            nInputs = left.getInputs().length;
            nInputsOnLeft = nInputs;
        } else {
            nInputs = 1;
            nInputsOnLeft = 1;
        }
        if (right instanceof MultiJoinRel) {
            rightMultiJoin = (MultiJoinRel) right;
            nInputs += right.getInputs().length;
        } else {
            nInputs += 1;
        }
        RelNode newInputs[] = new RelNode[nInputs];
        int i;
        if (left instanceof MultiJoinRel) {
            for (i = 0; i < left.getInputs().length; i++) {
                newInputs[i] = leftMultiJoin.getInput(i);
            }
        } else {
            newInputs[0] = left;
            i = 1;
        }
        if (right instanceof MultiJoinRel) {
            for ( ; i < nInputs; i++) {
                newInputs[i] = rightMultiJoin.getInput(i - nInputsOnLeft);
            }
        } else {
            newInputs[i] = right;
        }
        
        return newInputs;
    }
    
    /**
     * Combines the join filters from the left and right inputs (if they are
     * MultiJoinRels) with the join filter in the joinrel into a single
     * AND'd join filter
     * 
     * @param joinRel join rel
     * @param left left child of the joinrel
     * @param right right child of the joinrel
     * @return combined join filters AND'd together
     */
    private RexNode combineJoinFilters(
        JoinRel joinRel, RelNode left, RelNode right)
    {
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();
        
        // first need to adjust the RexInputs of the right child, since
        // those need to shift over to the right
        RexNode rightFilter = null;
        if (right instanceof MultiJoinRel) {
            MultiJoinRel multiJoin = (MultiJoinRel) right;
            rightFilter = multiJoin.getJoinFilter();
            if (rightFilter != null) {
                int nFieldsOnLeft = left.getRowType().getFields().length;
                int nFieldsOnRight = multiJoin.getRowType().getFields().length;
                int adjustments[] = new int[nFieldsOnRight];
                for (int i = 0; i < nFieldsOnRight; i++) {
                    adjustments[i] = nFieldsOnLeft;
                }
                rightFilter = RelOptUtil.convertRexInputRefs(
                    rexBuilder,
                    rightFilter,
                    multiJoin.getRowType().getFields(),
                    adjustments);
            }
        }
        
        // AND everything together
        RexNode newFilter = joinRel.getCondition();
        if (left instanceof MultiJoinRel) {
            RexNode leftFilter = ((MultiJoinRel) left).getJoinFilter();
            newFilter = RelOptUtil.andJoinFilters(
                rexBuilder, newFilter, leftFilter);
        }
        newFilter = RelOptUtil.andJoinFilters(
            rexBuilder, newFilter, rightFilter);
        
        return newFilter;
    }
}

// End ConvertMultiJoinRule.java

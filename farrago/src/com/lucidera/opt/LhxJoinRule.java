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

import java.util.ArrayList;
import java.util.List;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

import net.sf.farrago.query.*;

/**
 * LhxJoinRule implements the planner rule for converting a JoinRel
 * with join condition into a LhxJoinRel (hash join).
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public LhxJoinRule()
    {
        super(new RelOptRuleOperand(
                  JoinRel.class,
                  null));
    }

    //~ Methods ---------------------------------------------------------------

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
        RexNode joinCondition = joinRel.getCondition();
        RexNode nonJoinCondition;

        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        
        nonJoinCondition = RelOptUtil.splitJoinCondition(
            leftRel, rightRel, joinCondition, leftKeys, rightKeys, true);
        
        if (leftKeys.size() == 0 ||
            leftKeys.size() != rightKeys.size()) {
            return;
        }
        
        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        RelNode fennelLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                leftRel);
        
        if (fennelLeft == null) {
            return;
        }

        RelNode fennelRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                rightRel);
        
        if (fennelRight == null) {
            return;
        }

        LhxJoinRel rel =
            new LhxJoinRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                joinRel.getJoinType(),
                leftKeys,
                rightKeys,
                RelOptUtil.getFieldNameList(joinRel.getRowType()));
        
        transformCall(call, rel, nonJoinCondition);
    }


    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel =
                new FilterRel(rel.getCluster(), rel, extraFilter);
        }
        call.transformTo(rel);
    }

}
// End LhxJoinRule.java

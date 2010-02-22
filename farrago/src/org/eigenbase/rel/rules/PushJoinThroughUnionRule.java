/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2010-2010 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2010-2010 LucidEra, Inc.
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
import org.eigenbase.rel.metadata.*;

import java.util.*;

/**
 * PushJoinThroughUnionRule implements the rule for pushing a
 * {@link JoinRel} past a non-distinct {@link UnionRel}.
 *
 * @author John Sichi
 * @version $Id$
 */
public class PushJoinThroughUnionRule extends RelOptRule
{
    public static final PushJoinThroughUnionRule instanceUnionOnLeft =
        new PushJoinThroughUnionRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(UnionRel.class, RelOptRule.ANY),
                new RelOptRuleOperand(RelNode.class, ANY)),
            "union on left");

    public static final PushJoinThroughUnionRule instanceUnionOnRight =
        new PushJoinThroughUnionRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(UnionRel.class, RelOptRule.ANY)),
            "union on right");

    public PushJoinThroughUnionRule(RelOptRuleOperand operand, String id)
    {
        super(
            operand,
            "PushJoinThroughUnionRule: " + id);
    }

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        UnionRel unionRel;
        RelNode otherInput;
        boolean unionOnLeft;
        if (call.rels[1] instanceof UnionRel) {
            unionRel = (UnionRel) call.rels[1];
            otherInput = call.rels[2];
            unionOnLeft = true;
        } else {
            otherInput = call.rels[1];
            unionRel = (UnionRel) call.rels[2];
            unionOnLeft = false;
        }
        if (unionRel.isDistinct()) {
            return;
        }
        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }
        // The UNION ALL cannot be on the null generating side
        // of an outer join (otherwise we might generate incorrect
        // rows for the other side for join keys which lack a match
        // in one or both branches of the union)
        if (unionOnLeft) {
            if (joinRel.getJoinType().generatesNullsOnLeft()) {
                return;
            }
        } else {
            if (joinRel.getJoinType().generatesNullsOnRight()) {
                return;
            }
        }
        RelNode [] unionInputs = unionRel.getInputs();
        int nUnionInputs = unionInputs.length;
        RelNode [] newUnionInputs = new RelNode[nUnionInputs];
        RelOptCluster cluster = unionRel.getCluster();
        for (int i = 0; i < nUnionInputs; i++) {
            RelNode joinLeft, joinRight;
            if (unionOnLeft) {
                joinLeft = unionInputs[i];
                joinRight = otherInput;
            } else {
                joinLeft = otherInput;
                joinRight = unionInputs[i];
            }
            newUnionInputs[i] =
                new JoinRel(
                    cluster,
                    joinLeft,
                    joinRight,
                    joinRel.getCondition(),
                    joinRel.getJoinType(),
                    Collections.<String>emptySet());
        }
        UnionRel newUnionRel = new UnionRel(cluster, newUnionInputs, true);
        call.transformTo(newUnionRel);
    }
}

// End PushJoinThroughUnionRule.java

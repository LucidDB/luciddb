/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Portions Copyright (C) 2006-2009 John V. Sichi
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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * Rule to add a semijoin into a joinrel. Transformation is as follows:
 *
 * <p>JoinRel(X, Y) -> JoinRel(SemiJoinRel(X, Y), Y)
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class AddRedundantSemiJoinRule
    extends RelOptRule
{
    public static final AddRedundantSemiJoinRule instance =
        new AddRedundantSemiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an AddRedundantSemiJoinRule.
     */
    private AddRedundantSemiJoinRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel origJoinRel = (JoinRel) call.rels[0];
        if (origJoinRel.isSemiJoinDone()) {
            return;
        }

        // can't process outer joins using semijoins
        if (origJoinRel.getJoinType() != JoinRelType.INNER) {
            return;
        }

        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();
        RelOptUtil.splitJoinCondition(
            origJoinRel.getLeft(),
            origJoinRel.getRight(),
            origJoinRel.getCondition(),
            leftKeys,
            rightKeys);
        if (leftKeys.size() == 0) {
            return;
        }

        RelNode semiJoin =
            new SemiJoinRel(
                origJoinRel.getCluster(),
                origJoinRel.getLeft(),
                origJoinRel.getRight(),
                origJoinRel.getCondition(),
                leftKeys,
                rightKeys);

        RelNode newJoinRel =
            new JoinRel(
                origJoinRel.getCluster(),
                semiJoin,
                origJoinRel.getRight(),
                origJoinRel.getCondition(),
                JoinRelType.INNER,
                Collections.<String>emptySet(),
                true,
                origJoinRel.getSystemFieldList());

        call.transformTo(newJoinRel);
    }
}

// End AddRedundantSemiJoinRule.java

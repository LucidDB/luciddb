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

package org.eigenbase.rel;

import java.util.*;

import org.eigenbase.relopt.*;

/**
 * Rule to add a semijoin into a joinrel.  Transformation is as follows:
 *
 * <p>JoinRel(X, Y) -> JoinRel(SemiJoinRel(X, Y), Y)
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class AddRedundantSemiJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------
    
    public AddRedundantSemiJoinRule()
    {
        super(new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null),
                    new RelOptRuleOperand(RelNode.class, null)             
                }));
    }

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel origJoinRel = (JoinRel) call.rels[0];
        
        // terminate if we've already created a semijoin
        if (call.rels[1] instanceof SemiJoinRel ||
            call.rels[2] instanceof SemiJoinRel)
        {
            return;
        }
        
        // determine if we have a valid join condition
        // TODO - currently do not support multi-key joins
        int [] joinFieldOrdinals = new int[2];
        if (!RelOptUtil.analyzeSimpleEquiJoin(origJoinRel, joinFieldOrdinals)) {
            return;
        }

        RelNode semiJoin = new SemiJoinRel(
            origJoinRel.getCluster(),
            call.rels[1],
            call.rels[2],
            origJoinRel.getCondition(),
            joinFieldOrdinals[1]);
       
        RelNode newJoinRel = new JoinRel(
            origJoinRel.getCluster(),
            semiJoin,
            call.rels[2],
            origJoinRel.getCondition(),
            JoinRelType.INNER,
            Collections.EMPTY_SET);

        call.transformTo(newJoinRel);
    }

}

// End AddRedundantSemiJoinRule.java

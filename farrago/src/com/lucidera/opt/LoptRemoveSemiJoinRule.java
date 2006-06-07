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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rel.rules.*;

/**
 * LoptRemoveSemiJoinRule implements the rule that removes semijoins from
 * a join tree if it turns out it's not possible to convert a SemiJoinRel to
 * an indexed scan on a join factor.  Namely, if the join factor does not
 * reduce to a single table that can be scanned using an index.  This rule
 * should only be applied after attempts have been made to convert SemiJoinRels.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LoptRemoveSemiJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public LoptRemoveSemiJoinRule()
    {
        super(new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(SemiJoinRel.class, null)
                }));
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
    	JoinRel origJoinRel = (JoinRel) call.rels[0];
        JoinRel joinRel = new JoinRel(
                origJoinRel.getCluster(),
                call.rels[1].getInput(0),
                origJoinRel.getRight(),
                origJoinRel.getCondition(),
                origJoinRel.getJoinType(),
                origJoinRel.getVariablesStopped(),
                origJoinRel.isSemiJoinDone(),
                origJoinRel.isMultiJoinDone());

        call.transformTo(joinRel);
    }

}
// End LoptRemoveSemiJoinRule.java

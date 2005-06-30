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

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.convert.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * FennelCartesianJoinRule is a rule for converting an INNER JoinRel with no
 * join condition into a FennelCartesianProductRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelCartesianJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public FennelCartesianJoinRule()
    {
        super(new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null),
                    new RelOptRuleOperand(RelNode.class, null)
                }));
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

        RelNode leftRel = call.rels[1];
        RelNode rightRel = call.rels[2];

        if ((joinRel.getJoinType() != JoinRel.JoinType.INNER) &&
            (joinRel.getJoinType() != JoinRel.JoinType.LEFT)) {
            return;
        }

        if (!joinRel.getCondition().equals(
                joinRel.getCluster().getRexBuilder().makeLiteral(true))) {
            // TODO: implement condition with a filter, or better: do that in a
            // separate logical transformation
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

        FennelCartesianProductRel productRel =
            new FennelCartesianProductRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                joinRel.getJoinType());
        call.transformTo(productRel);

        // TODO jvs 15-Feb-2005: In most cases, buffering the
        // right-hand input is good for performance; it allows us to only
        // compute the right-hand side once, and since we store only
        // what we need, we may save I/O.  However, there are counterexamples;
        // if the right-hand side is a table scan with no filtering or
        // projection, there's no point buffering it.  So we should produce
        // plan variants with and without buffering and use cost to decide.
    }
}


// End FennelCartesianJoinRule.java

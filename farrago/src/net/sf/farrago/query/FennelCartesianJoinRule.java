/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

package net.sf.farrago.query;

import net.sf.farrago.catalog.*;
import net.sf.farrago.type.*;
import net.sf.farrago.util.*;

import net.sf.saffron.core.*;
import net.sf.saffron.opt.*;
import net.sf.saffron.rel.*;
import net.sf.saffron.rel.convert.*;
import net.sf.saffron.util.*;

/**
 * FennelCartesianJoinRule is a rule for converting an INNER JoinRel with no
 * join condition into a FennelCartesianProductRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FennelCartesianJoinRule extends VolcanoRule
{
    public FennelCartesianJoinRule()
    {
        super(
            new RuleOperand(
                JoinRel.class,
                new RuleOperand [] {
                    new RuleOperand(
                        SaffronRel.class,
                        null),
                    new RuleOperand(
                        SaffronRel.class,
                        null)
                }));
    }

    // implement VolcanoRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_CALLING_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        SaffronRel leftRel = call.rels[1];
        SaffronRel rightRel = call.rels[2];

        if (joinRel.getJoinType() != JoinRel.JoinType.INNER) {
            return;
        }

        if (!joinRel.getCondition().equals(
                joinRel.getCluster().rexBuilder.makeLiteral(true)))
        {
            // TODO: implement condition with a filter, or better: do that in a
            // separate logical transformation
            return;
        }

        // REVIEW:  need to worry about variables stopped?

        SaffronRel fennelLeft =
            convert(planner,leftRel,FennelRel.FENNEL_CALLING_CONVENTION);
        if (fennelLeft == null) {
            return;
        }
        
        SaffronRel fennelRight =
            convert(planner,rightRel,FennelRel.FENNEL_CALLING_CONVENTION);
        if (fennelRight == null) {
            return;
        }
        
        FennelCartesianProductRel productRel = new FennelCartesianProductRel(
            joinRel.getCluster(),
            fennelLeft,
            fennelRight);
        call.transformTo(productRel);

        // TODO:  supply a variant which uses a buffer, and let the optimizer
        // decide which to use based on cost
    }
}

// End FennelCartesianJoinRule.java

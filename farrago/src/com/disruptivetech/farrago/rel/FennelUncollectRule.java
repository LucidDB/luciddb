/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later Eigenbase-approved version.
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
package com.disruptivetech.farrago.rel;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.UncollectRel;
import net.sf.farrago.query.FennelPullRel;

/**
 * FennelUncollectRule is a rule to implement a call with the
 * {@link org.eigenbase.sql.fun.SqlStdOperatorTable#unnestOperator}
 *
 * @author Wael Chatila 
 * @since Dec 12, 2004
 * @version $Id$
 */
public class FennelUncollectRule extends RelOptRule
{
    public FennelUncollectRule() {
        super(new RelOptRuleOperand(
                UncollectRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
        UncollectRel  uncollectRel = (UncollectRel) call.rels[0];
        RelNode relInput = call.rels[1];
        RelNode fennelInput =
            convert(relInput, FennelPullRel.FENNEL_PULL_CONVENTION);
        if (fennelInput == null) {
            return;
        }

        FennelPullUncollectRel fennelUncollectRel =
            new FennelPullUncollectRel(uncollectRel.getCluster(), fennelInput);
        call.transformTo(fennelUncollectRel);
    }
}

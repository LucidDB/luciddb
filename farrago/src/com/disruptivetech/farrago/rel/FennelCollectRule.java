/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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
package com.disruptivetech.farrago.rel;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.CollectRel;
import net.sf.farrago.query.FennelPullRel;

/**
 * FennelCollectRule is a rule to implement a call made with the
 * {@link org.eigenbase.sql.fun.SqlMultisetOperator}
 *
 * @author Wael Chatila 
 * @since Dec 11, 2004
 * @version $Id$
 */
public class FennelCollectRule extends RelOptRule {

    public FennelCollectRule() {
        super(new RelOptRuleOperand(
                CollectRel.class,
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
        CollectRel collectRel = (CollectRel) call.rels[0];
        RelNode relInput = call.rels[1];
        RelNode fennelInput =
            mergeTraitsAndConvert(
                collectRel.getTraits(), FennelPullRel.FENNEL_PULL_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        FennelPullCollectRel fennelCollectRel =
            new FennelPullCollectRel(
                collectRel.getCluster(),
                fennelInput,
                collectRel.getFieldName());
        call.transformTo(fennelCollectRel);
    }
}

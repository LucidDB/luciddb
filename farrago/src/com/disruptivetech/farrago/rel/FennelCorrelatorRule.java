/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 Disruptive Tech
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

import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.rel.CollectRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.CorrelatorRel;
import net.sf.farrago.query.FennelPullRel;

import java.util.ArrayList;

/**
 * FennelCorrelateRule is a rule to implement two correlated join streams
 *
 * @author Wael Chatila 
 * @since Feb 1, 2005
 * @version $Id$
 */
public class FennelCorrelatorRule extends RelOptRule {

    public FennelCorrelatorRule() {
        super(new RelOptRuleOperand(
                CorrelatorRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null),
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
        CorrelatorRel correlatorRel = (CorrelatorRel) call.rels[0];
        RelNode relLeftInput = call.rels[1];
        RelNode fennelLeftInput =
            mergeTraitsAndConvert(
                correlatorRel.getTraits(),
                FennelPullRel.FENNEL_PULL_CONVENTION,
                relLeftInput);
        if (fennelLeftInput == null) {
            return;
        }

        RelNode relRightInput = call.rels[2];
        RelNode fennelRightInput =
            mergeTraitsAndConvert(
                correlatorRel.getTraits(),
                FennelPullRel.FENNEL_PULL_CONVENTION,
                relRightInput);
        if (fennelRightInput == null) {
            return;
        }

        FennelPullCorrelatorRel fennelPullCorrelatorRel =
            new FennelPullCorrelatorRel(
                correlatorRel.getCluster(),
                fennelLeftInput,
                fennelRightInput,
                (ArrayList) correlatorRel.getCorrelations().clone());
        call.transformTo(fennelPullCorrelatorRel);
    }
}

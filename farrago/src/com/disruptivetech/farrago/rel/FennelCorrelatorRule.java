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
        CorrelatorRel  correlatorRel = (CorrelatorRel) call.rels[0];
        RelNode relLeftInput = call.rels[1];
        RelNode fennelLeftInput =
            convert(relLeftInput, FennelPullRel.FENNEL_PULL_CONVENTION);
        if (fennelLeftInput == null) {
            return;
        }

        RelNode relRightInput = call.rels[2];
        RelNode fennelRightInput =
            convert(relRightInput, FennelPullRel.FENNEL_PULL_CONVENTION);
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

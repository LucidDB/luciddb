package net.sf.farrago.query;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.rel.OneRowRel;

/**
 * FennelOneRowRule generates an XO who simple outputs a single one row with the
 * value 0
 *
 * @author Wael Chatila 
 * @since Feb 4, 2005
 * @version $Id$
 */
public class FennelOneRowRule extends RelOptRule {

    private FarragoPreparingStmt stmt;

    public FennelOneRowRule(FarragoPreparingStmt stmt) {
        super(new RelOptRuleOperand(OneRowRel.class, null));
        this.stmt = stmt;
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call) {
        OneRowRel  oneRowRel = (OneRowRel) call.rels[0];
        if (oneRowRel.getClass() != OneRowRel.class) {
            return;
        }

        FennelPullOneRowRel fennelOneRowRel =
            new FennelPullOneRowRel(oneRowRel.getCluster(), stmt);
        call.transformTo(fennelOneRowRel);
    }
}


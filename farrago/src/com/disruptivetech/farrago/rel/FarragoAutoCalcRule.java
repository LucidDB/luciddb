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

import com.disruptivetech.farrago.calc.RexToCalcTranslator;

import net.sf.farrago.query.*;

import org.eigenbase.oj.rel.JavaRelImplementor;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexDynamicParam;
import org.eigenbase.rex.RexFieldAccess;
import org.eigenbase.rex.RexMultisetUtil;


/**
 * FarragoAutoCalcRule is a rule for implementing {@link CalcRel} via
 * a combination of the Fennel Calculator ({@link FennelCalcRel}) and
 * the Java Calculator ({@link org.eigenbase.oj.rel.IterCalcRel}).
 *
 * <p>This rule does not attempt to transform the matching
 * {@link org.eigenbase.relopt.RelOptRuleCall} if the entire CalcRel
 * can be implemented entirely via one calculator or the other.  A
 * future optimization might be to use a costing mechanism to
 * determine where expressions that can be implemented by both
 * calculators should be executed.
 *
 * <p><b>Strategy:</b> Each CalcRel can be considered a forest (e.g. a
 * group of trees).  The forest is comprised of the RexNode trees
 * contained in the project and conditional expressions in the
 * CalcRel.  The rule's basic strategy is to stratify the forest into
 * levels, such that each level can be implemented entirely by a
 * single calc.  Having divided the forest into levels, the rule
 * creates new CalcRel instances which each contain all of the
 * RexNodes associated with a given level and then connects RexNodes
 * across the levels by adding RexInputRef instances as needed.  The
 * planner will then evaluate each CalcRel and should find that either
 * IterCalcRel or FennelCalcRel is able to implement each of the
 * CalcRels this rule creates.  The planner will then automatically
 * place the necessary plumbing between IterCalcRel and FennelCalcRel
 * instances to convert between the two types of calculator.  This
 * strategy depends the rules that generate IterCalcRel and
 * FennelCalcRel not performing their transformations if a portion of
 * an expression cannot be implemented in the corresponding
 * calculator.  It also depends on accurate implementability
 * information regarding RexCalls.
 *
 * <p><b>Potential improvements:</b> Currently, the rule does not
 * exploit redundancy between trees in the forest.  For example,
 * consider a table T with columns C1 and C2 and calculator functions
 * F1, F2 and J where F1 and F2 are Fennel-only and J is Java-only.
 * The query <pre>
 *     select F1(C1), F2(C1), J(C2) from T</pre>
 * begins life as <pre>
 *     CalcRel Project: "F1($0), F2($0), J($1)"</pre>
 * After applying FarragoAutoCalcRule we get <pre>
 *     CalcRel Project: "F1($0), F2($1), $2"
 *     CalcRel Project: "$0, $0, J($1)"</pre>
 * Notice that although the calls to F1 and F2 refer to the same base
 * column, the rule treats them separately.  A better result
 * would be <pre>
 *     CalcRel Project: "F1($0), F2($0), $1"
 *     CalcRel Project: "$0, J($1)"</pre>
 *
 * <p>Another improvement relates to handling conditional expressions.
 * The current implementation of the FarragoAutoCalc rule only treats
 * the conditional expression specially in the top-most
 * post-transformation CalcRel.  For example, consider a table T with
 * columns C1 and C2 and calculator functions F and J where F is
 * Fennel-only and J is Java-only.  The query <pre>
 *     select F(C1) from T where J(C2)</pre>
 * begins life as <pre>
 *     CalcRel Project: "F($0)"    Conditional: "J($1)"</pre>
 * After applying FarragoAutoCalcRule we get <pre>
 *     CalcRel Project: "F($0)"    Conditional: "$1"
 *     CalcRel Project: "$0 J($1)" Conditional:     </pre>
 * Notice that even though the conditional expression could be
 * evaluated and used for filtering in the lower CalcRel, it's not.
 * This means that all rows are sent to the Fennel calculator.  A
 * better result would be <pre>
 *     CalcRel Project: "F($0)"    Contidional:
 *     CalcRel Project: "$0"       Conditional: "J($1)"</pre>
 * In this case, rows that don't match the conditional expression
 * would not reach the Fennel calculator.
 */
public class FarragoAutoCalcRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    private static final CalcRelSplitter.RelType REL_TYPE_JAVA =
        new CalcRelSplitter.RelType("REL_TYPE_JAVA");
    
    private static final CalcRelSplitter.RelType REL_TYPE_FENNEL =
        new CalcRelSplitter.RelType("REL_TYPE_FENNEL");

    /**
     * The singleton instance.
     */
    public static final FarragoAutoCalcRule instance =
        new FarragoAutoCalcRule();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoAutoCalcRule object.
     */
    private FarragoAutoCalcRule()
    {
        super(new RelOptRuleOperand(
                CalcRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Called when this rule matches a rel.  First uses
     * {@link RexToCalcTranslator} to determine if the rel can be
     * implemented in the Fennel calc.  If so, this method returns
     * without performing any transformation.  Next uses
     * {@link JavaRelImplementor} to perform the same test for the
     * Java calc.  Also returns with no transformation if the Java
     * calc can implement the rel.  Finally, transforms the given
     * CalcRel into a stack of CalcRels that can each individually be
     * implemented in the Fennel or Java calcs.
     */
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        RelNode relInput = call.rels[1];

        for (int i = 0; i < calc.projectExprs.length; i++) {
            if (RexMultisetUtil.containsMultiset(
                calc.projectExprs[i], true)) {
                return; // Let FarragoMultisetSplitter work on it first.
            }
        }
        if (calc.conditionExpr != null) {
            if (RexMultisetUtil.containsMultiset(
                calc.conditionExpr, true)) {
                return; // Let FarragoMultisetSplitter work on it first.
            }
        }

        // Test if we can translate the CalcRel to a fennel calc program
        RelNode fennelInput =
            mergeTraitsAndConvert(
                calc.getTraits(), FennelPullRel.FENNEL_PULL_CONVENTION, relInput);

        final RexToCalcTranslator translator =
            new RexToCalcTranslator(calc.getCluster().rexBuilder,
                calc.projectExprs, calc.conditionExpr);

        if (fennelInput != null) {
            boolean canTranslate = true;
            for (int i = 0; i < calc.projectExprs.length; i++) {
                if (!translator.canTranslate(calc.projectExprs[i], true)) {
                    canTranslate = false;
                    break;
                }
            }

            if ((calc.conditionExpr != null)
                    && !translator.canTranslate(calc.conditionExpr, true)) {
                canTranslate = false;
            }

            if (canTranslate) {
                // yes: do nothing, let FennelCalcRule handle this CalcRel
                return;
            }
        }

        // Test if we can translate the CalcRel to a java calc program
        final RelNode convertedChild =
            mergeTraitsAndConvert(
                calc.getTraits(), CallingConvention.ITERATOR, calc.child);

        final JavaRelImplementor relImplementor =
            calc.getCluster().getPlanner().getJavaRelImplementor(calc);

        if (convertedChild != null) {
            if (relImplementor.canTranslate(convertedChild,
                        calc.conditionExpr, calc.projectExprs)) {
                // yes: do nothing, let IterCalcRule handle this CalcRel
                return;
            }
        }

        CalcRelSplitter transform =
            new AutoCalcRelSplitter(calc, relImplementor, translator);

        RelNode resultCalcRelTree = transform.execute();

        call.transformTo(resultCalcRelTree);
    }

    //~ Inner Classes ---------------------------------------------------------

    private static class AutoCalcRelSplitter
        extends CalcRelSplitter
    {
        private final JavaRelImplementor relImplementor;

        private final RexToCalcTranslator translator;

        private AutoCalcRelSplitter(CalcRel calc, JavaRelImplementor relImplementor, RexToCalcTranslator translator)
        {
            super(calc, REL_TYPE_JAVA, REL_TYPE_FENNEL);

            this.relImplementor = relImplementor;
            this.translator = translator;
        }

        protected boolean canImplementAs(
            RexCall call, CalcRelSplitter.RelType relType)
        {
            if (relType == REL_TYPE_FENNEL) {
                return translator.canTranslate(call, false);
            } else if (relType == REL_TYPE_JAVA) {
                return relImplementor.canTranslate(calc, call, false);
            } else {
                assert(false): "Unknown rel type: " + relType;
                return false;
            }
        }

        protected boolean canImplementAs(RexDynamicParam param,
            RelType relType)
        {
            // Dynamic param rex nodes are Java-only
            return relType == REL_TYPE_JAVA;
        }

        protected boolean canImplementAs(RexFieldAccess field,
            RelType relType)
        {
            // Field access rex nodes are Java-only
            return relType == REL_TYPE_JAVA;
        }


    }
}

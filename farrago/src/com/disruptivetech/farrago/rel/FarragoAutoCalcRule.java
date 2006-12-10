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

import com.disruptivetech.farrago.calc.*;

import net.sf.farrago.query.*;

import org.eigenbase.oj.rel.*;
import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * FarragoAutoCalcRule is a rule for implementing {@link CalcRel} via a
 * combination of the Fennel Calculator ({@link FennelCalcRel}) and the Java
 * Calculator ({@link org.eigenbase.oj.rel.IterCalcRel}).
 *
 * <p>This rule does not attempt to transform the matching {@link
 * org.eigenbase.relopt.RelOptRuleCall} if the entire CalcRel can be implemented
 * entirely via one calculator or the other. A future optimization might be to
 * use a costing mechanism to determine where expressions that can be
 * implemented by both calculators should be executed.
 *
 * <p><b>Strategy:</b> Each CalcRel can be considered a forest (e.g. a group of
 * trees). The forest is comprised of the RexNode trees contained in the project
 * and conditional expressions in the CalcRel. The rule's basic strategy is to
 * stratify the forest into levels, such that each level can be implemented
 * entirely by a single calc. Having divided the forest into levels, the rule
 * creates new CalcRel instances which each contain all of the RexNodes
 * associated with a given level and then connects RexNodes across the levels by
 * adding RexInputRef instances as needed. The planner will then evaluate each
 * CalcRel and should find that either IterCalcRel or FennelCalcRel is able to
 * implement each of the CalcRels this rule creates. The planner will then
 * automatically place the necessary plumbing between IterCalcRel and
 * FennelCalcRel instances to convert between the two types of calculator. This
 * strategy depends the rules that generate IterCalcRel and FennelCalcRel not
 * performing their transformations if a portion of an expression cannot be
 * implemented in the corresponding calculator. It also depends on accurate
 * implementability information regarding RexCalls.
 *
 * <p><b>Potential improvements:</b> Currently, the rule does not exploit
 * redundancy between trees in the forest. For example, consider a table T with
 * columns C1 and C2 and calculator functions F1, F2 and J where F1 and F2 are
 * Fennel-only and J is Java-only. The query
 *
 * <pre>
 *     select F1(C1), F2(C1), J(C2) from T</pre>
 *
 * begins life as
 *
 * <pre>
 *     CalcRel Project: "F1($0), F2($0), J($1)"</pre>
 *
 * After applying FarragoAutoCalcRule we get
 *
 * <pre>
 *     CalcRel Project: "F1($0), F2($1), $2"
 *     CalcRel Project: "$0, $0, J($1)"</pre>
 *
 * Notice that although the calls to F1 and F2 refer to the same base column,
 * the rule treats them separately. A better result would be
 *
 * <pre>
 *     CalcRel Project: "F1($0), F2($0), $1"
 *     CalcRel Project: "$0, J($1)"</pre>
 *
 * <p>Another improvement relates to handling conditional expressions. The
 * current implementation of the FarragoAutoCalc rule only treats the
 * conditional expression specially in the top-most post-transformation CalcRel.
 * For example, consider a table T with columns C1 and C2 and calculator
 * functions F and J where F is Fennel-only and J is Java-only. The query
 *
 * <pre>
 *     select F(C1) from T where J(C2)</pre>
 *
 * begins life as
 *
 * <pre>
 *     CalcRel Project: "F($0)"    Conditional: "J($1)"</pre>
 *
 * After applying FarragoAutoCalcRule we get
 *
 * <pre>
 *     CalcRel Project: "F($0)"    Conditional: "$1"
 *     CalcRel Project: "$0 J($1)" Conditional:     </pre>
 *
 * Notice that even though the conditional expression could be evaluated and
 * used for filtering in the lower CalcRel, it's not. This means that all rows
 * are sent to the Fennel calculator. A better result would be
 *
 * <pre>
 *     CalcRel Project: "F($0)"    Contidional:
 *     CalcRel Project: "$0"       Conditional: "J($1)"</pre>
 *
 * In this case, rows that don't match the conditional expression would not
 * reach the Fennel calculator.
 */
public class FarragoAutoCalcRule
    extends RelOptRule
{

    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FarragoAutoCalcRule instance =
        new FarragoAutoCalcRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoAutoCalcRule object.
     */
    private FarragoAutoCalcRule()
    {
        super(new RelOptRuleOperand(
                CalcRel.class,
                null));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Called when this rule matches a rel. First uses {@link
     * RexToCalcTranslator} to determine if the rel can be implemented in the
     * Fennel calc. If so, this method returns without performing any
     * transformation. Next uses {@link JavaRelImplementor} to perform the same
     * test for the Java calc. Also returns with no transformation if the Java
     * calc can implement the rel. Finally, transforms the given CalcRel into a
     * stack of CalcRels that can each individually be implemented in the Fennel
     * or Java calcs.
     */
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        RelNode relInput = calc.getChild();

        // If there's a multiset expression, let FarragoMultisetSplitter work
        // on it first.
        if (RexMultisetUtil.containsMultiset(calc.getProgram())) {
            return;
        }

        // If there's a windowed agg expression, split that out first.
        if (RexOver.containsOver(calc.getProgram())) {
            return;
        }

        // Test if we can translate the CalcRel to a fennel calc program
        RelNode fennelInput =
            mergeTraitsAndConvert(
                calc.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);

        final RexToCalcTranslator translator =
            new RexToCalcTranslator(
                calc.getCluster().getRexBuilder(),
                calc);

        // If can translate, do nothing, and let FennelCalcRule handle this
        // CalcRel.
        if ((fennelInput != null)
            && translator.canTranslate(calc.getProgram())) {
            return;
        }

        // Test if we can translate the CalcRel to a java calc program
        final RelNode convertedChild =
            mergeTraitsAndConvert(
                calc.getTraits(),
                CallingConvention.ITERATOR,
                calc.getChild());

        final JavaRelImplementor relImplementor =
            calc.getCluster().getPlanner().getJavaRelImplementor(calc);

        if (convertedChild != null) {
            if (relImplementor.canTranslate(
                    convertedChild,
                    calc.getProgram())) {
                // yes: do nothing, let IterCalcRule handle this CalcRel
                return;
            }
        }

        RelNode root = calc.getCluster().getPlanner().getRoot();
        boolean promoteIteratorConvention = 
            root.getConvention() == CallingConvention.ITERATOR;
        
        AutoCalcRelSplitter transform =
            new AutoCalcRelSplitter(
                calc, relImplementor, translator, promoteIteratorConvention);

        if (transform.canImplement(calc)) {
            RelNode resultCalcRelTree = transform.execute();
            call.transformTo(resultCalcRelTree);
        }
    }

    /**
     * Returns whether an expression can be implemented in Fennel convention.
     */
    public boolean canImplementInFennel(CalcRel calc)
    {
        final RexToCalcTranslator translator =
            new RexToCalcTranslator(
                calc.getCluster().getRexBuilder(),
                calc);
        return new FennelRelType(translator).canImplement(calc.getProgram());
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class FennelRelType extends CalcRelSplitter.RelType
    {
        private final RexToCalcTranslator translator;
        
        private FennelRelType(RexToCalcTranslator translator)
        {
            super("REL_TYPE_FENNEL");
            
            this.translator = translator;
        }

        protected boolean canImplement(RexFieldAccess field)
        {
            // Field access rex nodes are Java-only
            return false;
        }

        protected boolean canImplement(RexDynamicParam param)
        {
            // Dynamic param rex nodes are Java-only
            return false;
        }

        protected boolean canImplement(RexLiteral literal)
        {
            return true;
        }

        protected boolean canImplement(RexCall call)
        {
            return translator.canTranslate(call, false);
        }
    }
    
    private static class JavaRelType extends CalcRelSplitter.RelType
    {
        private final CalcRel calc;
        private final JavaRelImplementor relImplementor;
        
        private JavaRelType(CalcRel calc, JavaRelImplementor relImplementor)
        {
            super("REL_TYPE_JAVA");
            
            this.calc = calc;
            this.relImplementor = relImplementor;
        }

        protected boolean canImplement(RexFieldAccess field)
        {
            return true;
        }

        protected boolean canImplement(RexDynamicParam param)
        {
            return true;
        }

        protected boolean canImplement(RexLiteral literal)
        {
            return true;
        }

        protected boolean canImplement(RexCall call)
        {
            return relImplementor.canTranslate(calc, call, false);
        }
    }
    
    private static class AutoCalcRelSplitter
        extends CalcRelSplitter
    {
        private AutoCalcRelSplitter(
            CalcRel calc,
            JavaRelImplementor relImplementor,
            RexToCalcTranslator translator,
            boolean promoteIteratorConvention)
        {
            // The last RelType in the array ends up at the top of the plan.
            // So, if we've been asked to promote iterator convention, we
            // try to make sure that the CalcRel that's implementable by
            // iterator convention ends up on top.
            super(
                calc,
                (promoteIteratorConvention
                    ? new RelType[] {
                        new FennelRelType(translator),
                        new JavaRelType(calc, relImplementor)
                      }
                    : new RelType[] {
                        new JavaRelType(calc, relImplementor),
                        new FennelRelType(translator)
                      })
                );
        }

        protected boolean canImplement(CalcRel rel)
        {
            if (RexUtil.requiresDecimalExpansion(
                    rel.getProgram(),
                    true)) {
                return false;
            }
            return true;
        }
    }
}

// End FarragoAutoCalcRule.java

/*
// $Id$
// Saffron preprocessor and data engine
// (C) Copyright 2004-2004 Disruptive Tech
// You must accept the terms in LICENSE.html to use this software.
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
package net.sf.saffron.rel;

import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rex.*;

/**
 * Planner rule which merges a {@link ProjectRel} and a {@link CalcRel}. The
 * resulting {@link CalcRel} has the same project list as the original
 * {@link ProjectRel}, but expressed in terms of the original {@link CalcRel}'s
 * inputs.
 *
 * @see MergeFilterOntoCalcRule
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public class MergeProjectOntoCalcRule extends VolcanoRule {
    public static MergeProjectOntoCalcRule instance =
            new MergeProjectOntoCalcRule();

    private MergeProjectOntoCalcRule() {
        super(new RuleOperand(ProjectRel.class, new RuleOperand[] {
            new RuleOperand(CalcRel.class, null),
        }));
    }

    public void onMatch(VolcanoRuleCall call) {
        final ProjectRel project = (ProjectRel) call.rels[0];
        final CalcRel calc = (CalcRel) call.rels[1];

        // Expand all references to columns in the project exprs. For example,
        //
        // SELECT x + 1 AS p, x + y AS q FROM (
        //   SELECT a + b AS x, c AS y
        //   FROM t
        //   WHERE c < 6)
        //
        // becomes
        //
        // SELECT (a + b) + 1 AS p, (a + b) + c AS q
        // FROM t
        // WHERE c < 6
        final RexNode[] projectExprs = RexUtil.clone(project.exps);
        final RexShuttle shuttle = new RexShuttle() {
            public RexNode visit(RexInputRef input) {
                return calc._projectExprs[input.index];
            }
        };
        for (int i = 0; i < projectExprs.length; i++) {
            projectExprs[i] = shuttle.visit(projectExprs[i]);
        }
        final CalcRel newCalc = new CalcRel(calc.cluster, calc.child,
                project.getRowType(), projectExprs, calc._conditionExpr);
        call.transformTo(newCalc);
    }
}

// End MergeProjectOntoCalcRule.java
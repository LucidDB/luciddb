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

import net.sf.saffron.core.SaffronType;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexUtil;

/**
 * Rule to convert a {@link ProjectRel} to a {@link CalcRel}
 *
 * <p>The rule does not fire if the child is a {@link ProjectRel},
 * {@link FilterRel} or {@link CalcRel}. If it did, then the same
 * {@link CalcRel} would be formed via several transformation paths, which
 * is a waste of effort.</p>
 *
 * @see FilterToCalcRule
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public class ProjectToCalcRule extends VolcanoRule {
    public static ProjectToCalcRule instance =
            new ProjectToCalcRule();

    private ProjectToCalcRule() {
        super(new RuleOperand(ProjectRel.class, new RuleOperand[] {
            new RuleOperand(SaffronRel.class, null),
        }));
    }

    public void onMatch(VolcanoRuleCall call) {
        final ProjectRel project = (ProjectRel) call.rels[0];
        final SaffronRel child = call.rels[1];
        if (child instanceof FilterRel ||
                child instanceof ProjectRel ||
                child instanceof CalcRel) {
            // don't create a CalcRel if the input is, or is potentially, a
            // CalcRel
            return;
        }
        final SaffronType rowType = project.getRowType();
        final RexNode[] projectExprs = RexUtil.clone(project.exps);
        final CalcRel calc = new CalcRel(project.cluster, child, rowType,
                projectExprs, null);
        call.transformTo(calc);
    }
}

// End ProjectToCalcRule.java
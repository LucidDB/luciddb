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
import net.sf.saffron.rel.FilterRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rex.RexNode;
import net.sf.saffron.rex.RexBuilder;
import net.sf.saffron.core.SaffronType;
import net.sf.saffron.core.SaffronField;

/**
 * Planner rule which converts a {@link FilterRel} to a {@link CalcRel}.
 *
 * <p>The rule does <em>NOT</em> fire if the child is a {@link FilterRel} or a
 * {@link ProjectRel} (we assume they they will be converted using
 * {@link FilterToCalcRule} or {@link ProjectToCalcRule}) or a {@link CalcRel}.
 * This {@link FilterRel} will eventually be converted by
 * {@link MergeFilterOntoCalcRule}.
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public class FilterToCalcRule extends VolcanoRule {
    public static FilterToCalcRule instance =
            new FilterToCalcRule();

    private FilterToCalcRule() {
        super(new RuleOperand(FilterRel.class, new RuleOperand[] {
            new RuleOperand(SaffronRel.class, null)
        }));
    }

    public void onMatch(VolcanoRuleCall call) {
        final FilterRel filter = (FilterRel) call.rels[0];
        final SaffronRel rel = call.rels[1];
        if (rel instanceof FilterRel ||
                rel instanceof ProjectRel ||
                rel instanceof CalcRel) {
            // don't create a CalcRel if the input is, or is potentially, a
            // CalcRel
            return;
        }
        final SaffronType rowType = rel.getRowType();
        final SaffronField [] fields = rowType.getFields();
        RexNode[] exprs = new RexNode[fields.length];
        final RexBuilder rexBuilder = filter.cluster.rexBuilder;
        for (int i = 0; i < exprs.length; i++) {
            exprs[i] = rexBuilder.makeInputRef(fields[i].getType(), i);
        }
        final CalcRel calc = new CalcRel(filter.cluster, rel, rowType, exprs,
                filter.condition);
        call.transformTo(calc);
    }
}

// End FilterToCalcRule.java
/*
// Farrago is a relational database management system.
// Copyright (C) 2003-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

package com.disruptivetech.farrago.rel;

import com.disruptivetech.farrago.calc.RexToCalcTranslator;

import net.sf.farrago.query.*;

import net.sf.saffron.opt.CallingConvention;
import net.sf.saffron.opt.RuleOperand;
import net.sf.saffron.opt.VolcanoRule;
import net.sf.saffron.opt.VolcanoRuleCall;
import net.sf.saffron.rel.CalcRel;
import net.sf.saffron.rel.SaffronRel;
import net.sf.saffron.rel.SaffronRel;

// REVIEW jvs 11-May-2004:  shouldn't FennelCalcRule extend ConverterRule
// (just like IterCalcRule)?

/**
 * FennelCalcRule is a rule for implementing {@link CalcRel} via a Fennel
 * Calculator ({@link FennelCalcRel}).
 *
 * @author jhyde
 * @version $Id$
 */
public class FennelCalcRule extends VolcanoRule {
    /**
     * The singleton instance.
     */
    public static final FennelCalcRule instance = new FennelCalcRule();
    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelCalcRule object.
     */
    private FennelCalcRule() {
        super(
            new RuleOperand(
                CalcRel.class,
                new RuleOperand[] {
                    new RuleOperand(SaffronRel.class, null)}));
    }

    //~ Methods ---------------------------------------------------------------

    // implement VolcanoRule
    public CallingConvention getOutConvention() {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement VolcanoRule
    public void onMatch(VolcanoRuleCall call) {
        CalcRel calc = (CalcRel) call.rels[0];
        SaffronRel relInput = call.rels[1];
        SaffronRel fennelInput = convert(relInput,
                FennelPullRel.FENNEL_PULL_CONVENTION);
        if (fennelInput == null) {
            return;
        }

        final RexToCalcTranslator translator = new RexToCalcTranslator(
                calc.getCluster().rexBuilder,
                calc._projectExprs,
                calc._conditionExpr);
        for(int i = 0; i < calc._projectExprs.length; i++) {
            if (!translator.canTranslate(calc._projectExprs[i], true)) {
                return;
            }
        }
        if (calc._conditionExpr != null && 
            !translator.canTranslate(calc._conditionExpr, true)) {
            return;
        }

        FennelPullCalcRel fennelCalcRel =
                new FennelPullCalcRel(
                        calc.getCluster(),
                        fennelInput,
                        calc.getRowType(),
                        calc._projectExprs,
                        calc._conditionExpr);
        call.transformTo(fennelCalcRel);
    }
}

// End FennelCalcRule.java

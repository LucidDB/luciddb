/*
// $Id$
// Farrago is a relational database management system.
// Copyright (C) 2002-2004 Disruptive Tech
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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
import net.sf.farrago.fem.fennel.FemExecutionStreamDef;

import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.CallingConvention;
import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;


/**
 * FennelWindowRule is a rule for implementing a {@link CalcRel} which
 * contains windowed aggregates via a {@link FennelWindowRel}.
 *
 * @author jhyde
 * @version $Id$
 */
public class FennelWindowRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * The singleton instance.
     */
    public static final FennelWindowRule instance = new FennelWindowRule();

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FennelCalcRule object.
     */
    private FennelWindowRule()
    {
        super(new RelOptRuleOperand(
                CalcRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) call.rels[0];
        RelNode relInput = call.rels[1];
        RelNode fennelInput =
            convert(relInput, FennelPullRel.FENNEL_PULL_CONVENTION);
        if (fennelInput == null) {
            return;
        }

        final RexToCalcTranslator translator =
            new RexToCalcTranslator(calc.getCluster().rexBuilder,
                calc.projectExprs, calc.conditionExpr);
        for (int i = 0; i < calc.projectExprs.length; i++) {
            if (!translator.canTranslate(calc.projectExprs[i], true)) {
                return;
            }
        }
        if ((calc.conditionExpr != null)
                && !translator.canTranslate(calc.conditionExpr, true)) {
            return;
        }

        FennelWindowRel fennelCalcRel =
            new FennelWindowRel(calc.getCluster(),
                    fennelInput,
                    calc.getRowType(),
                    calc.projectExprs,
                    calc.conditionExpr);
        call.transformTo(fennelCalcRel);
    }
}


// End FennelCalcRule.java

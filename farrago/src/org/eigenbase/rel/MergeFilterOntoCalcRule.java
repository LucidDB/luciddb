/*
// $Id$
// Package org.eigenbase is a class library of database components.
// Copyright (C) 2002-2004 Disruptive Tech
// Copyright (C) 2003-2004 John V. Sichi
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

package org.eigenbase.rel;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.rex.*;


/**
 * Planner rule which merges a {@link FilterRel} and a {@link CalcRel}. The
 * result is a {@link CalcRel} whose filter condition is the logical AND of
 * the two.
 *
 * @see MergeFilterOntoCalcRule
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 **/
public class MergeFilterOntoCalcRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    public static MergeFilterOntoCalcRule instance =
        new MergeFilterOntoCalcRule();

    //~ Constructors ----------------------------------------------------------

    private MergeFilterOntoCalcRule()
    {
        super(new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(CalcRel.class, null),
                }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final FilterRel filter = (FilterRel) call.rels[0];
        final CalcRel calc = (CalcRel) call.rels[1];

        // Expand all references to columns in the condition, and AND with any
        // existing condition. For example,
        //
        // SELECT * FROM (
        //   SELECT a + b AS x, c AS y
        //   FROM t
        //   WHERE c < 6)
        // WHERE x > 5
        //
        // becomes
        //
        // SELECT a + b AS x, c AS y
        // FROM t
        // WHERE c < 6 AND (a + b) > 5
        final RexNode condition = RexUtil.clone(filter.condition);
        final RexShuttle shuttle =
            new RexShuttle() {
                public RexNode visit(RexInputRef input)
                {
                    return calc.projectExprs[input.index];
                }
            };
        RexNode newCondition = shuttle.visit(condition);
        if (calc.conditionExpr != null) {
            newCondition =
                calc.cluster.rexBuilder.makeCall(RexKind.And,
                    calc.conditionExpr, newCondition);
        }
        final CalcRel newCalc =
            new CalcRel(calc.cluster, calc.child,
                calc.getRowType(), calc.projectExprs, newCondition);
        call.transformTo(newCalc);
    }
}


// End MergeFilterOntoCalcRule.java

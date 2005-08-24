/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

package org.eigenbase.rel;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.relopt.RelOptUtil;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


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

    public static final MergeFilterOntoCalcRule instance =
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

        // Don't merge a filter onto a calc which contains windowed aggregates.
        // That would effectively be pushing a multiset down through a filter.
        // We'll have chance to merge later, when the over is expanded.
        if (RexOver.containsOver(
            calc.getProjectExprs(),
            calc.getCondition())) {
            return;
        }

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
        final RexShuttle shuttle =
            new RexShuttle() {
                public RexNode visit(RexInputRef input)
                {
                    return calc.projectExprs[input.getIndex()];
                }
            };
        RexNode newCondition = shuttle.visit(filter.getCondition());
        if (calc.getCondition() != null) {
            newCondition =
                calc.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.andOperator,
                    calc.getCondition(), newCondition);
        }
        final CalcRel newCalc =
            new CalcRel(calc.getCluster(), RelOptUtil.clone(calc.traits),
                calc.getChild(), calc.getRowType(), calc.projectExprs,
                newCondition);
        call.transformTo(newCalc);
    }
}


// End MergeFilterOntoCalcRule.java

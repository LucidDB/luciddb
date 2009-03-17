/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * Planner rule which merges a {@link FilterRel} and a {@link CalcRel}. The
 * result is a {@link CalcRel} whose filter condition is the logical AND of the
 * two.
 *
 * @author jhyde
 * @version $Id$
 * @see MergeFilterOntoCalcRule
 * @since Mar 7, 2004
 */
public class MergeFilterOntoCalcRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final MergeFilterOntoCalcRule instance =
        new MergeFilterOntoCalcRule();

    //~ Constructors -----------------------------------------------------------

    private MergeFilterOntoCalcRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(CalcRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final FilterRel filter = (FilterRel) call.rels[0];
        final CalcRel calc = (CalcRel) call.rels[1];

        // Don't merge a filter onto a calc which contains windowed aggregates.
        // That would effectively be pushing a multiset down through a filter.
        // We'll have chance to merge later, when the over is expanded.
        if (calc.getProgram().containsAggs()) {
            return;
        }

        // Create a program containing the filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RexProgramBuilder progBuilder =
            new RexProgramBuilder(
                calc.getRowType(),
                rexBuilder);
        progBuilder.addIdentity();
        progBuilder.addCondition(filter.getCondition());
        RexProgram topProgram = progBuilder.getProgram();
        RexProgram bottomProgram = calc.getProgram();

        // Merge the programs together.
        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);
        final CalcRel newCalc =
            new CalcRel(
                calc.getCluster(),
                RelOptUtil.clone(calc.traits),
                calc.getChild(),
                filter.getRowType(),
                mergedProgram,
                Collections.<RelCollation>emptyList());
        call.transformTo(newCalc);
    }
}

// End MergeFilterOntoCalcRule.java

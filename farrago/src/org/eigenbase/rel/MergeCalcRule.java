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

import org.eigenbase.relopt.*;
import org.eigenbase.rex.RexOver;
import org.eigenbase.rex.RexProgram;
import org.eigenbase.rex.RexProgramBuilder;


/**
 * Planner rule which merges a {@link CalcRel} onto a {@link CalcRel}. The
 * resulting {@link CalcRel} has the same project list as the upper
 * {@link CalcRel}, but expressed in terms of the lower {@link CalcRel}'s
 * inputs.
 *
 * @author jhyde
 * @since Mar 7, 2004
 * @version $Id$
 */
public class MergeCalcRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    public static final MergeCalcRule instance = new MergeCalcRule();

    //~ Constructors ----------------------------------------------------------

    private MergeCalcRule()
    {
        super(new RelOptRuleOperand(
                CalcRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(CalcRel.class, null),
                }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final CalcRel topCalc = (CalcRel) call.rels[0];
        final CalcRel bottomCalc = (CalcRel) call.rels[1];

        // Don't merge a calc which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter.
        RexProgram program = topCalc.getProgram();
        if (RexOver.containsOver(program)) {
            return;
        }

        // Merge the programs together.

        RexProgram mergedProgram = RexProgramBuilder.mergePrograms(
            topCalc.getProgram(),
            bottomCalc.getProgram(),
            topCalc.getCluster().getRexBuilder());
        final CalcRel newCalc =
            new CalcRel(
                bottomCalc.getCluster(),
                RelOptUtil.clone(bottomCalc.traits),
                bottomCalc.getChild(),
                topCalc.getRowType(),
                mergedProgram,
                RelCollation.emptyList);
        call.transformTo(newCalc);
    }

}

// End MergeCalcRule.java

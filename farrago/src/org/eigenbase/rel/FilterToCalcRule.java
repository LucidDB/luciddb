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
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;


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
public class FilterToCalcRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    public static final FilterToCalcRule instance = new FilterToCalcRule();

    //~ Constructors ----------------------------------------------------------

    private FilterToCalcRule()
    {
        super(new RelOptRuleOperand(
                FilterRel.class,
                null));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final FilterRel filter = (FilterRel) call.rels[0];
        final RelNode rel = filter.getChild();

        // Create a program containing a filter.
        final RexBuilder rexBuilder = filter.getCluster().getRexBuilder();
        final RelDataType inputRowType = rel.getRowType();
        final RexProgramBuilder programBuilder =
            new RexProgramBuilder(inputRowType, rexBuilder);
        programBuilder.addIdentity();
        if (filter.getCondition() != null) {
            programBuilder.addCondition(filter.getCondition());
        }
        final RexProgram program = programBuilder.getProgram();

        final CalcRel calc =
            new CalcRel(
                filter.getCluster(), RelOptUtil.clone(filter.traits), rel,
                inputRowType, program);
        call.transformTo(calc);
    }
}


// End FilterToCalcRule.java

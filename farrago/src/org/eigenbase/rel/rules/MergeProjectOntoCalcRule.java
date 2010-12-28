/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * Planner rule which merges a {@link ProjectRel} and a {@link CalcRel}. The
 * resulting {@link CalcRel} has the same project list as the original {@link
 * ProjectRel}, but expressed in terms of the original {@link CalcRel}'s inputs.
 *
 * @author jhyde
 * @version $Id$
 * @see MergeFilterOntoCalcRule
 * @since Mar 7, 2004
 */
public class MergeProjectOntoCalcRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public static final MergeProjectOntoCalcRule instance =
        new MergeProjectOntoCalcRule();

    //~ Constructors -----------------------------------------------------------

    private MergeProjectOntoCalcRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(CalcRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        final ProjectRel project = (ProjectRel) call.rels[0];
        final CalcRel calc = (CalcRel) call.rels[1];

        // Don't merge a project which contains windowed aggregates onto a
        // calc. That would effectively be pushing a windowed aggregate down
        // through a filter. Transform the project into an identical calc,
        // which we'll have chance to merge later, after the over is
        // expanded.
        RexProgram program =
            RexProgram.create(
                calc.getRowType(),
                project.getProjectExps(),
                null,
                project.getRowType(),
                project.getCluster().getRexBuilder());
        if (RexOver.containsOver(program)) {
            CalcRel projectAsCalc =
                new CalcRel(
                    project.getCluster(),
                    project.cloneTraits(),
                    calc,
                    project.getRowType(),
                    program,
                    Collections.<RelCollation>emptyList());
            call.transformTo(projectAsCalc);
            return;
        }

        // Create a program containing the project node's expressions.
        final RexBuilder rexBuilder = project.getCluster().getRexBuilder();
        final RexProgramBuilder progBuilder =
            new RexProgramBuilder(
                calc.getRowType(),
                rexBuilder);
        final RelDataTypeField [] fields = project.getRowType().getFields();
        for (int i = 0; i < project.getProjectExps().length; i++) {
            progBuilder.addProject(
                project.getProjectExps()[i],
                fields[i].getName());
        }
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
                RelOptUtil.clone(calc.getTraits()),
                calc.getChild(),
                project.getRowType(),
                mergedProgram,
                Collections.<RelCollation>emptyList());
        call.transformTo(newCalc);
    }
}

// End MergeProjectOntoCalcRule.java

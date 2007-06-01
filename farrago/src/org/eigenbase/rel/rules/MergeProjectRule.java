/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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
import org.eigenbase.rex.*;


/**
 * MergeProjectRule merges a {@link ProjectRel} into another {@link ProjectRel},
 * provided the projects aren't projecting identical sets of input references.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class MergeProjectRule
    extends RelOptRule
{
    // ~ Instance fields -------------------------------------------------------

    //~ Instance fields --------------------------------------------------------

    /**
     * if true, always merge projects
     */
    private boolean force;

    //~ Constructors -----------------------------------------------------------

    public MergeProjectRule(boolean force)
    {
        this();
        this.force = force;
        description = "MergeProjectRule: force mode";
    }

    public MergeProjectRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(ProjectRel.class, null)
                }));
        force = false;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel topProject = (ProjectRel) call.rels[0];
        ProjectRel bottomProject = (ProjectRel) call.rels[1];
        RexBuilder rexBuilder = topProject.getCluster().getRexBuilder();

        // if we're not in force mode and the two projects reference identical
        // inputs, then return and either let FennelRenameRule or
        // RemoveTrivialProjectRule replace the projects
        if (!force) {
            if (RelOptUtil.checkProjAndChildInputs(topProject, false)) {
                return;
            }
        }

        // create a RexProgram for the bottom project
        RexProgram bottomProgram =
            RexProgram.create(
                bottomProject.getChild().getRowType(),
                bottomProject.getProjectExps(),
                null,
                bottomProject.getRowType(),
                rexBuilder);

        // create a RexProgram for the topmost project
        RexNode [] projExprs = topProject.getProjectExps();
        RexProgram topProgram =
            RexProgram.create(
                bottomProject.getRowType(),
                projExprs,
                null,
                topProject.getRowType(),
                rexBuilder);

        // combine the two RexPrograms
        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        // re-expand the topmost projection expressions, now that they
        // reference the children of the bottom-most project
        int nProjExprs = projExprs.length;
        RexNode [] newProjExprs = new RexNode[nProjExprs];
        List<RexLocalRef> projList = mergedProgram.getProjectList();
        for (int i = 0; i < nProjExprs; i++) {
            newProjExprs[i] = mergedProgram.expandLocalRef(projList.get(i));
        }

        // replace the two projects with a combined projection
        ProjectRel newProjectRel =
            (ProjectRel) CalcRel.createProject(
                bottomProject.getChild(),
                newProjExprs,
                RelOptUtil.getFieldNames(topProject.getRowType()));

        call.transformTo(newProjectRel);
    }
}

// End MergeProjectRule.java

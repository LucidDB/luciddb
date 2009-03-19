/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * PushProjectIntoMultiJoinRule implements the rule for pushing projection
 * information from a {@link ProjectRel} into the {@link MultiJoinRel} that is
 * input into the {@link ProjectRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjectIntoMultiJoinRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public PushProjectIntoMultiJoinRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(MultiJoinRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel project = (ProjectRel) call.rels[0];
        MultiJoinRel multiJoin = (MultiJoinRel) call.rels[1];

        // if all inputs have their projFields set, then projection information
        // has already been pushed into each input
        boolean allSet = true;
        for (int i = 0; i < multiJoin.getInputs().length; i++) {
            if (multiJoin.getProjFields()[i] == null) {
                allSet = false;
                break;
            }
        }
        if (allSet) {
            return;
        }

        // create a new MultiJoinRel that reflects the columns in the projection
        // above the MultiJoinRel
        MultiJoinRel newMultiJoin =
            RelOptUtil.projectMultiJoin(multiJoin, project);
        ProjectRel newProject =
            (ProjectRel) CalcRel.createProject(
                newMultiJoin,
                project.getProjectExps(),
                RelOptUtil.getFieldNames(project.getRowType()));

        call.transformTo(newProject);
    }
}

// End PushProjectIntoMultiJoinRule.java

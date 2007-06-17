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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * PushProjectPastSetOpRule implements the rule for pushing a {@link ProjectRel}
 * past a {@link SetOpRel}. The children of the {@link SetOpRel} will project
 * only the {@link RexInputRef}s referenced in the original {@link ProjectRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjectPastSetOpRule
    extends RelOptRule
{
    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private Set<SqlOperator> preserveExprs;

    //~ Constructors -----------------------------------------------------------

    //  ~ Constructors ---------------------------------------------------------

    public PushProjectPastSetOpRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(SetOpRel.class, null)
                }));
        this.preserveExprs = Collections.EMPTY_SET;
    }

    public PushProjectPastSetOpRule(Set<SqlOperator> preserveExprs)
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(SetOpRel.class, null)
                }));
        this.preserveExprs = preserveExprs;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = (ProjectRel) call.rels[0];
        SetOpRel setOpRel = (SetOpRel) call.rels[1];

        // cannot push project past a distinct
        if (setOpRel.isDistinct()) {
            return;
        }

        // locate all fields referenced in the projection; if all fields are
        // being projected and there are no special expressions, no point in
        // proceeding any further
        PushProjector pushProject =
            new PushProjector(origProj, null, setOpRel, preserveExprs);
        if (pushProject.locateAllRefs()) {
            return;
        }

        RelNode [] setOpInputs = setOpRel.getInputs();
        int nSetOpInputs = setOpInputs.length;
        RelNode [] newSetOpInputs = new RelNode[nSetOpInputs];

        // project the input references, referenced in the original projection,
        // from each setop child
        for (int i = 0; i < nSetOpInputs; i++) {
            newSetOpInputs[i] =
                pushProject.createProjectRefsAndExprs(
                    setOpInputs[i],
                    true,
                    false);
        }

        // create a new setop whose children are the ProjectRels created above
        SetOpRel newSetOpRel =
            RelOptUtil.createNewSetOpRel(setOpRel, newSetOpInputs);

        // put the original project on top of the new setop, converting it to
        // reference the modified projection list
        int [] adjustments = pushProject.getAdjustments();
        ProjectRel newProject =
            pushProject.createNewProject(newSetOpRel, adjustments);

        call.transformTo(newProject);
    }
}

// End PushProjectPastSetOpRule.java

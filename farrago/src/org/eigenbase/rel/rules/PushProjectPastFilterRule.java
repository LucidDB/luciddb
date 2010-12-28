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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


/**
 * PushProjectPastFilterRule implements the rule for pushing a projection past a
 * filter.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjectPastFilterRule
    extends RelOptRule
{
    public static final PushProjectPastFilterRule instance =
        new PushProjectPastFilterRule();

    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private final PushProjector.ExprCondition preserveExprCondition;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushProjectPastFilterRule.
     */
    private PushProjectPastFilterRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(FilterRel.class, ANY)));
        this.preserveExprCondition = PushProjector.ExprCondition.FALSE;
    }

    /**
     * Creates a PushProjectPastFilterRule with an explicit root operand
     * and condition to preserve operands.
     *
     * @param operand root operand, must not be null
     *
     * @param id Part of description
     */
    public PushProjectPastFilterRule(
        RelOptRuleOperand operand,
        PushProjector.ExprCondition preserveExprCondition,
        String id)
    {
        super(operand, "PushProjectPastFilterRule: " + id);
        this.preserveExprCondition = preserveExprCondition;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj;
        FilterRel filterRel;

        if (call.rels.length == 2) {
            origProj = (ProjectRel) call.rels[0];
            filterRel = (FilterRel) call.rels[1];
        } else {
            origProj = null;
            filterRel = (FilterRel) call.rels[0];
        }
        RelNode rel = filterRel.getChild();
        RexNode origFilter = filterRel.getCondition();

        if ((origProj != null)
            && RexOver.containsOver(
                origProj.getProjectExps(),
                null))
        {
            // Cannot push project through filter if project contains a windowed
            // aggregate -- it will affect row counts. Abort this rule
            // invocation; pushdown will be considered after the windowed
            // aggregate has been implemented. It's OK if the filter contains a
            // windowed aggregate.
            return;
        }

        PushProjector pushProjector =
            new PushProjector(
                origProj, origFilter, rel, preserveExprCondition);
        ProjectRel topProject = pushProjector.convertProject(null);

        if (topProject != null) {
            call.transformTo(topProject);
        }
    }
}

// End PushProjectPastFilterRule.java

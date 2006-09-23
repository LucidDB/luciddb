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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * PushProjectPastFilterRule implements the rule for pushing a projection past a
 * filter.
 *
 * <p>REVIEW (jhyde, 2006/5/18): Rules of this kind, dealing in {@link
 * ProjectRel}s and {@link FilterRel}s, are deprecated. We should write rules
 * which deal with calcs, which are effectively projects and filters fused
 * together. It is still possible to ask a calc whether it is a pure project or
 * a pure filter, or to split a calc into its pure project and filter parts, if
 * desired. An added advantage is the corpus of code in {@link RexProgram} for
 * performing typical operations on {@link RexNode} expressions, in particular
 * common sub-expression elimination.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjectPastFilterRule
    extends RelOptRule
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private Set<SqlOperator> preserveExprs;

    //~ Constructors -----------------------------------------------------------

    //  ~ Constructors ---------------------------------------------------------

    public PushProjectPastFilterRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(FilterRel.class, null)
                }));
        this.preserveExprs = Collections.EMPTY_SET;
    }

    public PushProjectPastFilterRule(
        RelOptRuleOperand rule,
        Set<SqlOperator> preserveExprs,
        String id)
    {
        super(rule);
        this.preserveExprs = preserveExprs;
        description = "PushProjectPastFilterRule: " + id;
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
                null)) {
            // Cannot push project through filter if project contains a windowed
            // aggregate -- it will affect row counts. Abort this rule
            // invocation; pushdown will be considered after the windowed
            // aggregate has been implemented. It's OK if the filter contains a
            // windowed aggregate.
            return;
        }

        PushProjector pushProjector =
            new PushProjector(origProj, origFilter, rel, preserveExprs);
        ProjectRel topProject = pushProjector.convertProject(null);

        if (topProject != null) {
            call.transformTo(topProject);
        }
    }
}

// End PushProjectPastFilterRule.java

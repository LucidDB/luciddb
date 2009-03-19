/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2002-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2003-2009 John V. Sichi
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
 * PushFilterPastProjectRule implements the rule for pushing a {@link FilterRel}
 * past a {@link ProjectRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushFilterPastProjectRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public PushFilterPastProjectRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(ProjectRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = (FilterRel) call.rels[0];
        ProjectRel projRel = (ProjectRel) call.rels[1];

        // convert the filter to one that references the child of the project
        RexNode newCondition =
            RelOptUtil.pushFilterPastProject(filterRel.getCondition(), projRel);

        FilterRel newFilterRel =
            new FilterRel(
                filterRel.getCluster(),
                projRel.getChild(),
                newCondition);

        ProjectRel newProjRel =
            (ProjectRel) CalcRel.createProject(
                newFilterRel,
                projRel.getProjectExps(),
                RelOptUtil.getFieldNames(projRel.getRowType()));

        call.transformTo(newProjRel);
    }
}

// End PushFilterPastProjectRule.java

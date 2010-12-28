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
 * MergeFilterRule implements the rule for combining two {@link FilterRel}s
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class MergeFilterRule
    extends RelOptRule
{
    public static final MergeFilterRule instance = new MergeFilterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a MergeFilterRule.
     */
    private MergeFilterRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(FilterRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel topFilter = (FilterRel) call.rels[0];
        FilterRel bottomFilter = (FilterRel) call.rels[1];

        // use RexPrograms to merge the two FilterRels into a single program
        // so we can convert the two FilterRel conditions to directly
        // reference the bottom FilterRel's child
        RexBuilder rexBuilder = topFilter.getCluster().getRexBuilder();
        RexProgram bottomProgram = createProgram(bottomFilter);
        RexProgram topProgram = createProgram(topFilter);

        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        RexNode newCondition =
            mergedProgram.expandLocalRef(
                mergedProgram.getCondition());

        FilterRel newFilterRel =
            new FilterRel(
                topFilter.getCluster(),
                bottomFilter.getChild(),
                newCondition);

        call.transformTo(newFilterRel);
    }

    /**
     * Creates a RexProgram corresponding to a FilterRel
     *
     * @param filterRel the FilterRel
     *
     * @return created RexProgram
     */
    private RexProgram createProgram(FilterRel filterRel)
    {
        RexProgramBuilder programBuilder =
            new RexProgramBuilder(
                filterRel.getRowType(),
                filterRel.getCluster().getRexBuilder());
        programBuilder.addIdentity();
        programBuilder.addCondition(filterRel.getCondition());
        return programBuilder.getProgram();
    }
}

// End MergeFilterRule.java

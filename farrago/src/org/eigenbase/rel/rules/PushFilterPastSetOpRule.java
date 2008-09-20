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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * PushFilterPastSetOpRule implements the rule for pushing a {@link FilterRel}
 * past a {@link SetOpRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushFilterPastSetOpRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public PushFilterPastSetOpRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(SetOpRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = (FilterRel) call.rels[0];
        SetOpRel setOpRel = (SetOpRel) call.rels[1];

        RelNode [] setOpInputs = setOpRel.getInputs();
        int nSetOpInputs = setOpInputs.length;
        RelNode [] newSetOpInputs = new RelNode[nSetOpInputs];
        RelOptCluster cluster = setOpRel.getCluster();
        RexNode condition = filterRel.getCondition();

        // create filters on top of each setop child, modifying the filter
        // condition to reference each setop child
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        RelDataTypeField [] origFields = setOpRel.getRowType().getFields();
        int [] adjustments = new int[origFields.length];
        for (int i = 0; i < nSetOpInputs; i++) {
            RexNode newCondition =
                condition.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        origFields,
                        setOpInputs[i].getRowType().getFields(),
                        adjustments));
            newSetOpInputs[i] =
                new FilterRel(cluster, setOpInputs[i], newCondition);
        }

        // create a new setop whose children are the filters created above
        SetOpRel newSetOpRel =
            RelOptUtil.createNewSetOpRel(setOpRel, newSetOpInputs);

        call.transformTo(newSetOpRel);
    }
}

// End PushFilterPastSetOpRule.java

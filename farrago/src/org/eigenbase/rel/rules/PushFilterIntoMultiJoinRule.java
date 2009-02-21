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


/**
 * PushFilterIntoMultiJoinRule implements the rule for pushing a
 * {@link FilterRel} into a {@link MultiJoinRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushFilterIntoMultiJoinRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public PushFilterIntoMultiJoinRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(MultiJoinRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = (FilterRel) call.rels[0];        
        MultiJoinRel multiJoinRel = (MultiJoinRel) call.rels[1];

        MultiJoinRel newMultiJoinRel =
            new MultiJoinRel(
                multiJoinRel.getCluster(),
                multiJoinRel.getInputs(),
                multiJoinRel.getJoinFilter(),
                multiJoinRel.getRowType(),
                multiJoinRel.isFullOuterJoin(),
                multiJoinRel.getOuterJoinConditions(),
                multiJoinRel.getJoinTypes(),
                multiJoinRel.getProjFields(),
                multiJoinRel.getJoinFieldRefCountsMap(),
                filterRel.getCondition());

        call.transformTo(newMultiJoinRel);
    }
}

// End PushFilterIntoMultiJoinRule.java

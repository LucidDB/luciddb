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
 * PushSemiJoinPastFilterRule implements the rule for pushing semijoins down in
 * a tree past a filter in order to trigger other rules that will convert
 * semijoins. SemiJoinRel(FilterRel(X), Y) --> FilterRel(SemiJoinRel(X, Y))
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushSemiJoinPastFilterRule
    extends RelOptRule
{
    //  ~ Constructors --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    public PushSemiJoinPastFilterRule()
    {
        super(
            new RelOptRuleOperand(
                SemiJoinRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(FilterRel.class, null)
                }));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SemiJoinRel semiJoin = (SemiJoinRel) call.rels[0];
        FilterRel filter = (FilterRel) call.rels[1];

        RelNode newSemiJoin =
            new SemiJoinRel(
                semiJoin.getCluster(),
                filter.getChild(),
                semiJoin.getRight(),
                semiJoin.getCondition(),
                semiJoin.getLeftKeys(),
                semiJoin.getRightKeys());

        RelNode newFilter =
            CalcRel.createFilter(
                newSemiJoin,
                filter.getCondition());

        call.transformTo(newFilter);
    }
}

// End PushSemiJoinPastFilterRule.java

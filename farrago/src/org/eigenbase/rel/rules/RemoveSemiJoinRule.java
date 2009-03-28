/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2006-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Portions Copyright (C) 2006-2009 John V. Sichi
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

import org.eigenbase.relopt.*;


/**
 * RemoveSemiJoinRule implements the rule that removes semijoins from a join
 * tree if it turns out it's not possible to convert a SemiJoinRel to an indexed
 * scan on a join factor. Namely, if the join factor does not reduce to a single
 * table that can be scanned using an index. This rule should only be applied
 * after attempts have been made to convert SemiJoinRels.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class RemoveSemiJoinRule
    extends RelOptRule
{
    public static final RemoveSemiJoinRule instance =
        new RemoveSemiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a RemoveSemiJoinRule.
     */
    private RemoveSemiJoinRule()
    {
        super(new RelOptRuleOperand(SemiJoinRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        call.transformTo(call.rels[0].getInput(0));
    }
}

// End RemoveSemiJoinRule.java

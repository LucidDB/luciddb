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

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataTypeField;


/**
 * Rule to convert an {@link JoinRel inner join} to a {@link FilterRel filter}
 * on top of a {@link JoinRel cartesian inner join}.
 *
 * <p>One benefit of this transformation is that after it, the join condition
 * can be combined with conditions and expressions above the join. It also makes
 * the <code>FennelCartesianJoinRule</code> applicable.
 *
 * @author jhyde
 * @version $Id$
 * @since 3 February, 2006
 */
public final class ExtractJoinFilterRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton.
     */
    public static final ExtractJoinFilterRule instance =
        new ExtractJoinFilterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an ExtractJoinFilterRule.
     */
    private ExtractJoinFilterRule()
    {
        super(new RelOptRuleOperand(JoinRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        if (joinRel.getJoinType() != JoinRelType.INNER) {
            return;
        }

        if (joinRel.getCondition().isAlwaysTrue()) {
            return;
        }

        if (!joinRel.getSystemFieldList().isEmpty()) {
            // FIXME Enable this rule for joins with system fields
            return;
        }

        // NOTE jvs 14-Mar-2006:  See SwapJoinRule for why we
        // preserve attribute semiJoinDone here.

        RelNode cartesianJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                joinRel.getLeft(),
                joinRel.getRight(),
                joinRel.getCluster().getRexBuilder().makeLiteral(true),
                joinRel.getJoinType(),
                Collections.<String>emptySet(),
                joinRel.isSemiJoinDone(),
                Collections.<RelDataTypeField>emptyList());

        RelNode filterRel =
            CalcRel.createFilter(
                cartesianJoinRel,
                joinRel.getCondition());

        call.transformTo(filterRel);
    }
}

// End ExtractJoinFilterRule.java

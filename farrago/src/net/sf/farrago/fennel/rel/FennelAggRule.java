/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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
package net.sf.farrago.fennel.rel;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.Util;


/**
 * FennelAggRule is a rule for transforming {@link AggregateRel} to {@link
 * FennelAggRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelAggRule
    extends RelOptRule
{
    public static final FennelAggRule instance =
        new FennelAggRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelAggRule.
     */
    private FennelAggRule()
    {
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = (AggregateRel) call.rels[0];

        if (!aggRel.getSystemFieldList().isEmpty()) {
            return;
        }

        for (AggregateCall aggCall : aggRel.getAggCallList()) {
            if (aggCall.isDistinct()) {
                // AGG(DISTINCT x) must be rewritten before this rule
                // can apply
                return;
            }
            if (!FennelRelUtil.isFennelBuiltinAggFunction(aggCall)) {
                return;
            }
        }

        RelNode relInput = aggRel.getChild();
        RelNode fennelInput;

        if (!aggRel.getGroupSet().isEmpty()) {
            // add a FennelSortRel node beneath AggRel with sort keys
            // corresponding to the group by keys
            RelNode sortInput =
                convert(
                    relInput,
                    aggRel.getTraits().plus(FennelRel.FENNEL_EXEC_CONVENTION));
            if (sortInput == null) {
                return;
            }

            Integer [] keyProjection = Util.toArray(aggRel.getGroupSet());
            boolean discardDuplicates = false;
            FennelSortRel fennelSortRel =
                new FennelSortRel(
                    aggRel.getCluster(),
                    sortInput,
                    keyProjection,
                    discardDuplicates);
            fennelInput = fennelSortRel;
        } else {
            fennelInput =
                convert(
                    relInput,
                    aggRel.getTraits().plus(FennelRel.FENNEL_EXEC_CONVENTION));
            if (fennelInput == null) {
                return;
            }
        }

        FennelAggRel fennelAggRel =
            new FennelAggRel(
                aggRel.getCluster(),
                fennelInput,
                aggRel.getSystemFieldList(),
                aggRel.getGroupSet(),
                aggRel.getAggCallList());
        call.transformTo(fennelAggRel);
    }
}

// End FennelAggRule.java

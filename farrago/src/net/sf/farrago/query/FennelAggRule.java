/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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
package net.sf.farrago.query;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


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
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FennelAggRule object.
     */
    public FennelAggRule()
    {
        super(new RelOptRuleOperand(
                AggregateRel.class,
                null));
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

        AggregateRel.Call [] calls = aggRel.getAggCalls();
        for (int i = 0; i < calls.length; ++i) {
            if (calls[i].isDistinct()) {
                // AGG(DISTINCT x) must be rewritten before this rule
                // can apply
                return;
            }

            // TODO jvs 5-Oct-2005:  find a better way of detecting
            // whether the aggregate function is one of the builtins supported
            // by Fennel; also test whether we can handle input datatype
            try {
                FennelAggRel.lookupAggFunction(calls[i]);
            } catch (IllegalArgumentException ex) {
                return;
            }
        }

        RelNode relInput = aggRel.getChild();
        RelNode fennelInput;

        if (aggRel.getGroupCount() > 0) {
            // add a FennelSortRel node beneath AggRel with sort keys
            // corresponding to the group by keys
            RelNode sortInput =
                mergeTraitsAndConvert(
                    aggRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    relInput);
            if (sortInput == null) {
                return;
            }

            Integer [] keyProjection = new Integer[aggRel.getGroupCount()];
            for (int i = 0; i < keyProjection.length; ++i) {
                keyProjection[i] = i;
            }

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
                mergeTraitsAndConvert(
                    aggRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    relInput);
            if (fennelInput == null) {
                return;
            }
        }

        FennelAggRel fennelAggRel =
            new FennelAggRel(
                aggRel.getCluster(),
                fennelInput,
                aggRel.getGroupCount(),
                aggRel.getAggCalls());
        call.transformTo(fennelAggRel);
    }
}

// End FennelAggRule.java

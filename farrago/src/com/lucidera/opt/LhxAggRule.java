/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
package com.lucidera.opt;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * LhxAggRule is a rule for transforming {@link AggregateRel} to {@link
 * LhxAggRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxAggRule
    extends RelOptRule
{

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LhxAggRule object.
     */
    public LhxAggRule()
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
                LhxAggRel.lookupAggFunction(calls[i]);
            } catch (IllegalArgumentException ex) {
                return;
            }
        }

        RelNode relInput = aggRel.getChild();
        RelNode fennelInput;

        fennelInput =
            mergeTraitsAndConvert(
                aggRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        Double numInputRows = RelMetadataQuery.getRowCount(fennelInput);
        if (numInputRows == null) {
            numInputRows = 10000.0;
        }

        // Derive cardinality of RHS join keys.
        Double cndGroupByKey;
        BitSet groupByKeyMap = new BitSet();

        for (int i = 0; i < aggRel.getGroupCount(); i++) {
            groupByKeyMap.set(i);
        }

        cndGroupByKey =
            RelMetadataQuery.getPopulationSize(
                fennelInput,
                groupByKeyMap);

        if ((cndGroupByKey == null) || (cndGroupByKey > numInputRows)) {
            cndGroupByKey = numInputRows;
        }

        if (aggRel.getGroupCount() > 0) {
            LhxAggRel lhxAggRel =
                new LhxAggRel(
                    aggRel.getCluster(),
                    fennelInput,
                    aggRel.getGroupCount(),
                    aggRel.getAggCalls(),
                    numInputRows.intValue(),
                    cndGroupByKey.intValue());

            call.transformTo(lhxAggRel);
        } else {
            FennelAggRel fennelAggRel =
                new FennelAggRel(
                    aggRel.getCluster(),
                    fennelInput,
                    aggRel.getGroupCount(),
                    aggRel.getAggCalls());
            call.transformTo(fennelAggRel);
        }
    }
}

// End LhxAggRule.java

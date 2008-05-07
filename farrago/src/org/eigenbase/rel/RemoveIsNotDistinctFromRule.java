/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 Disruptive Tech
// Copyright (C) 2006-2007 LucidEra, Inc.
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
package org.eigenbase.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.*;


/**
 * Rule to replace isNotDistinctFromOperator with logical equivalent conditions
 * in a {@link FilterRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public final class RemoveIsNotDistinctFromRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton.
     */
    public static final RemoveIsNotDistinctFromRule instance =
        new RemoveIsNotDistinctFromRule();

    //~ Constructors -----------------------------------------------------------

    private RemoveIsNotDistinctFromRule()
    {
        super(new RelOptRuleOperand(FilterRel.class, ANY));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        FilterRel oldFilterRel = (FilterRel) call.rels[0];
        RexNode oldFilterCond = oldFilterRel.getCondition();

        if (RexUtil.findOperatorCall(
                SqlStdOperatorTable.isNotDistinctFromOperator,
                oldFilterCond)
            == null)
        {
            // no longer contains isNotDistinctFromOperator
            return;
        }

        // Now replace all the "a isNotDistinctFrom b"
        // with the RexNode given by RelOptUtil.isDistinctFrom() method

        RemoveIsNotDistinctFromRexShuttle rewriteShuttle =
            new RemoveIsNotDistinctFromRexShuttle(
                oldFilterRel.getCluster().getRexBuilder());

        RelNode newFilterRel =
            CalcRel.createFilter(
                oldFilterRel.getChild(),
                oldFilterCond.accept(rewriteShuttle));

        call.transformTo(newFilterRel);
    }

    //~ Inner Classes ----------------------------------------------------------

    private class RemoveIsNotDistinctFromRexShuttle
        extends RexShuttle
    {
        RexBuilder rexBuilder;

        public RemoveIsNotDistinctFromRexShuttle(
            RexBuilder rexBuilder)
        {
            this.rexBuilder = rexBuilder;
        }

        // override RexShuttle
        public RexNode visitCall(RexCall call)
        {
            RexNode newCall = super.visitCall(call);

            if (call.getOperator()
                == SqlStdOperatorTable.isNotDistinctFromOperator)
            {
                RexCall tmpCall = (RexCall) newCall;
                newCall =
                    RelOptUtil.isDistinctFrom(
                        rexBuilder,
                        tmpCall.getOperands()[0],
                        tmpCall.getOperands()[1],
                        true);
            }
            return newCall;
        }
    }
}

// End RemoveIsNotDistinctFromRule.java

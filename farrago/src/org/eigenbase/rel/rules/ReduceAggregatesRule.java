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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;


/**
 * Rule to reduce aggregates to simpler forms. Currently only AVG(x) to
 * SUM(x)/COUNT(x), but eventually will handle others such as STDDEV.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class ReduceAggregatesRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton.
     */
    public static final ReduceAggregatesRule instance =
        new ReduceAggregatesRule();

    //~ Constructors -----------------------------------------------------------

    private ReduceAggregatesRule()
    {
        super(new RelOptRuleOperand(AggregateRel.class, null));
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall ruleCall)
    {
        AggregateRel oldAggRel = (AggregateRel) ruleCall.rels[0];

        int i = -1;
        for (AggregateCall aggCall : oldAggRel.getAggCallList()) {
            ++i;
            if (aggCall.getAggregation().getName().equals("AVG")) {
                // NOTE jvs 24-Apr-2006: To make life simple, we peel these off
                // one at a time.  This may appear extravagant; the assumption
                // is that there will be other rules to take care of combining
                // the extra projections that result in case of multiple AVG
                // calls, and more importantly, recognizing the COUNT common
                // subexpressions in the replacement agg.  Both of these are
                // better done generically (since user SQL may already include
                // such redundancy) rather than complicating the rule here.
                reduceAverage(ruleCall, oldAggRel, aggCall, i);
                return;
            }
        }
    }

    private void reduceAverage(
        RelOptRuleCall ruleCall,
        AggregateRel oldAggRel,
        AggregateCall avgCall,
        int iOldCall)
    {
        RelDataTypeFactory typeFactory =
            oldAggRel.getCluster().getTypeFactory();
        RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

        List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
        int nGroups = oldAggRel.getGroupCount();

        // + 1 for COUNT; we'll replace AVG with SUM to make
        // projection arithmetic easier
        List<AggregateCall> newCalls =
            new ArrayList<AggregateCall>();

        assert (avgCall.getArgList().size() == 1);
        int iAvgInput = avgCall.getArgList().get(0);

        List<RexNode> projList = new ArrayList<RexNode>();

        // pass through group key
        for (int i = 0; i < nGroups; ++i) {
            projList.add(
                rexBuilder.makeInputRef(
                    getFieldType(oldAggRel, i),
                    i));
        }

        // create new agg function calls and rest of project list together
        SqlAggFunction countAgg = SqlStdOperatorTable.countOperator;
        RelDataType countType = countAgg.getReturnType(typeFactory);
        for (int i = 0; i < oldCalls.size() + 1; ++i) {
            if (i == iOldCall) {
                // replace original AVG with SUM
                RelDataType avgInputType =
                    getFieldType(
                        oldAggRel.getChild(),
                        iAvgInput);
                RelDataType sumType =
                    typeFactory.createTypeWithNullability(
                        avgInputType,
                        true);
                SqlSumAggFunction sumAgg = new SqlSumAggFunction(sumType);
                newCalls.add(
                    new AggregateCall(
                        sumAgg,
                        avgCall.isDistinct(),
                        avgCall.getArgList(),
                        sumType,
                        null));

                // NOTE:  these references are with respect to the output
                // of newAggRel
                RexNode numeratorRef =
                    rexBuilder.makeInputRef(
                        sumType,
                        nGroups + i);
                RexNode denominatorRef =
                    rexBuilder.makeInputRef(
                        countType,
                        nGroups + oldCalls.size());
                projList.add(
                    rexBuilder.makeCast(
                        avgCall.getType(),
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.divideOperator,
                            numeratorRef,
                            denominatorRef)));
            } else if (i == oldCalls.size()) {
                // at end:  append new COUNT
                newCalls.add(
                    new AggregateCall(
                        countAgg,
                        avgCall.isDistinct(),
                        avgCall.getArgList(),
                        countType,
                        null));
            } else {
                // anything else:  preserve original call
                final AggregateCall oldCall = oldCalls.get(i);
                newCalls.add(oldCall);
                projList.add(
                    rexBuilder.makeInputRef(
                        oldCall.getType(),
                        i + nGroups));
            }
        }

        AggregateRel newAggRel =
            new AggregateRel(
                oldAggRel.getCluster(),
                oldAggRel.getChild(),
                nGroups,
                newCalls);

        RelNode projectRel =
            CalcRel.createProject(
                newAggRel,
                projList,
                null);

        ruleCall.transformTo(projectRel);
    }

    private RelDataType getFieldType(RelNode relNode, int i)
    {
        RelDataTypeField inputField = relNode.getRowType().getFields()[i];
        return inputField.getType();
    }
}

// End ReduceAggregatesRule.java

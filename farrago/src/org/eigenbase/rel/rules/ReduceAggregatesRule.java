/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2007 The Eigenbase Project
// Copyright (C) 2006-2007 SQLstream, Inc.
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
        new ReduceAggregatesRule(
            new RelOptRuleOperand(AggregateRel.class, ANY));

    //~ Constructors -----------------------------------------------------------

    protected ReduceAggregatesRule(RelOptRuleOperand operand)
    {
        super(operand);
    }

    //~ Methods ----------------------------------------------------------------

    public void onMatch(RelOptRuleCall ruleCall)
    {
        AggregateRelBase oldAggRel = (AggregateRelBase) ruleCall.rels[0];

        for (AggregateCall aggCall : oldAggRel.getAggCallList()) {
            if (aggCall.getAggregation() instanceof SqlAvgAggFunction) {
                reduceAverages(ruleCall, oldAggRel);
                return;
            }
        }
    }

    /*
     * This will reduce all avg's in aggregates list to sum/count. It needs to
     * handle newly generated common subexpressions since this was done
     * at the sql2rel stage.
     */
    private void reduceAverages(
        RelOptRuleCall ruleCall,
        AggregateRelBase oldAggRel)
    {
        RelDataTypeFactory typeFactory =
            oldAggRel.getCluster().getTypeFactory();
        RexBuilder rexBuilder = oldAggRel.getCluster().getRexBuilder();

        List<AggregateCall> oldCalls = oldAggRel.getAggCallList();
        int nGroups = oldAggRel.getGroupCount();

        List<AggregateCall> newCalls = new ArrayList<AggregateCall>();
        Map<AggregateCall, RexNode> aggCallMapping =
            new HashMap<AggregateCall, RexNode>();

        List<RexNode> projList = new ArrayList<RexNode>();

        // pass through group key
        for (int i = 0; i < nGroups; ++i) {
            projList.add(
                rexBuilder.makeInputRef(
                    getField(oldAggRel, i),
                    i));
        }

        // create new agg function calls and rest of project list together
        SqlAggFunction countAgg = SqlStdOperatorTable.countOperator;
        RelDataType countType = countAgg.getReturnType(typeFactory);
        for (AggregateCall oldCall : oldCalls) {
            if (oldCall.getAggregation() instanceof SqlAvgAggFunction) {
                // replace original AVG with SUM/COUNT
                int iAvgInput = oldCall.getArgList().get(0);
                RelDataType avgInputType =
                    getField(
                        oldAggRel.getChild(),
                        iAvgInput);
                RelDataType sumType =
                    typeFactory.createTypeWithNullability(
                        avgInputType,
                        true);
                SqlSumAggFunction sumAgg = new SqlSumAggFunction(sumType);
                AggregateCall sumCall =
                    new AggregateCall(
                        sumAgg,
                        oldCall.isDistinct(),
                        oldCall.getArgList(),
                        sumType,
                        null);
                AggregateCall countCall =
                    new AggregateCall(
                        countAgg,
                        oldCall.isDistinct(),
                        oldCall.getArgList(),
                        countType,
                        null);

                // NOTE:  these references are with respect to the output
                // of newAggRel
                RexNode numeratorRef =
                    rexBuilder.addAggCall(
                        sumCall,
                        nGroups,
                        newCalls,
                        aggCallMapping);
                RexNode denominatorRef =
                    rexBuilder.addAggCall(
                        countCall,
                        nGroups,
                        newCalls,
                        aggCallMapping);
                projList.add(
                    rexBuilder.makeCast(
                        oldCall.getType(),
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.divideOperator,
                            numeratorRef,
                            denominatorRef)));
            } else {
                // anything else:  preserve original call
                projList.add(
                    rexBuilder.addAggCall(
                        oldCall,
                        nGroups,
                        newCalls,
                        aggCallMapping));
            }
        }

        AggregateRelBase newAggRel =
            newAggregateRel(
                oldAggRel,
                newCalls);

        List<String> fieldNames = new ArrayList<String>();
        for (RelDataTypeField field : oldAggRel.getRowType().getFieldList()) {
            fieldNames.add(field.getName());
        }
        RelNode projectRel =
            CalcRel.createProject(
                newAggRel,
                projList,
                fieldNames);

        ruleCall.transformTo(projectRel);
    }

    /**
     * Do a shallow clone of oldAggRel and update aggCalls. Could be refactored
     * into AggregateRelBase and subclasses - but it's only needed for some
     * subclasses.
     *
     * @param oldAggRel AggregateRel to clone.
     * @param newCalls New list of AggregateCalls
     *
     * @return shallow clone with new list of AggregateCalls.
     */
    protected AggregateRelBase newAggregateRel(
        AggregateRelBase oldAggRel,
        List<AggregateCall> newCalls)
    {
        return new AggregateRel(
            oldAggRel.getCluster(),
            oldAggRel.getChild(),
            oldAggRel.getGroupCount(),
            newCalls);
    }

    private RelDataType getField(RelNode relNode, int i)
    {
        final RelDataTypeField inputField = relNode.getRowType().getFields()[i];
        return inputField.getType();
    }
}

// End ReduceAggregatesRule.java

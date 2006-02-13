/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2006-2006 The Eigenbase Project
// Copyright (C) 2006-2006 Disruptive Tech
// Copyright (C) 2006-2006 LucidEra, Inc.
// Portions Copyright (C) 2006-2006 John V. Sichi
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
import org.eigenbase.reltype.*;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.util.Util;

import java.util.*;

/**
 * Rule to remove distinct aggregates from a {@link AggregateRel}.
 *
 * @author jhyde
 * @version $Id$
 *
 * @since 3 February, 2006
 */
public final class RemoveDistinctAggregateRule extends RelOptRule
{
    /**
     * The singleton.
     */
    public static final RemoveDistinctAggregateRule instance =
        new RemoveDistinctAggregateRule();

    //~ Constructors ----------------------------------------------------------
    private RemoveDistinctAggregateRule()
    {
        super(new RelOptRuleOperand(AggregateRel.class, null));
    }

    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggregate = (AggregateRel) call.rels[0];
        if (!aggregate.containsDistinctCall()) {
            return;
        }

        // Find all of the agg expressions. We use a LinkedHashSet to ensure
        // determinism.
        int nonDistinctCount = 0;
        Set<List<Integer> > argListSets = new LinkedHashSet<List<Integer> >();
        for (AggregateRelBase.Call aggCall : aggregate.aggCalls) {
            if (!aggCall.isDistinct()) {
                ++nonDistinctCount;
                continue;
            }
            ArrayList<Integer> argList = new ArrayList<Integer>();
            for (int arg : aggCall.getArgs()) {
                argList.add(arg);
            }
            argListSets.add(argList);
        }
        Util.permAssert(argListSets.size() > 0, "containsDistinctCall lied");

        // If all of the agg expressions are distinct and have the same
        // arguments then we can use a more efficient form.
        if (nonDistinctCount == 0 &&
            argListSets.size() == 1) {
            RelNode converted = convertMonopole(
                aggregate, argListSets.iterator().next());
            call.transformTo(converted);
            return;
        }

        // For each set of operands, find and rewrite all calls which have that
        // set of operands.
        RelNode rel = aggregate.getChild();
        final AggregateRelBase.Call[] newAggCalls = aggregate.aggCalls.clone();
        for (List<Integer> argList : argListSets) {
            rel = doRewrite(aggregate, rel, argList, newAggCalls);
        }

        // Create an aggregate with the new aggregate list.
        AggregateRel newAggregate = new AggregateRel(
            aggregate.getCluster(),
            rel,
            aggregate.groupCount,
            newAggCalls);

        call.transformTo(newAggregate);
    }

    /**
     * Converts an aggregrate relational expression which contains just one
     * distinct aggregate function (or perhaps several over the same arguments)
     * and no non-distinct aggregate functions.
     */
    private RelNode convertMonopole(
        AggregateRel aggregate,
        List<Integer> argList)
    {
        // For example,
        //    SELECT deptno, COUNT(DISTINCT sal), SUM(DISTINCT sal)
        //    FROM emp
        //    GROUP BY deptno
        //
        // becomes
        //
        //    SELECT deptno, COUNT(distinct_sal), SUM(distinct_sal)
        //    FROM (
        //      SELECT DISTINCT deptno, sal AS distinct_sal
        //      FROM EMP GROUP BY deptno)
        //    GROUP BY deptno

        // Project the columns of the GROUP BY plus the arguments
        // to the agg function.
        Map<Integer,Integer> sourceOf = new HashMap<Integer, Integer>();
        final AggregateRel distinct =
            createSelectDistinct(aggregate, sourceOf, argList);

        // Create an aggregate on top.
        AggregateRel newAggregate =
            createTopAgg(aggregate, argList, sourceOf, distinct);

        return newAggregate;
    }

    /**
     * Converts all distinct aggregate calls to a given set of arguments.
     *
     * <p>This method is called several times, one for each set of arguments.
     * Each time it is called, it generates a JOIN to a new SELECT DISTINCT
     * relational expression, and modifies the set of top-level calls.
     *
     * @param aggregate Original aggregate
     * @param left Child relational expression (either the original aggregate,
     *   or the output from the previous call to this method)
     * @param argList Arguments to the distinct aggregate function
     * @param newAggCalls Array of calls. Those relating to this arg list will
     *   be modified
     * @return Relational expression
     */
    private RelNode doRewrite(
        AggregateRel aggregate,
        RelNode left,
        List<Integer> argList,
        AggregateRelBase.Call[] newAggCalls)
    {
        final RexBuilder rexBuilder = aggregate.getCluster().getRexBuilder();

        // AggregateRel(
        //   child,
        //   {COUNT(DISTINCT 1), SUM(DISTINCT 1), SUM(2)})
        //
        // becomes
        //
        // AggregateRel(
        //   JoinRel(
        //     child,
        //     AggregateRel(
        //       child,
        //       < all columns >
        //       {})
        //     INNER,
        //     <f2 = f5>))
        //
        // E.g.
        //   SELECT deptno, SUM(sal), COUNT(DISTINCT gender)
        //   FROM Emps
        //
        // becomes
        //
        //   SELECT e.deptno, SUM(e.sal), COUNT(d.gender)
        //   FROM Emps AS e
        //   JOIN (
        //     SELECT DISTINCT deptno, gender
        //     FROM Emps) AS de
        //   ON e.deptno = de.deptno
        //   GROUP BY e.deptno, de.deptno

        // Project the columns of the GROUP BY plus the arguments
        // to the agg function.
        Map<Integer,Integer> sourceOf = new HashMap<Integer, Integer>();
        final AggregateRel distinct =
            createSelectDistinct(aggregate, sourceOf, argList);

        // Create the join condition. It is of the form
        //  'left.f0 = right.f0 and left.f1 = right.f1 and ...'
        // where {f0, f1, ...} are the GROUP BY fields.
        final RelDataTypeField[] distinctFields =
            distinct.getRowType().getFields();
        final RelDataTypeField[] leftFields =
            left.getRowType().getFields();
        RexNode condition = null;
        for (Integer arg : argList) {
            final int leftOrdinal = arg;
            final int rightOrdinal = sourceOf.get(arg);
            RexNode equi = rexBuilder.makeCall(
                SqlStdOperatorTable.equalsOperator,
                new RexInputRef(
                    leftOrdinal,
                    leftFields[leftOrdinal].getType()),
                new RexInputRef(
                    leftFields.length + rightOrdinal,
                    distinctFields[rightOrdinal].getType()));
            if (condition == null) {
                condition = equi;
            } else {
                condition = rexBuilder.makeCall(
                    SqlStdOperatorTable.andOperator,
                    condition,
                    equi);
            }
        }

        // Join in the new 'select distinct' relation.
        final RelNode join =
            new JoinRel(
                aggregate.getCluster(),
                left,
                distinct,
                condition,
                JoinRelType.INNER,
                Collections.emptySet());

        xxxxx(newAggCalls, argList, sourceOf);

        return join;
    }

    /**
     * @deprecated maybe inline this
     */ 
    private static AggregateRel createTopAgg(
        AggregateRel aggregate,
        List<Integer> argList,
        Map<Integer, Integer> sourceOf,
        final RelNode child)
    {
        final AggregateRelBase.Call[] newAggCalls = aggregate.aggCalls.clone();
        xxxxx(newAggCalls, argList, sourceOf);

        // Create an aggregate with the new aggregate list.
        AggregateRel newAggregate = new AggregateRel(
            aggregate.getCluster(),
            child,
            aggregate.groupCount,
            newAggCalls);
        return newAggregate;
    }

    private static void xxxxx(
        final AggregateRelBase.Call[] newAggCalls, 
        List<Integer> argList, 
        Map<Integer, Integer> sourceOf)
    {
        // Rewrite the agg calls. Each distinct agg becomes a non-distinct call
        // to the corresponding field from the right; for example,
        //   "COUNT(DISTINCT e.sal)"
        // becomes
        //   "COUNT(distinct_e.sal)".
        for (int i = 0; i < newAggCalls.length; i++) {
            final AggregateRelBase.Call aggCall = newAggCalls[i];
            // Ignore agg calls which are not distinct or have the wrong set
            // arguments. If we're rewriting aggs whose args are {sal}, we will
            // rewrite COUNT(DISTINCT sal) and SUM(DISTINCT sal) but ignore
            // COUNT(DISTINCT gender) or SUM(sal).
            if (!aggCall.isDistinct()) {
                continue;
            }
            if (!equals(aggCall.getArgs(), argList)) {
                continue;
            }
            // Re-map arguments.
            final int[] newArgs = aggCall.getArgs().clone();
            for (int j = 0; j < newArgs.length; j++) {
                newArgs[j] = sourceOf.get(newArgs[j]);
            }
            final AggregateRelBase.Call newAggCall =
                new AggregateRelBase.Call(
                    aggCall.getAggregation(),
                    false,
                    newArgs);
            newAggCalls[i] = newAggCall;
        }
    }

    private static AggregateRel createSelectDistinct(
        AggregateRel aggregate,
        Map<Integer, Integer> sourceOf,
        List<Integer> argList)
    {
        List<RexNode> exprList = new ArrayList<RexNode>();
        List<String> nameList = new ArrayList<String>();
        final RelNode child = aggregate.getChild();
        final RelDataTypeField[] childFields = child.getRowType().getFields();
        for (int i = 0; i < aggregate.getGroupCount(); i++) {
            exprList.add(new RexInputRef(i, childFields[i].getType()));
            nameList.add(childFields[i].getName());
            sourceOf.put(i, i);
        }
        for (Integer arg : argList) {
            if (sourceOf.get(arg) != null) {
                continue;
            }
            sourceOf.put(arg, exprList.size());
            exprList.add(new RexInputRef(arg, childFields[arg].getType()));
            nameList.add(childFields[arg].getName());
        }
        final RelNode project =
            CalcRel.createProject(
                child,
                (RexNode[]) exprList.toArray(new RexNode[exprList.size()]),
                (String[]) nameList.toArray(new String[nameList.size()]));

        // Get the distinct values of the GROUP BY fields and the arguments
        // to the agg functions.
        List<AggregateRelBase.Call> distinctAggCallList =
            new ArrayList<AggregateRelBase.Call>();
        final AggregateRel distinct =
            new AggregateRel(
                aggregate.getCluster(),
                project,
                exprList.size(),
                (AggregateRelBase.Call[]) distinctAggCallList.toArray(
                    new AggregateRelBase.Call[distinctAggCallList.size()]));
        return distinct;
    }

    /**
     * Returns whether an integer array has the same content as an integer
     * list.
     */
    private static boolean equals(int[] args, List<Integer> argList)
    {
        if (args.length != argList.size()) {
            return false;
        }
        for (int i = 0; i < args.length; i++) {
            if (args[i] != argList.get(i)) {
                return false;
            }
        }
        return true;
    }
}

// End RemoveDistinctAggregateRule.java

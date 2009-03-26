/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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

import java.util.*;
import java.util.logging.*;

import net.sf.farrago.trace.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * Planner rule which folds projections and filters into an underlying {@link
 * ValuesRel}. Returns an {@link EmptyRel} if all rows are filtered away.
 *
 * <p>For example,
 *
 * <blockquote><code>select a - b from (values (1, 2), (3, 5), (7, 11)) as t (a,
 * b) where a + b > 4</code></blockquote>
 * becomes
 *
 * <blockquote><code>select x from (values (-2), (-4))</code></blockquote>
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class FarragoReduceValuesRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    private static final Logger tracer = FarragoTrace.getOptimizerRuleTracer();

    /**
     * Singleton instance of this rule which applies to the pattern
     * Filter(Values).
     */
    public static final FarragoReduceValuesRule filterInstance =
        new FarragoReduceValuesRule(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(
                    ValuesRel.class)),
            "FilterRel") {
            public void onMatch(RelOptRuleCall call)
            {
                apply(
                    call,
                    null,
                    (FilterRel) call.rels[0],
                    (ValuesRel) call.rels[1]);
            }
        };

    /**
     * @deprecated use {@link #filterInstance} instead
     */
    public static final FarragoReduceValuesRule FILTER_INSTANCE =
        filterInstance;

    /**
     * Singleton instance of this rule which applies to the pattern
     * Project(Values).
     */
    public static final FarragoReduceValuesRule projectInstance =
        new FarragoReduceValuesRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(
                    ValuesRel.class)),
            "ProjectRel") {
            public void onMatch(RelOptRuleCall call)
            {
                apply(
                    call,
                    (ProjectRel) call.rels[0],
                    null,
                    (ValuesRel) call.rels[1]);
            }
        };

    /**
     * @deprecated use {@link #projectInstance} instead
     */
    public static final FarragoReduceValuesRule PROJECT_INSTANCE =
        projectInstance;

    /**
     * Singleton instance of this rule which applies to the pattern
     * Project(Filter(Values)).
     */
    public static final FarragoReduceValuesRule projectFilterInstance =
        new FarragoReduceValuesRule(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(
                    FilterRel.class,
                    new RelOptRuleOperand(
                        ValuesRel.class))),
            "ProjectRel+FilterRel") {
            public void onMatch(RelOptRuleCall call)
            {
                apply(
                    call,
                    (ProjectRel) call.rels[0],
                    (FilterRel) call.rels[1],
                    (ValuesRel) call.rels[2]);
            }
        };

    /**
     * @deprecated use {@link #projectFilterInstance} instead
     */
    public static final FarragoReduceValuesRule PROJECT_FILTER_INSTANCE =
        projectFilterInstance;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new FarragoReduceExpressionsRule object.
     *
     * @param operand class of rels to which this rule should apply
     */
    private FarragoReduceValuesRule(RelOptRuleOperand operand, String desc)
    {
        super(operand);
        description = "FarragoReduceValuesRule:" + desc;
        Util.discard(tracer);
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Does the work.
     *
     * @param call Rule call
     * @param project Project, may be null
     * @param filter Filter, may be null
     * @param values Values rel to be reduced
     */
    protected void apply(
        RelOptRuleCall call,
        ProjectRel project,
        FilterRel filter,
        ValuesRel values)
    {
        assert values != null;
        assert (filter != null) || (project != null);
        final RexNode conditionExpr =
            (filter == null) ? null : filter.getCondition();
        final RexNode [] projectExprs =
            (project == null) ? null : project.getProjectExps();
        RexBuilder rexBuilder = values.getCluster().getRexBuilder();

        // Find reducible expressions.
        List<RexNode> reducibleExps = new ArrayList<RexNode>();
        final MyRexShuttle shuttle = new MyRexShuttle();
        for (final List<RexLiteral> literalList : values.getTuples()) {
            shuttle.literalList = literalList;
            if (conditionExpr != null) {
                RexNode c = conditionExpr.accept(shuttle);
                reducibleExps.add(c);
            }
            if (projectExprs != null) {
                int k = -1;
                for (RexNode projectExpr : projectExprs) {
                    ++k;
                    RexNode e = projectExpr.accept(shuttle);
                    if (RexLiteral.isNullLiteral(e)) {
                        e = rexBuilder.makeAbstractCast(
                            project.getRowType().getFieldList().get(k)
                                   .getType(),
                            e);
                    }
                    reducibleExps.add(e);
                }
            }
        }
        int fieldsPerRow =
            ((conditionExpr == null) ? 0 : 1)
            + ((projectExprs == null) ? 0 : projectExprs.length);
        assert fieldsPerRow > 0;
        assert reducibleExps.size()
            == (values.getTuples().size() * fieldsPerRow);

        // Compute the values they reduce to.
        FarragoReduceExpressionsRule.reduceExpressions(
            values,
            reducibleExps);

        int changeCount = 0;
        final List<List<RexLiteral>> tupleList =
            new ArrayList<List<RexLiteral>>();
        for (int row = 0; row < values.getTuples().size(); ++row) {
            int i = 0;
            RexNode reducedValue;
            if (conditionExpr != null) {
                reducedValue = reducibleExps.get((row * fieldsPerRow) + i);
                ++i;
                if (!reducedValue.isAlwaysTrue()) {
                    ++changeCount;
                    continue;
                }
            }

            List<RexLiteral> valuesList = new ArrayList<RexLiteral>();
            if (projectExprs != null) {
                ++changeCount;
                for (; i < fieldsPerRow; ++i) {
                    reducedValue = reducibleExps.get((row * fieldsPerRow) + i);
                    if (reducedValue instanceof RexLiteral) {
                        valuesList.add((RexLiteral) reducedValue);
                    } else if (RexUtil.isNullLiteral(reducedValue, true)) {
                        valuesList.add(rexBuilder.constantNull());
                    } else {
                        return;
                    }
                }
            } else {
                valuesList = values.getTuples().get(row);
            }
            tupleList.add(valuesList);
        }

        if (changeCount > 0) {
            final RelDataType rowType;
            if (projectExprs != null) {
                rowType = project.getRowType();
            } else {
                rowType = values.getRowType();
            }
            final RelNode newRel;
            if (tupleList.isEmpty()) {
                newRel =
                    new EmptyRel(
                        values.getCluster(),
                        rowType);
            } else {
                newRel =
                    new ValuesRel(
                        values.getCluster(),
                        rowType,
                        tupleList);
            }
            call.transformTo(newRel);

            // New plan is absolutely better than old plan.
            call.getPlanner().setImportance(filter, 0.0);
        } else {
            // Filter had no effect, so we can say that Filter(Values) ==
            // Values.
            call.transformTo(values);
        }
    }

    //~ Inner Classes ----------------------------------------------------------

    private static class MyRexShuttle
        extends RexShuttle
    {
        private List<RexLiteral> literalList;

        public RexNode visitInputRef(RexInputRef inputRef)
        {
            return literalList.get(inputRef.getIndex());
        }
    }
}

// End FarragoReduceValuesRule.java

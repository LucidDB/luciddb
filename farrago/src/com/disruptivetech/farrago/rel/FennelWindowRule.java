/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 The Eigenbase Project
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

package com.disruptivetech.farrago.rel;

import net.sf.farrago.query.FennelPullRel;
import org.eigenbase.rel.CalcRel;
import org.eigenbase.rel.RelNode;
import org.eigenbase.rel.WindowedAggregateRel;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlNode;
import org.eigenbase.util.Util;

import java.util.ArrayList;
import java.util.List;


/**
 * FennelWindowRule is a rule for implementing a {@link CalcRel} which
 * contains windowed aggregates via a {@link FennelWindowRel}.
 *
 * <p>There are several instances of the rule ({@link #CalcOnWinOnCalc},
 * {@link #CalcOnWin}, {@link #WinOnCalc}, {@link #Win}), which pull in any
 * {@link CalcRel} objects above or below. (It may be better to write a rule
 * which merges {@link CalcRel} and {@link WindowedAggregateRel} objects
 * together, thereby dealing with this problem at the logical level.)
 *
 * @author jhyde
 * @version $Id$
 */
public abstract class FennelWindowRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------

    /**
     * Instance of the rule which matches {@link CalcRel}
     * on top of {@link WindowedAggregateRel}
     * on top of {@link CalcRel}.
     */
    public static final FennelWindowRule CalcOnWinOnCalc =
        new FennelWindowRule(
            new RelOptRuleOperand(
                CalcRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        WindowedAggregateRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(
                                CalcRel.class,
                                new RelOptRuleOperand [] {
                                    new RelOptRuleOperand(
                                        RelNode.class,
                                        null)})
                        })
                }))
        {
            // implement RelOptRule
            public void onMatch(RelOptRuleCall call)
            {
                final CalcRel outCalc = (CalcRel) call.rels[0];
                final WindowedAggregateRel winAgg =
                    (WindowedAggregateRel) call.rels[1];
                final CalcRel inCalc = (CalcRel) call.rels[2];
                final RelNode child = call.rels[3];
                if (inCalc.conditionExpr != null) {
                    // FennelWindowRel cannot filter its input. Leave it to
                    // the Calc-on-Win rule.
                    return;
                }
                doThing(call, outCalc, winAgg, inCalc, child);
            }
        };

    /**
     * Instance of the rule which matches a {@link CalcRel}
     * on top of a {@link WindowedAggregateRel}.
     */
    public static final FennelWindowRule CalcOnWin =
        new FennelWindowRule(
            new RelOptRuleOperand(
                CalcRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        WindowedAggregateRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(
                                RelNode.class,
                                null)})
                }))
        {
            // implement RelOptRule
            public void onMatch(RelOptRuleCall call)
            {
                if (call.rels[2] instanceof CalcRel) {
                    // The Calc-on-Win-on-Calc rule will have dealt with this.
                    return;
                }
                final CalcRel outCalc = (CalcRel) call.rels[0];
                if (RexOver.containsOver(
                    outCalc.projectExprs, outCalc.conditionExpr)) {
                    return;
                }
                final WindowedAggregateRel winAggRel =
                    (WindowedAggregateRel) call.rels[1];
                final RelNode child = call.rels[2];
                if (child instanceof CalcRel) {
                    CalcRel calcRel = (CalcRel) child;
                    if (calcRel.conditionExpr == null) {
                        // The Calc-on-Win-on-Calc rule will deal with this.
                        return;
                    }
                }
                doThing(call, outCalc, winAggRel, null, child);
            }
        };

    /**
     * Instance of the rule which matches a {@link WindowedAggregateRel}
     * on top of a {@link CalcRel}.
     */
    public static final FennelWindowRule WinOnCalc =
        new FennelWindowRule(
            new RelOptRuleOperand(
                WindowedAggregateRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        CalcRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(
                                RelNode.class,
                                null)})
                        }))
        {
            // implement RelOptRule
            public void onMatch(RelOptRuleCall call)
            {
                final WindowedAggregateRel winAgg =
                    (WindowedAggregateRel) call.rels[0];
                final CalcRel inCalc = (CalcRel) call.rels[1];
                final RelNode child = call.rels[2];
                doThing(call, null, winAgg, inCalc, child);
            }
        };

    /**
     * Instance of the rule which matches a {@link WindowedAggregateRel}
     * on top of a {@link RelNode}.
     */
    public static final FennelWindowRule Win =
        new FennelWindowRule(
            new RelOptRuleOperand(
                WindowedAggregateRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        RelNode.class,
                        null)}))
        {
            // implement RelOptRule
            public void onMatch(RelOptRuleCall call)
            {
                final WindowedAggregateRel winAgg =
                    (WindowedAggregateRel) call.rels[0];
                final RelNode child = call.rels[1];
                if (child instanceof CalcRel) {
                    // The Win-Calc rule will deal with this.
                    return;
                }
                doThing(call, null, winAgg, null, child);
            }
        };

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a rule.
     */
    private FennelWindowRule(RelOptRuleOperand operand)
    {
        super(operand);
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelPullRel.FENNEL_PULL_CONVENTION;
    }

    protected void doThing(
        RelOptRuleCall call,
        CalcRel outCalc,
        WindowedAggregateRel winAggRel,
        CalcRel inCalc,
        RelNode child)
    {
        Util.pre(winAggRel != null, "winAggRel != null");
        final RelTraitSet traits = inCalc != null ? inCalc.getTraits()
            : winAggRel.getTraits();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                traits, FennelPullRel.FENNEL_PULL_CONVENTION,
                child);
        if (fennelInput == null) {
            return;
        }

        RexNode[] inputExprs;
        if (inCalc == null) {
            inputExprs = RexUtil.toInputRefs(child.getRowType());
        } else {
            inputExprs = inCalc.projectExprs;
        }

        RexNode[] outputExprs;
        RexNode conditionExpr;
        final RelDataType outRowType;
        if (outCalc == null) {
            outputExprs = RexUtil.toInputRefs(winAggRel.rowType);
            conditionExpr = null;
            outRowType = winAggRel.getRowType();
        } else {
            outputExprs = outCalc.projectExprs;
            conditionExpr = outCalc.conditionExpr;
            outRowType = outCalc.getRowType();
        }

        final RexBuilder rexBuilder = winAggRel.getCluster().rexBuilder;
        List windowList = new ArrayList();
        for (int i = 0; i < winAggRel.aggs.length; i++) {
            RexNode agg = winAggRel.aggs[i];
            if (agg instanceof RexOver) {
                addWindows(rexBuilder, windowList, (RexOver) agg);
            }
        }
        final FennelWindowRel.Window[] windows = (FennelWindowRel.Window[])
            windowList.toArray(new FennelWindowRel.Window[windowList.size()]);
        // Now the windows are complete, compute their digests.
        for (int i = 0; i < windows.length; i++) {
            windows[i].computeDigest();
        }
        FennelWindowRel fennelCalcRel =
            new FennelWindowRel(
                winAggRel.getCluster(),
                fennelInput,
                outRowType,
                inputExprs,
                windows,
                outputExprs,
                conditionExpr);
        call.transformTo(fennelCalcRel);
    }

    private void addWindows(
        RexBuilder rexBuilder,
        List windowList,
        RexOver over)
    {
        final RexOver.RexWindow aggWindow = over.window;
        // Look up or create a window.
        Integer[] orderKeys = RexUtil.toOrdinalArray(aggWindow.orderKeys);
        FennelWindowRel.Window fennelWindow = lookupWindow(
            windowList, aggWindow.physical, aggWindow.getLowerBound(),
            aggWindow.getUpperBound(), orderKeys);

        // Lookup or create a partition within the window.
        Integer[] partitionKeys = RexUtil.toOrdinalArray(aggWindow.partitionKeys);
        FennelWindowRel.Partition fennelPartition =
            fennelWindow.lookupOrCreatePartition(partitionKeys);
        Util.discard(fennelPartition);

        // Create a clone the 'over' expression, omitting the window (which is
        // already part of the partition spec), and add the clone to the
        // partition.
        fennelPartition.addOver(
            over.getType(), over.getAggOperator(), over.getOperands());
    }

    private FennelWindowRel.Window lookupWindow(
        List windowList,
        boolean physical,
        SqlNode lowerBound,
        SqlNode upperBound,
        Integer[] orderKeys)
    {
        for (int i = 0; i < windowList.size(); i++) {
            FennelWindowRel.Window window =
                (FennelWindowRel.Window) windowList.get(i);
            if (physical == window.physical &&
                Util.equal(lowerBound, window.lowerBound) &&
                Util.equal(upperBound, window.upperBound) &&
                Util.equal(orderKeys, window.orderKeys)) {
                return window;
            }
        }
        final FennelWindowRel.Window window = new FennelWindowRel.Window(
            physical, lowerBound, upperBound, orderKeys);
        windowList.add(window);
        return window;
    }

}

// End FennelWindowRule.java

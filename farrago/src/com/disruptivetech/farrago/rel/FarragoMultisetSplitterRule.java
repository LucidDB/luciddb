package com.disruptivetech.farrago.rel;

import org.eigenbase.relopt.*;
import org.eigenbase.rel.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.util.Util;

import java.util.Set;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.HashSet;

/**
 * FarragoMultisetSplitterRule works in three ways.
 * <ol>
 * <li>It works on relational expressions consisting
 * of a mix between multiset and non-multisets calls, splitting them up. Example being
 * <code>CARDINALITY(ms)<>632</code></li>
 * <li>It works on relational expressions consisting
 * of nested multiset calls, splitting them up. Example being
 * <code>CARDINALITY(ms1 MULTISET UNION ms2)</code></li>
 * <li>It also transforms a multiset call to an equivalent RelNode tree.</li>
 * <p>
 * It uses the {@link CalcRelSplitter} for it's splitting.<br>
 * Example:<br>
 * The expression <code>CARDINALITY(ms)=5</code><br>
 * begins its life as: <code>CalcRel=[=(CARD(ms),5)]</code><br>
 * After the split it, looks in principle like: <br>
 * <code>
 * CalcRel=[=($in_ms,5]<br>
 * &nbsp;&nbsp;CalcRel=[CARD(ms)]
 * </code>
 * see {@link CalcRelSplitter} on details of the split.
 * <p>
 * CalcRel=[CARD(ms)] is intercepted in this very same rule
 * and an equivalent RelNode tree is injected in its place.
 *
 * @author Wael Chatila
 * @since Mar 10, 2005
 * @version $Id$
 */
public class FarragoMultisetSplitterRule extends RelOptRule
{
    //~ Static fields/initializers --------------------------------------------
    private static final CalcRelSplitter.RelType REL_TYPE_MULTISET =
        new CalcRelSplitter.RelType("REL_TYPE_MULTISET");

    private static final CalcRelSplitter.RelType REL_TYPE_NOT_MULTISET =
        new CalcRelSplitter.RelType("REL_TYPE_NOT_MULTISET");

    private static final CalcRelSplitter.RelType REL_TYPE_NESTED =
        new CalcRelSplitter.RelType("REL_TYPE_NESTED");

    private static final CalcRelSplitter.RelType REL_TYPE_NOT_NESTED =
        new CalcRelSplitter.RelType("REL_TYPE_NOT_NESTED");

    /** A set defining all implementable multiset calls */
    private static final Set multisetOperators = new java.util.HashSet();
    private static final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

    static {
        final SqlStdOperatorTable opTab = SqlStdOperatorTable.instance();

        multisetOperators.add(opTab.cardinalityFunc);
    }

    //~ Constructors ----------------------------------------------------------

    /**
     * Creates a new FarragoMultisetSplitter object.
     */
    public FarragoMultisetSplitterRule()
    {
        super(new RelOptRuleOperand(
            CalcRel.class,
            new RelOptRuleOperand [] {
                new RelOptRuleOperand(RelNode.class, null)
            }));
    }

    //~ Methods ---------------------------------------------------------------

    public void onMatch(RelOptRuleCall call)
    {
        CalcRel calc = (CalcRel) ((CalcRel) call.rels[0]).clone();

        boolean doSplit = false;
        // Check if we need multiset/non-multiset splitting ...
        for (int i = 0; i < calc.projectExprs.length; i++) {
            if (containsMixing(calc.projectExprs[i])) {
                doSplit = true;
                break;
            }
        }
        if (calc.conditionExpr != null) {
            if (containsMixing(calc.conditionExpr)) {
                doSplit = true;
            }
        }

        if (doSplit) {
            // Call contains mixing between multisets and non-multisets calls
            // we need to split them apart
            CalcRelSplitter transform = new MultisetRelSplitter(calc);
            RelNode newRel = transform.execute();
            call.transformTo(newRel);
            return;
        }

        // Check if we need nested/non-nested splitting ...
        for (int i = 0; i < calc.projectExprs.length; i++) {
            if (containsNestedMultiset(calc.projectExprs[i], true)) {
                doSplit = true;
                break;
            }
        }
        if (calc.conditionExpr != null) {
            if (containsNestedMultiset(calc.conditionExpr, true)) {
                doSplit = true;
            }
        }

        if (doSplit) {
            // Call contains nested multisets
            // we need to split them apart
            CalcRelSplitter transform = new NestedRelSplitter(calc);
            RelNode newRel = transform.execute();
            call.transformTo(newRel);
            return;
        }

        // We have no mixing between multisets and non-multisets and we have
        // no nested multisets e.g. CARDINALITY(MS1 FUSION MS2)

        for (int i = 0; i < calc.projectExprs.length; i++) {
            if (containsMultiset(calc.projectExprs[i], false)) {
                assert(calc.projectExprs[i] instanceof RexCall);
                insertRels(call, calc, new Integer(i));
                call.transformTo(calc);
                return;
            }
        }
        if ((calc.conditionExpr != null)
            && containsMultiset(calc.conditionExpr, false)) {
            assert(calc.conditionExpr instanceof RexCall);
            // todo do rewrite here
            return;
        }

        // If we come here, we have all non-mulisets so we are
        // (funny to say) all set, nothing to do.
        // Let other rules take handle things from here on.
        return;
    }

    /**
     * Injects an equivalent RelNode tree to a RexCall's multiset operator
     * @param call
     * @param calc The CalcRel that the multiset call belongs to
     * @param offset This parameter is used to identify which RexCall to transform.
     *               A <code>null</code> value identifies <code>CalcRel.conditionExpr</code>
     *               as the RexCall.<br>
     *               If not null, it's identifing CalcRel.projectExprs[offset.intValue()]</code> as the RexCall.
     */
    private void insertRels(RelOptRuleCall call, CalcRel calc, final Integer offset)
    {
        RelNode input = calc.child;
        // newInputRel is the new input rel that will replace the child of the
        // current calc.child
        RelNode newInputRel = null;
        RexCall rexCall;
        if (null != offset) {
            rexCall = (RexCall) calc.projectExprs[offset.intValue()];
        } else {
            rexCall = (RexCall) calc.conditionExpr;
        }
        final RelOptCluster cluster = calc.getCluster();
        if (opTab.cardinalityFunc == rexCall.op) {
            // A call to
            // CalcRel=[...,CARDINALITY($in_i),...]
            //   CalcInput
            //
            //is eq. to
            //
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     AggregateRel=[count]
            //       Uncollect
            //          ProjectRel=[output=$cor0]
            //            OneRowRel
            final String dyn_inIdStr = cluster.query.createCorrelUnresolved("wael deffered");
            final int dyn_inId = cluster.query.getCorrelOrdinal(dyn_inIdStr);
            assert(rexCall.operands[0] instanceof RexInputRef);
            final RexInputRef rexInput = (RexInputRef) rexCall.operands[0];

            final RexNode corRef = cluster.rexBuilder.makeCorrel(rexInput.getType(), dyn_inIdStr);
            ProjectRel projectRel = new ProjectRel(
                cluster,
                new OneRowRel(cluster),
                new RexNode[]{corRef},
                new String[]{"output"},
                ProjectRel.Flags.Boxed);

            UncollectRel uncollect = new UncollectRel(cluster, projectRel);

            // TODO wael 3/15/05: need to create proper count agg call def.
            AggregateRel aggregateRel = new AggregateRel(cluster, uncollect, 1, new AggregateRel.Call[]{});

            ArrayList correlations = new ArrayList();
            correlations.add(new CorrelatorRel.Correlation(dyn_inId,rexInput.index));
            final CorrelatorRel correlatorRel = new CorrelatorRel(cluster, input, aggregateRel, correlations);

            newInputRel = correlatorRel;
            if (null == offset) {
                calc = new CalcRel(
                    cluster,
                    calc.getTraits(),
                    calc.child,
                    calc.getRowType(),
                    calc.projectExprs,
                    cluster.rexBuilder.makeInputRef(
                        calc.conditionExpr.getType(),
                        call.rels[1].getRowType().getFields().length+1));
            } else {
                calc.projectExprs[offset.intValue()] =
                    cluster.rexBuilder.makeInputRef(
                        calc.projectExprs[offset.intValue()].getType(),
                        call.rels[1].getRowType().getFields().length+1);
            }
            HashSet stoppedVariableSet = new HashSet();
            stoppedVariableSet.add(corRef);
            correlatorRel.setVariablesStopped(stoppedVariableSet);
//            correlatorRel.registerCorrelVariable(dyn_inIdStr);
        }

        Util.permAssert(
            null != newInputRel,
            rexCall.op.name + " is not defined to be implemenatble");

        calc.replaceInput(0,newInputRel);
        call.transformTo(calc);
        return;
    }

    /**
     * Returns true if a node contains a mixing between multiset and
     * non-multiset calls.
     */
    public static boolean containsMixing(RexNode node)
    {
        RexCallOperatorCounter countShuttle = new RexCallOperatorCounter();
        countShuttle.visit(node);
        if (countShuttle.totalCount == countShuttle.multisetCount) {
            return false;
        }

        if (0 == countShuttle.multisetCount) {
            return false;
        }

        return true;
    }

    /**
     * Returns true if node contains a multiset operator, otherwise false
     * Use it with deep=false when checking if a RexCall is a multiset call.
     */
    public static boolean containsMultiset(final RexNode node, boolean deep)
    {
        return null != findFirstMultiset(node, deep);
    }

    /**
     * Convenicence methods equivalent to {@link #containsMultiset} with deep=false
     */
    public static boolean isMultiset(final RexNode node)
    {
        return containsMultiset(node, false);
    }

    /**
     * Returns a reference to the first found multiset call.
     */
    private static RexCall findFirstMultiset(final RexNode node, boolean deep)
    {
        if (!(node instanceof RexCall)) {
            return null;
        }
        final RexCall call = (RexCall) node;
        assert(null != call);
        RexCall firstOne = null;
        Iterator it = multisetOperators.iterator();
        while (it.hasNext()) {
            SqlOperator op = (SqlOperator) it.next();
            firstOne = RexUtil.findOperatorCall(op, call);
            if (null != firstOne) {
                break;
            }
        }

        if (!deep && (firstOne != call)) {
            return null;
        }
        return firstOne;
    }

    /**
     * Returns true if node contains a multiset call directly under another
     * multiset call.<br>
     */
    private boolean containsNestedMultiset(RexNode node, boolean deep)
    {
        RexCall multisetCall = findFirstMultiset(node,deep);
        if (null == multisetCall) {
            return false;
        }
        boolean ret = true;
        for (int i = 0; i < multisetCall.operands.length; i++) {
            if (multisetCall.operands[i] instanceof RexCall &&
                containsMultiset(multisetCall.operands[i], false)) {

                ret = false;
                break;
            }
        }
        return !ret;
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * A RexShuttle that traverse all RexNode and counts total number of RexCalls
     * traversed and number of multiset calls traversed.<p>
     * totalCount >= multisetCount always holds true.
     */
    private static class RexCallOperatorCounter extends RexShuttle {
        public int totalCount = 0;
        public int multisetCount = 0;

        public RexNode visit(RexCall call)
        {
            totalCount++;
            if (multisetOperators.contains(call.op)) {
                multisetCount++;
            }
            return super.visit(call);
        }
    };

    /**
     * Unmix mixing between multisets and non-multisets
     */
    private class MultisetRelSplitter
        extends CalcRelSplitter
    {

        private MultisetRelSplitter(CalcRel calc)
        {
            super(calc, REL_TYPE_MULTISET, REL_TYPE_NOT_MULTISET);
        }

        protected boolean canImplementAs(
            RexCall call, CalcRelSplitter.RelType relType)
        {
            if (relType == REL_TYPE_NOT_MULTISET) {
                return !isMultiset(call);
            } else if (relType == REL_TYPE_MULTISET) {
                return isMultiset(call);
            } else {
                assert(false): "Unknown rel type: " + relType;
                return false;
            }
        }

        protected boolean canImplementAs(RexDynamicParam param, RelType relType)
        {
            // Dynamic param rex nodes are not multisets
            return relType == REL_TYPE_NOT_MULTISET;
        }

        protected boolean canImplementAs(RexFieldAccess field, RelType relType)
        {
            // Field access rex nodes are not multisets
            return relType == REL_TYPE_NOT_MULTISET;
        }
    }

    /**
     * Unnest nested multiset calls
     */
    private class NestedRelSplitter
        extends CalcRelSplitter
    {

        private NestedRelSplitter(CalcRel calc)
        {
            super(calc, REL_TYPE_NESTED, REL_TYPE_NOT_NESTED);
        }

        protected boolean canImplementAs(
            RexCall call, CalcRelSplitter.RelType relType)
        {
            if (relType == REL_TYPE_NOT_NESTED) {
                return !containsNestedMultiset(call, false);
            } else if (relType == REL_TYPE_NESTED) {
                return containsNestedMultiset(call, false);
            } else {
                assert(false): "Unknown rel type: " + relType;
                return false;
            }
        }

        protected boolean canImplementAs(RexDynamicParam param, RelType relType)
        {
            // Dynamic param rex nodes are not multisets
            return relType == REL_TYPE_NOT_MULTISET;
        }

        protected boolean canImplementAs(RexFieldAccess field, RelType relType)
        {
            // Field access rex nodes are not multisets
            return relType == REL_TYPE_NOT_MULTISET;
        }
    }
}

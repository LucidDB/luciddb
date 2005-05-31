/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 Disruptive Tech
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.rex.*;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.SqlStateCodes;
import org.eigenbase.sql.fun.SqlStdOperatorTable;
import org.eigenbase.sql.type.SqlTypeName;
import org.eigenbase.util.Util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Collections;

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
            if (RexMultisetUtil.containsMixing(calc.projectExprs[i])) {
                doSplit = true;
                break;
            }
        }
        if (calc.conditionExpr != null) {
            if (RexMultisetUtil.containsMixing(calc.conditionExpr)) {
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
            final RexNode expr = calc.projectExprs[i];
            if (RexMultisetUtil.containsMultiset(expr, false)) {
                assert expr instanceof RexCall ||
                    expr instanceof RexFieldAccess;
                insertRels(call, calc, new Integer(i));
                call.transformTo(calc);
                return;
            }
        }
        if ((calc.conditionExpr != null)
            && RexMultisetUtil.containsMultiset(calc.conditionExpr, false)) {
            assert calc.conditionExpr instanceof RexCall ||
                calc.conditionExpr instanceof RexFieldAccess;
            insertRels(call, calc, null);
            call.transformTo(calc);
            return;
        }

        // If we come here, we have all non-mulisets so we are
        // (funny to say) all set, nothing to do.
        // Let other rules handle things from here on.
        assert !RexMultisetUtil.containsMultiset(
            calc.projectExprs, calc.conditionExpr);
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
        // correlatorRel is the new input rel that will replace the child of the
        // current calc.child
        RelNode convertedRel = null;

        final RexCall rexCall;
        final RexNode rexNode;
        if (null != offset) {
            rexNode = calc.projectExprs[offset.intValue()];
        } else {
            rexNode = calc.conditionExpr;
        }

        if (rexNode instanceof RexFieldAccess) {
            rexCall = (RexCall) ((RexFieldAccess) rexNode).expr;
        } else {
            rexCall = (RexCall) rexNode;
        }

        ArrayList correlations = new ArrayList();
        final RelOptCluster cluster = calc.getCluster();
        final RexNode corRef;
        if ((RexMultisetUtil.opTab.memberOfOperator == rexCall.op) &&
            !(rexCall.operands[0] instanceof RexInputRef)) {
            corRef = rexCall.operands[0];
        } else {
            final String dyn_inIdStr = cluster.query.createCorrel();
            final int dyn_inId = RelOptQuery.getCorrelOrdinal(dyn_inIdStr);
            assert(rexCall.operands[0] instanceof RexInputRef);
            final RexInputRef rexInput = (RexInputRef) rexCall.operands[0];
            correlations.add(
                new CorrelatorRel.Correlation(dyn_inId,rexInput.index));
            corRef  =
                cluster.rexBuilder.makeCorrel(rexInput.getType(), dyn_inIdStr);
        }
        ProjectRel corProjectRel = new ProjectRel(
            cluster,
            new OneRowRel(cluster),
            new RexNode[]{corRef},
            new String[]{"output"+corRef.toString()},
            ProjectRel.Flags.Boxed);

        if (RexMultisetUtil.opTab.cardinalityFunc == rexCall.op) {
            // A call to
            // CalcRel=[...,CARDINALITY($in_i),...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     AggregateRel=[count]
            //       Uncollect
            //          ProjectRel=[output=$cor0]
            //            OneRowRel
            UncollectRel uncollect = new UncollectRel(cluster, corProjectRel);
            // TODO wael 3/15/05: need to create proper count agg call def.
            AggregateRel aggregateRel =
                new AggregateRel(
                    cluster, uncollect, 1, new AggregateRel.Call[]{});
            convertedRel =
                new CorrelatorRel(cluster, input, aggregateRel, correlations);
//            HashSet stoppedVariableSet = new HashSet();
//            stoppedVariableSet.add(corRef);
//            convertedRel.setVariablesStopped(stoppedVariableSet);
//            correlatorRel.registerCorrelVariable(dyn_inIdStr);
        } else if (RexMultisetUtil.opTab.castFunc == rexCall.op) {
            // A call to
            // CalcRel=[...,CAST($in_i AS XYZ MULTISET),...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     Collect
            //       ProjectRel=[CAST($in) AS XYZ]
            //         Uncollect
            //           ProjectRel=[output=$cor0]
            //             OneRowRel
            UncollectRel uncollect = new UncollectRel(cluster, corProjectRel);
            RelDataType type = rexCall.getType().getComponentType();
            assert(null != type);
            assert(type.isStruct());
            type = type.getFields()[0].getType();
            RexNode newCastCall =
                cluster.rexBuilder.makeCast(
                    type,
                    cluster.rexBuilder.makeInputRef(
                        uncollect.getRowType().getFields()[0].getType(), 0));
            ProjectRel castRel = new ProjectRel(
                cluster,
                uncollect,
                new RexNode[]{newCastCall},
                new String[]{uncollect.getRowType().getFields()[0].getName()},
                ProjectRel.Flags.Boxed);
            convertedRel =
                new CorrelatorRel(cluster, input, castRel, correlations);
        } else if (RexMultisetUtil.opTab.isASetOperator == rexCall.op) {
            // (ms IS A SET) <=>
            // UNIQUE(UNNEST(ms)) <=>
            // NOT EXITS ((select <ms element>, count(*) as c from UNNEST(ms) GROUP BY <ms element>) WHERE c > 1)
            //
            // A call to
            // CalcRel=[...,$in_i IS A SET,...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     ProjectRel=[CASE $0 == 0 THEN true ELSE false END]
            //       AggregateRel[count(*)]
            //         FilterRel=[c > 1]
            //           AggregateRel[count(*) as c group by $0]
            //             Uncollect
            //               ProjectRel=[output=$cor0]
            //                 OneRowRel
            UncollectRel uncollect = new UncollectRel(cluster, corProjectRel);
            // TODO wael 4/5/05: need to create proper count & group by agg call def.
            AggregateRel aggregateGroupRel =
                new AggregateRel(
                    cluster, uncollect, 1, new AggregateRel.Call[]{});
            RexNode c = cluster.rexBuilder.makeInputRef(
                cluster.typeFactory.createSqlType(
                    SqlTypeName.Integer), 0);
            RelNode filterRel =
                RelOptUtil.createExistsPlan(
                    cluster,
                    aggregateGroupRel,
                    new RexNode[]{ cluster.rexBuilder.makeCall(
                        RexMultisetUtil.opTab.greaterThanOperator,
                        c,
                        cluster.rexBuilder.makeExactLiteral(new BigDecimal(1)))},
                    null,
                    null);
            RelNode notExistRel = createExistsPlanSingleRow(filterRel, true);
            convertedRel =
                new CorrelatorRel(cluster, input, notExistRel, correlations);
        } else if (RexMultisetUtil.opTab.elementFunc == rexCall.op) {
            // A call to
            // CalcRel=[...,ELEMENT($in_i),...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     LimitRel(max=1)
            //       Uncollect
            //         ProjectRel=[output=$cor0]
            //           OneRowRel
            //
            // LimitRel (which doesnt exists) is further expanded to
            //
            // ProjectRel($0)
            //   FilterRel[condition=CASE WHEN $0==1 THEN true ELSE throw("21000") END]
            //      AggregateRel[count(*), $0]
            //
            // 21000 is the standard CARDINALITY VIOLATION error code
            UncollectRel uncollect = new UncollectRel(cluster, corProjectRel);
            // TODO wael 3/29/05: need to create proper count agg call def.
            AggregateRel aggregateRel =
                new AggregateRel(
                    cluster, uncollect, 1, new AggregateRel.Call[]{});
            RexNode[] whenThenElse = new RexNode[] {
                // when
                cluster.rexBuilder.makeCall(RexMultisetUtil.opTab.equalsOperator
                    ,cluster.rexBuilder.makeInputRef(
                        cluster.typeFactory.createSqlType(
                            SqlTypeName.Integer), 0)
                    ,cluster.rexBuilder.makeExactLiteral(new BigDecimal(1)))
                // then
                ,cluster.rexBuilder.makeLiteral(true)
                // else
                ,cluster.rexBuilder.makeCall(RexMultisetUtil.opTab.throwOperator,
                    cluster.rexBuilder.makeLiteral(
                        SqlStateCodes.CardinalityViolation.getState()))};
            RexNode condition =
                cluster.rexBuilder.makeCall(RexMultisetUtil.opTab.caseOperator, whenThenElse);
            FilterRel filterRel =
                new FilterRel(cluster, aggregateRel, condition);
            ProjectRel limitRel =
                new ProjectRel(
                    cluster,
                    filterRel,
                    new RexNode[]{
                        cluster.rexBuilder.makeRangeReference(
                            uncollect.getRowType(), 1)},
                    new String[]{"rangeref"},
                    ProjectRel.Flags.Boxed);
            convertedRel =
                new CorrelatorRel(cluster, input, limitRel, correlations);
        } else if (rexCall.op instanceof
            SqlStdOperatorTable.SqlMultisetSetOperator  ||
            (RexMultisetUtil.opTab.memberOfOperator == rexCall.op)) {
            // A call to
            // CalcRel=[...,ms1 UNION ms2,...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$ms1, $cor1=$ms2]
            //     CalcInput
            //     CollectRel
            //       UnionRel
            //         Uncollect
            //           ProjectRel=[output=$cor0]
            //            OneRowRel
            //         Uncollect
            //           ProjectRel=[output=$cor1]
            //            OneRowRel
            // %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
            // (x MEMBER OF ms) <=>
            // (x IN UNNEST(ms)) <=>
            // (EXISTS (select <multiset element> from UNNEST(ms) WHERE <multiset element>=x))
            //
            // A call to
            // CalcRel=[...,$in_i MEMBER OF $in_j,...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i, $cor1=in_j]
            //     CalcInput
            //     ProjectRel=[CASE $0 > 0 THEN true ELSE false END]
            //       AggregateRel[count(*)]
            //         FilterRel=[$cor0 = $0]
            //           Uncollect
            //             ProjectRel=[output=$cor1]
            //                 OneRowRel
            final String dyn_inIdStr2 = cluster.query.createCorrel();
            final int dyn_inId2 = RelOptQuery.getCorrelOrdinal(dyn_inIdStr2);
            assert(rexCall.operands[1] instanceof RexInputRef);
            final RexInputRef rexInput2 = (RexInputRef) rexCall.operands[1];
            correlations.add(
                new CorrelatorRel.Correlation(dyn_inId2,rexInput2.index));
            final RexNode corRef2 =
                cluster.rexBuilder.makeCorrel(rexInput2.getType(), dyn_inIdStr2);
            ProjectRel projectRel2 = new ProjectRel(
                cluster,
                new OneRowRel(cluster),
                new RexNode[]{corRef2},
                new String[]{"output"+corRef2.toString()},
                ProjectRel.Flags.Boxed);

            ///////////////////////////////////
            if (rexCall.op instanceof
                SqlStdOperatorTable.SqlMultisetSetOperator) {
                final UncollectRel uncollectRel =
                    new UncollectRel(cluster, corProjectRel);
                final UncollectRel uncollectRel2 =
                    new UncollectRel(cluster, projectRel2);
                RelNode[] inputs = new RelNode[]{ uncollectRel, uncollectRel2};
                final RelNode setRel;
                if (RexMultisetUtil.opTab.multisetExceptAllOperator == rexCall.op) {
                    setRel =
                        new MinusRel(cluster, inputs, true);
                } else if (RexMultisetUtil.opTab.multisetExceptOperator == rexCall.op) {
                    setRel =
                        new MinusRel(cluster, inputs, false);
                } else if (RexMultisetUtil.opTab.multisetIntersectAllOperator == rexCall.op) {
                    setRel =
                        new IntersectRel(cluster, inputs, true);
                } else if (RexMultisetUtil.opTab.multisetIntersectOperator == rexCall.op) {
                    setRel =
                        new IntersectRel(cluster, inputs, false);
                } else if (RexMultisetUtil.opTab.multisetUnionAllOperator == rexCall.op) {
                    setRel =
                        new UnionRel(cluster, inputs, true);
                } else if (RexMultisetUtil.opTab.multisetUnionOperator == rexCall.op) {
                    setRel =
                        new UnionRel(cluster, inputs, false);
                } else {
                    throw Util.newInternal("should never come here");
                }

                final CollectRel collectRel =
                    new CollectRel(cluster, setRel, "multiset");
                convertedRel =
                    new CorrelatorRel(cluster, input, collectRel, correlations);
            } else if (RexMultisetUtil.opTab.memberOfOperator == rexCall.op) {
                final UncollectRel uncollectRel =
                    new UncollectRel(cluster, projectRel2);
                RelDataType elementType = rexInput2.getType().getComponentType();
                assert(null != elementType);
                RelNode filterRel =
                    RelOptUtil.createExistsPlan(
                        cluster,
                        uncollectRel,
                        new RexNode[]{ cluster.rexBuilder.makeCall(
                            RexMultisetUtil.opTab.equalsOperator,
                            corRef,
                            cluster.rexBuilder.makeInputRef(elementType, 0))},
                        null,
                        null);
                RelNode existsRel = createExistsPlanSingleRow(filterRel, false);
                convertedRel =
                    new CorrelatorRel(cluster, input, existsRel, correlations);
            }
        } else if (RexMultisetUtil.opTab.submultisetOfOperator == rexCall.op) {
            // (ms1 SUBMULTISET OF ms2 ) <=>
            // [ u1 := SELECT*FROM UNNEST(ms1) as u1 ]
            // [ u2 := SELECT*FROM UNNEST(ms2) as u2 ]
            // <==>
            // NOT EXISTS(SELECT c1, c2 FROM
            //    (SELECT *, COUNT(*) as c1 FROM u1 GROUP BY ms1.$0)
            //    LEFT OUTER JOIN
            //    (SELECT *, COUNT(*) as c2 FROM u2 GROUP BY ms2.$0)
            //    ON (u1.* IS NOT DISTINCT FROM u2.*)
            //    WHERE (c1>c2) OR (c2 IS NULL)
            // )
            //
            // A call to
            // CalcRel=[...,$in_i SUBMULTISET $in_j,...]
            //   CalcInput
            //is eq. to
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i, $cor1=$in_j]
            //     CalcInput
            //     NotExistsRel
            //       FilterRel=[c1>c2 OR c2 IS NULL]
            //         JoinRel=[left outer join on $0.* IS NOT DISTINCT FROM $1.*]
            //           AggregateRel[$0, count(*) as c1 group by $0]
            //             Uncollect
            //               ProjectRel=[output=$cor0]
            //                 OneRowRel
            //           AggregateRel[$0, count(*) as c2 group by $0]
            //             Uncollect
            //               ProjectRel=[output=$cor1]
            //                 OneRowRel
            final String dyn_inIdStr2 = cluster.query.createCorrel();
            final int dyn_inId2 = RelOptQuery.getCorrelOrdinal(dyn_inIdStr2);
            assert(rexCall.operands[1] instanceof RexInputRef);
            final RexInputRef rexInput2 = (RexInputRef) rexCall.operands[1];
            correlations.add(
                new CorrelatorRel.Correlation(dyn_inId2,rexInput2.index));
            final RexNode corRef2 =
                cluster.rexBuilder.makeCorrel(rexInput2.getType(), dyn_inIdStr2);
            ProjectRel projectRel2 = new ProjectRel(
                cluster,
                new OneRowRel(cluster),
                new RexNode[]{corRef2},
                new String[]{"output"+corRef2.toString()},
                ProjectRel.Flags.Boxed);

            final UncollectRel u1 =
                new UncollectRel(cluster, corProjectRel);
            final UncollectRel u2 =
                new UncollectRel(cluster, projectRel2);
            // TODO wael 4/26/05: need to create proper count & group by agg call def.
            AggregateRel aggregateGroupRel1 =
                new AggregateRel(
                    cluster, u1, 1, new AggregateRel.Call[]{});
            AggregateRel aggregateGroupRel2 =
                new AggregateRel(
                    cluster, u2, 1, new AggregateRel.Call[]{});
            JoinRel joinRel =
                new JoinRel(
                    cluster,
                    aggregateGroupRel1,
                    aggregateGroupRel2,
                    RelOptUtil.isDistinctFrom(
                        cluster.rexBuilder,
                        cluster.rexBuilder.makeRangeReference(u1.getRowType()), //todo get the right input ref from agg
                        cluster.rexBuilder.makeRangeReference(u2.getRowType()), //todo get the right input ref from agg
                        true),
                    JoinRel.JoinType.LEFT,
                    Collections.EMPTY_SET);
            RexNode c = cluster.rexBuilder.makeInputRef(
                cluster.typeFactory.createSqlType(
                    SqlTypeName.Integer), 0);
            RelNode filterRel =
                RelOptUtil.createExistsPlan(
                    cluster,
                    joinRel,
                    new RexNode[]{ cluster.rexBuilder.makeCall(
                        RexMultisetUtil.opTab.greaterThanOperator,
                        c,
                        cluster.rexBuilder.makeExactLiteral(new BigDecimal(1)))},
                    null,
                    null);
            RelNode notExistsRel = createExistsPlanSingleRow(filterRel, true);
            convertedRel =
                new CorrelatorRel(cluster, input, notExistsRel, correlations);
        }

        Util.permAssert(
            null != convertedRel,
            rexCall.op.name + " is not defined to be implementable");

        final RexNode newRexInputRef = cluster.rexBuilder.makeInputRef(
            rexNode.getType(),
            call.rels[1].getRowType().getFields().length+1);
        if (rexNode instanceof RexFieldAccess) {
            ((RexFieldAccess) rexNode).expr = newRexInputRef;
        } else if (null == offset) {
            calc = new CalcRel(
                cluster,
                calc.getTraits(),
                calc.child,
                calc.getRowType(),
                calc.projectExprs,
                newRexInputRef);
        } else {
            calc.projectExprs[offset.intValue()] = newRexInputRef;
        }

        calc.replaceInput(0, convertedRel);
        call.transformTo(RelOptUtil.clone(calc));
        return;
    }

    /**
     * Returns true if node contains a multiset call directly under another
     * multiset call.<br>
     */
    private boolean containsNestedMultiset(RexNode node, boolean deep)
    {
        RexCall multisetCall = RexMultisetUtil.findFirstMultiset(node,deep);
        if (null == multisetCall) {
            return false;
        }
        boolean ret = true;
        for (int i = 0; i < multisetCall.operands.length; i++) {
            if (multisetCall.operands[i] instanceof RexCall &&
                RexMultisetUtil.containsMultiset(multisetCall.operands[i], false)) {

                ret = false;
                break;
            }
        }
        return !ret;
    }

    //~ Inner Classes ---------------------------------------------------------

    ;

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
            boolean containsMultiset =
                RexMultisetUtil.containsMultiset(call, false);
            if (relType == REL_TYPE_NOT_MULTISET) {
                return !containsMultiset;
            } else if (relType == REL_TYPE_MULTISET) {
                return containsMultiset;
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

    public RelNode createExistsPlanSingleRow(RelNode child, boolean neg) {
        RelOptCluster cluster = child.getCluster();
        // TODO wael 4/5/05: need to create proper count call def.
        AggregateRel countAggregateRel =
            new AggregateRel(
                cluster, child, 1, new AggregateRel.Call[]{});
        SqlOperator op;
        if (neg) {
            op = RexMultisetUtil.opTab.equalsOperator;
        } else {
            op = RexMultisetUtil.opTab.greaterThanOperator;
        }
        RexNode[] whenThenElse = new RexNode[] {
            // when
            cluster.rexBuilder.makeCall(op
                ,cluster.rexBuilder.makeInputRef(
                    cluster.typeFactory.createSqlType(
                        SqlTypeName.Integer), 0)
                ,cluster.rexBuilder.makeExactLiteral(new BigDecimal(0)))
            // then
            ,cluster.rexBuilder.makeLiteral(true)
            // else
            ,cluster.rexBuilder.makeLiteral(false)};
        RexNode caseRexNode =
            cluster.rexBuilder.makeCall(RexMultisetUtil.opTab.caseOperator, whenThenElse);
        ProjectRel caseRel =
            new ProjectRel(
                cluster,
                countAggregateRel,
                new RexNode[]{caseRexNode},
                new String[]{"case"},
                ProjectRel.Flags.Boxed);
        return caseRel;
    }

}

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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
package net.sf.farrago.fennel.rel;

import java.math.*;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;
import org.eigenbase.sql.fun.*;
import org.eigenbase.util.*;


/**
 * FarragoMultisetSplitterRule is a planner rule which removes multiset
 * operations from an expression.
 *
 * <p>It works in three ways:
 *
 * <ol>
 * <li>It works on relational expressions consisting of a mix between multiset
 * and non-multisets calls, splitting them up. For example, <code>
 * CARDINALITY(ms) != 632</code></li>
 * <li>It works on relational expressions consisting of nested multiset calls,
 * splitting them up. For example, <code>CARDINALITY(ms1 MULTISET UNION
 * ms2)</code></li>
 * <li>It also transforms a multiset call to an equivalent RelNode tree.</li>
 * </ol>
 *
 * <p>It uses the {@link CalcRelSplitter} for its splitting. Example: The
 * expression <code>CARDINALITY(ms) = 5</code> begins its life as: <code>
 * CalcRel=[=(CARD(ms),5)]</code> After the split it, looks in principle like:
 * <code>
 * <pre>CalcRel=[=($in_ms,5]
 * &nbsp;&nbsp;CalcRel=[CARD(ms)]</pre>
 * </code> See {@link CalcRelSplitter} on details of the split.
 *
 * <p>CalcRel=[CARD(ms)] is intercepted in this very same rule and an equivalent
 * RelNode tree is injected in its place.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Mar 10, 2005
 */
public class FarragoMultisetSplitterRule
    extends RelOptRule
{
    public static final FarragoMultisetSplitterRule instance =
        new FarragoMultisetSplitterRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FarragoMultisetSplitterRule.
     */
    private FarragoMultisetSplitterRule()
    {
        super(
            new RelOptRuleOperand(
                CalcRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    private int findMultiset(List<RexNode> exprs)
    {
        int i = 0;
        for (RexNode expr : exprs) {
            if (RexMultisetUtil.containsMultiset(expr, false)) {
                return i;
            }
            ++i;
        }
        return -1; // not found
    }

    public void onMatch(RelOptRuleCall call)
    {
        final CalcRel calc = (CalcRel) call.rels[0];

        // Check if we need multiset/non-multiset splitting ...
        final RexProgram program = calc.getProgram();
        boolean doSplit = RexMultisetUtil.containsMixing(program);
        if (doSplit) {
            // Call contains mixing between multisets and non-multisets calls
            // we need to split them apart
            CalcRelSplitter transform = new MultisetRelSplitter(calc);
            RelNode newRel = transform.execute();
            call.transformTo(newRel);
            return;
        }

        // Check if we need nested/non-nested splitting ...
        doSplit = containsNestedMultiset(program);
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
        final List<RexNode> exprList = program.getExprList();
        int targetField = findMultiset(exprList);
        if (targetField < 0) {
            return;
        }

        final RexNode expr = exprList.get(targetField);
        assert (expr instanceof RexCall) || (expr instanceof RexFieldAccess);
        final CorrelatorRel correlatorRel = insertRels(calc, targetField);
        final int inputFieldCount = program.getInputRowType().getFieldCount();

        // Use a visitor to convert the old program. The "targetField"
        // field becomes a new input field, and the expressions before it
        // are shuffled down the order.
        //           inputFields             targetField
        //          /           \            |
        // Before: [i0] [i1] [i2] [a]  [b]  [c]  [d]
        //                          _________|
        //                         |
        // After:  [i0] [i1] [i2] [i3] [a]  [b]  [d]

        // By default, the replacement field is the new last field of
        // the input relational expression.
        int replacementField = inputFieldCount;
        RexNode [] newExprs = exprList.toArray(new RexNode[exprList.size()]);

        // If the expression we are transforming is a multiset of
        // non-record types, the result will be a multiset of records.
        // Add a slice function to make the types compatible.
        if ((expr.getType().getComponentType() != null)
            && !expr.getType().getComponentType().isStruct())
        {
            RexNode slicedExpr =
                calc.getCluster().getRexBuilder().makeCall(
                    SqlStdOperatorTable.sliceOp,
                    RelOptUtil.createInputRef(
                        correlatorRel,
                        replacementField));
            newExprs = exprList.toArray(new RexNode[exprList.size() + 1]);
            newExprs[newExprs.length - 1] = slicedExpr;
            replacementField = newExprs.length - 1;
        }

        // Change all references to the multiset expression to point
        // to the new expression.
        // Fields above inputFields and below target field move up
        // one.
        Permutation permutation = new Permutation(newExprs.length);
        for (int i = 0; i < inputFieldCount; i++) {
            permutation.set(i, i);
            newExprs[i] = exprList.get(i);
        }
        newExprs[inputFieldCount] =
            RelOptUtil.createInputRef(
                correlatorRel,
                inputFieldCount);
        for (int i = inputFieldCount; i < targetField; i++) {
            permutation.set(i, i + 1);
            newExprs[i + 1] = exprList.get(i);
        }
        permutation.set(targetField, replacementField);
        final RexShuttle shuttle = new RexPermutationShuttle(permutation);

        final List<RexLocalRef> projectRefList = program.getProjectList();
        RexProgramBuilder programBuilder =
            RexProgramBuilder.create(
                calc.getCluster().getRexBuilder(),
                correlatorRel.getRowType(),
                Arrays.asList(newExprs),
                projectRefList,
                program.getCondition(),
                program.getOutputRowType(),
                shuttle,
                false);

        // Eliminate unused expressions, and create a program.
        programBuilder.eliminateUnused();
        RexProgram newProgram = programBuilder.getProgram();
        CalcRel newCalc =
            new CalcRel(
                calc.getCluster(),
                calc.cloneTraits(),
                correlatorRel,
                calc.getRowType(),
                newProgram,
                newProgram.getCollations(correlatorRel.getCollationList()));
        call.transformTo(newCalc);
    }

    /**
     * Injects an equivalent {@link RelNode} tree to a {@link RexCall}'s
     * multiset operator.
     *
     * <p>If the expression is a multiset of scslar values (say an INTEGER
     * MULTISET) then the relation's type will be a row with a single field, and
     * that field will need to be dereferenced.
     *
     * @param calc The {@link CalcRel} that the multiset call belongs to
     * @param offset Ordinal of the expression to transform.
     */
    private CorrelatorRel insertRels(CalcRel calc, int offset)
    {
        RelNode input = calc.getChild();

        final RexCall rexCall;
        final RexNode rexNode = calc.getProgram().getExprList().get(offset);

        if (rexNode instanceof RexFieldAccess) {
            rexCall = (RexCall) ((RexFieldAccess) rexNode).getReferenceExpr();
        } else {
            rexCall = (RexCall) rexNode;
        }

        final RelOptCluster cluster = calc.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();
        final SqlOperator op = rexCall.getOperator();
        final RelDataType componentType =
            rexCall.operands[0].getType().getComponentType();

        if (SqlStdOperatorTable.cardinalityFunc == op) {
            // A call to
            //
            // CalcRel=[...,CARDINALITY($in_i),...]
            //   CalcInput
            //
            // is eq. to
            //
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     AggregateRel=[count]
            //       Uncollect
            //          ProjectRel=[output=$cor0]
            //            OneRowRel
            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            UncollectRel uncollect =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[0],
                    correlationList);
            RelDataType countType =
                SqlStdOperatorTable.countOperator.inferReturnType(
                    rexBuilder.getTypeFactory(),
                    new RelDataType[0]);
            final List<Integer> argList = Collections.emptyList();
            final AggregateCall countCall =
                new AggregateCall(
                    SqlStdOperatorTable.countOperator,
                    false,
                    argList,
                    countType,
                    null);
            AggregateRel aggregateRel =
                new AggregateRel(
                    cluster,
                    uncollect,
                    1,
                    Collections.singletonList(countCall));
            final RexNode inputRef =
                RexUtil.maybeCast(
                    rexBuilder,
                    rexNode.getType(),
                    RelOptUtil.createInputRef(aggregateRel, -1));
            final RelNode projectRel =
                CalcRel.createProject(
                    aggregateRel,
                    new RexNode[] { inputRef },
                    null);
            return new CorrelatorRel(
                cluster,
                input,
                projectRel,
                correlationList,
                JoinRelType.INNER);
        } else if (SqlStdOperatorTable.castFunc == op) {
            // A call to
            //
            // CalcRel=[...,CAST($in_i AS XYZ MULTISET),...]
            //   CalcInput
            //
            // is eq. to
            //
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     Collect
            //       ProjectRel=[CAST($in) AS XYZ]
            //         Uncollect
            //           ProjectRel=[output=$cor0]
            //             OneRowRel
            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            UncollectRel uncollect =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[0],
                    correlationList);
            assert null != componentType;
            assert componentType.isStruct();
            RelDataType type = componentType.getFields()[0].getType();
            RexNode newCastCall =
                rexBuilder.makeCast(
                    type,
                    RelOptUtil.createInputRef(uncollect, 0));
            RelNode castRel =
                CalcRel.createProject(
                    uncollect,
                    new RexNode[] { newCastCall },
                    new String[] {
                        uncollect.getRowType().getFields()[0].getName()
                    });
            return new CorrelatorRel(
                cluster,
                input,
                castRel,
                correlationList,
                JoinRelType.LEFT);
        } else if (SqlStdOperatorTable.isASetOperator == op) {
            // (ms IS A SET) <=>
            // UNIQUE(UNNEST(ms)) <=>
            // NOT EXISTS ((select <ms element>, count(*) as c
            // from UNNEST(ms) GROUP BY <ms element>) WHERE c > 1)
            //
            // A call to
            //
            // CalcRel=[...,$in_i IS A SET,...]
            //   CalcInput
            //
            // is eq. to
            //
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
            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            UncollectRel uncollect =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[0],
                    correlationList);
            RelDataType countType =
                SqlStdOperatorTable.countOperator.inferReturnType(
                    rexBuilder.getTypeFactory(),
                    new RelDataType[0]);
            final List<Integer> argList = Collections.emptyList();
            final AggregateCall countCall =
                new AggregateCall(
                    SqlStdOperatorTable.countOperator,
                    false,
                    argList,
                    countType,
                    null);
            AggregateRel aggregateRel =
                new AggregateRel(
                    cluster,
                    uncollect,
                    1,
                    Collections.singletonList(countCall));
            RexNode c = RelOptUtil.createInputRef(aggregateRel, -1);
            RelNode filterRel =
                RelOptUtil.createExistsPlan(
                    cluster,
                    aggregateRel,
                    new RexNode[] {
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.greaterThanOperator,
                            c,
                            rexBuilder.makeExactLiteral(
                                new BigDecimal(BigInteger.ONE)))
                    },
                    null,
                    null);
            RelNode notExistRel = createExistsPlanSingleRow(filterRel, true);
            return new CorrelatorRel(
                cluster,
                input,
                notExistRel,
                correlationList,
                JoinRelType.LEFT);
        } else if (
            (SqlStdOperatorTable.elementFunc == op)
            || (SqlStdOperatorTable.elementSlicefunc == op))
        {
            // A call to
            //
            // CalcRel=[...,ELEMENT($in_i),...]
            //   CalcInput
            //
            // is eq. to
            //
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i]
            //     CalcInput
            //     LimitRel(max=1)
            //       Uncollect
            //         ProjectRel=[output=$cor0]
            //           OneRowRel
            //
            // (LimitRel is actually a collection of simpler rels.)
            //
            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            RelNode uncollect =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[0],
                    correlationList);
            RelNode limitRel = createLimitRel(uncollect);
            return new CorrelatorRel(
                cluster,
                input,
                limitRel,
                correlationList,
                JoinRelType.INNER);
        } else if (op instanceof SqlMultisetSetOperator) {
            // A call to
            //
            // CalcRel=[...,ms1 UNION ms2,...]
            //   CalcInput
            //
            // is eq. to
            //
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
            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            final UncollectRel uncollectRel0 =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[0],
                    correlationList);
            final UncollectRel uncollectRel1 =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[1],
                    correlationList);
            RelNode [] inputs = { uncollectRel0, uncollectRel1 };
            final RelNode setRel;
            if (SqlStdOperatorTable.multisetExceptAllOperator == op) {
                setRel = new MinusRel(cluster, inputs, true);
            } else if (SqlStdOperatorTable.multisetExceptOperator == op) {
                setRel = new MinusRel(cluster, inputs, false);
            } else if (SqlStdOperatorTable.multisetIntersectAllOperator == op) {
                setRel = new IntersectRel(cluster, inputs, true);
            } else if (SqlStdOperatorTable.multisetIntersectOperator == op) {
                setRel = new IntersectRel(cluster, inputs, false);
            } else if (SqlStdOperatorTable.multisetUnionAllOperator == op) {
                setRel = new UnionRel(cluster, inputs, true);
            } else if (SqlStdOperatorTable.multisetUnionOperator == op) {
                setRel = new UnionRel(cluster, inputs, false);
            } else {
                throw Util.newInternal("unexpected op " + op);
            }

            final CollectRel collectRel =
                new CollectRel(cluster, setRel, "multiset");
            return new CorrelatorRel(
                cluster,
                input,
                collectRel,
                correlationList,
                JoinRelType.INNER);
        } else if (SqlStdOperatorTable.memberOfOperator == op) {
            // (x MEMBER OF ms) <=>
            // (x IN UNNEST(ms)) <=>
            // (EXISTS (select <multiset element>
            //          from UNNEST(ms) WHERE <multiset element>=x))
            //
            // A call to
            //
            // CalcRel=[...,$in_i MEMBER OF $in_j,...]
            //   CalcInput
            //
            // is eq. to
            //
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i, $cor1=in_j]
            //     CalcInput
            //     ProjectRel=[CASE $0 > 0 THEN true ELSE false END]
            //       AggregateRel[count(*)]
            //         FilterRel=[$cor0 = $0]
            //           Uncollect
            //             ProjectRel=[output=$cor1]
            //                 OneRowRel

            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            final RexLocalRef localRef = (RexLocalRef) rexCall.operands[1];
            final UncollectRel uncollectRel =
                createUncollect(calc, localRef, correlationList);

            RexNode operand0 = rexCall.operands[0];
            final RexNode corRef;
            if (operand0 instanceof RexLocalRef) {
                final RexLocalRef local = (RexLocalRef) operand0;
                final String dyn_inIdStr = cluster.getQuery().createCorrel();
                final int dyn_inId = RelOptQuery.getCorrelOrdinal(dyn_inIdStr);
                correlationList.add(
                    new CorrelatorRel.Correlation(
                        dyn_inId,
                        local.getIndex()));
                corRef =
                    rexBuilder.makeCorrel(
                        local.getType(),
                        dyn_inIdStr);
            } else {
                corRef = operand0;
            }

            RelDataType elementType = localRef.getType().getComponentType();
            assert (null != elementType);
            RelNode filterRel =
                RelOptUtil.createExistsPlan(
                    cluster,
                    uncollectRel,
                    new RexNode[] {
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.equalsOperator,
                            corRef,
                            rexBuilder.makeInputRef(
                                elementType,
                                0))
                    },
                    null,
                    null);
            RelNode existsRel = createExistsPlanSingleRow(filterRel, false);
            return new CorrelatorRel(
                cluster,
                input,
                existsRel,
                correlationList,
                JoinRelType.LEFT);
        } else if (SqlStdOperatorTable.submultisetOfOperator == op) {
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
            //
            // CalcRel=[...,$in_i SUBMULTISET $in_j,...]
            //   CalcInput
            //
            // is eq. to
            //
            // CalcRel=[...,$in_N,...]
            //   CorrelRel=[$cor0=$in_i, $cor1=$in_j]
            //     CalcInput
            //     NotExistsRel
            //       FilterRel=[c1>c2 OR c2 IS NULL]
            //         JoinRel=[left outer join
            //                  on $0.* IS NOT DISTINCT FROM $1.*]
            //           AggregateRel[$0, count(*) as c1 group by $0]
            //             Uncollect
            //               ProjectRel=[output=$cor0]
            //                 OneRowRel
            //           AggregateRel[$0, count(*) as c2 group by $0]
            //             Uncollect
            //               ProjectRel=[output=$cor1]
            //                 OneRowRel
            final String dyn_inIdStr = cluster.getQuery().createCorrel();
            final int dyn_inId = RelOptQuery.getCorrelOrdinal(dyn_inIdStr);
            assert rexCall.operands[1] instanceof RexLocalRef;
            final RexLocalRef rexInput2 = (RexLocalRef) rexCall.operands[1];
            List<CorrelatorRel.Correlation> correlationList =
                new ArrayList<CorrelatorRel.Correlation>();
            correlationList.add(
                new CorrelatorRel.Correlation(
                    dyn_inId,
                    rexInput2.getIndex()));
            final RexNode corRef2 =
                rexBuilder.makeCorrel(
                    rexInput2.getType(),
                    dyn_inIdStr);
            RelNode projectRel2 =
                CalcRel.createProject(
                    new OneRowRel(cluster),
                    new RexNode[] { corRef2 },
                    new String[] { "output" + corRef2.toString() });

            final UncollectRel u1 =
                createUncollect(
                    calc,
                    (RexLocalRef) rexCall.operands[0],
                    correlationList);
            final UncollectRel u2 = new UncollectRel(cluster, projectRel2);

            // TODO wael 4/26/05: need to create proper count & group by agg
            // call def.
            final List<AggregateCall> aggCalls = Collections.emptyList();
            final Set<String> variablesStopped = Collections.emptySet();
            AggregateRel aggregateGroupRel1 =
                new AggregateRel(
                    cluster,
                    u1,
                    1,
                    aggCalls);
            AggregateRel aggregateGroupRel2 =
                new AggregateRel(
                    cluster,
                    u2,
                    1,
                    aggCalls);
            JoinRel joinRel =
                new JoinRel(
                    cluster,
                    aggregateGroupRel1,
                    aggregateGroupRel2,
                    RelOptUtil.isDistinctFrom(
                        rexBuilder,
                        rexBuilder.makeRangeReference(u1.getRowType()), //todo get the right input ref from agg
                        rexBuilder.makeRangeReference(u2.getRowType()), //todo get the right input ref from agg
                        true),
                    JoinRelType.LEFT,
                    variablesStopped);
            RelNode filterRel =
                RelOptUtil.createExistsPlan(
                    cluster,
                    joinRel,
                    new RexNode[] {
                        rexBuilder.makeCall(
                            SqlStdOperatorTable.greaterThanOperator,
                            RelOptUtil.createInputRef(joinRel, 0),
                            rexBuilder.makeExactLiteral(
                                new BigDecimal(BigInteger.ONE)))
                    },
                    null,
                    null);
            RelNode notExistsRel = createExistsPlanSingleRow(filterRel, true);
            return new CorrelatorRel(
                cluster,
                input,
                notExistsRel,
                correlationList,
                JoinRelType.LEFT);
        } else {
            throw Util.newInternal(
                op.getName() + " is not defined to be implementable");
        }
    }

    private static UncollectRel createUncollect(
        CalcRel calc,
        RexLocalRef local,
        List<CorrelatorRel.Correlation> correlationList)
    {
        RelNode corProjectRel = createProject(calc, local, correlationList);
        return new UncollectRel(
            calc.getCluster(),
            corProjectRel);
    }

    private static RelNode createProject(
        CalcRel calc,
        RexLocalRef local,
        List<CorrelatorRel.Correlation> correlationList)
    {
        // If the operand is a call to the 'slice' operator, ignore the slice
        // operator. The slice expression, if unused, will be garbage-collected
        // later.
        boolean slice = false;
        final RexNode expr =
            calc.getProgram().getExprList().get(local.getIndex());
        if (expr instanceof RexCall) {
            RexCall call = (RexCall) expr;
            if (call.getOperator() == SqlStdOperatorTable.sliceOp) {
                slice = true;
                local = (RexLocalRef) call.getOperands()[0];
            }
        }
        Util.discard(slice);

        // Create a correlation, and add it to the list.
        RelOptCluster cluster = calc.getCluster();
        final String dyn_inIdStr = cluster.getQuery().createCorrel();
        final int dyn_inId = RelOptQuery.getCorrelOrdinal(dyn_inIdStr);
        correlationList.add(
            new CorrelatorRel.Correlation(
                dyn_inId,
                local.getIndex()));
        final RexNode corRef =
            cluster.getRexBuilder().makeCorrel(
                local.getType(),
                dyn_inIdStr);

        // Create and return a projection.
        return CalcRel.createProject(
            new OneRowRel(cluster),
            new RexNode[] { corRef },
            new String[] { "output" + corRef.toString() });
    }

    /**
     * Creates a relational expression which ensures that a given expression
     * only returns one row, and throws a runtime error otherwise. The result is
     *
     * <pre>
     * ProjectRel($0)
     *   FilterRel[condition=CASE WHEN $0==1 THEN true ELSE throw("21000") END]
     *     AggregateRel[count(*), $0]
     *
     * </pre>
     *
     * where 21000 is the standard CARDINALITY VIOLATION error code.
     *
     * @param child Child relational expression
     *
     * @return A relational expression of the same type
     *
     * @post return.getRowType() == child.getRowType()
     */
    private static RelNode createLimitRel(RelNode child)
    {
        final RelOptCluster cluster = child.getCluster();
        final RexBuilder rexBuilder = cluster.getRexBuilder();

        // TODO wael 3/29/05: need to create proper count agg call def.
        RelDataType countType =
            SqlStdOperatorTable.countOperator.inferReturnType(
                rexBuilder.getTypeFactory(),
                new RelDataType[0]);
        final List<Integer> argList = Collections.emptyList();
        final AggregateCall countCall =
            new AggregateCall(
                SqlStdOperatorTable.countOperator,
                false,
                argList,
                countType,
                null);
        final int groupCount = child.getRowType().getFieldCount();
        AggregateRel aggregateRel =
            new AggregateRel(
                cluster,
                child,
                groupCount,
                Collections.singletonList(countCall));
        RexNode expr0 = RelOptUtil.createInputRef(aggregateRel, -1);
        RexNode [] whenThenElse =
        { // when


            rexBuilder.makeCall(
                SqlStdOperatorTable.equalsOperator,
                expr0,
                rexBuilder.makeExactLiteral(new BigDecimal(BigInteger.ONE))),

            // then
            rexBuilder.makeLiteral(true),

            // else
            rexBuilder.makeCall(
                SqlStdOperatorTable.throwOperator,
                rexBuilder.makeLiteral(
                    SqlStateCodes.CardinalityViolation.getState()))
        };
        RexNode condition =
            rexBuilder.makeCall(
                SqlStdOperatorTable.caseOperator,
                whenThenElse);
        RelNode filterRel = CalcRel.createFilter(aggregateRel, condition);
        final RelDataTypeField field = filterRel.getRowType().getFields()[0];
        RelNode limitRel =
            CalcRel.createProject(
                filterRel,
                new RexNode[] { RelOptUtil.createInputRef(filterRel, 0) },
                new String[] { field.getName() });
        assert RelOptUtil.eq(
            "return.getRowType()",
            limitRel.getRowType(),
            "child.getRowType()",
            child.getRowType(),
            true) : "post: return.getRowType() == child.getRowType()";
        return limitRel;
    }

    /**
     * Returns true if any expression in a program contains a multiset call
     * directly under another multiset call.
     */
    private boolean containsNestedMultiset(RexProgram program)
    {
        for (RexNode expr : program.getExprList()) {
            if (containsNestedMultiset(expr, true)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if node contains a multiset call directly under another
     * multiset call.
     */
    private boolean containsNestedMultiset(RexNode node, boolean deep)
    {
        RexCall multisetCall = RexMultisetUtil.findFirstMultiset(node, deep);
        if (null == multisetCall) {
            return false;
        }
        for (int i = 0; i < multisetCall.operands.length; i++) {
            if ((multisetCall.operands[i] instanceof RexCall)
                && RexMultisetUtil.containsMultiset(
                    multisetCall.operands[i],
                    false))
            {
                return true;
            }
        }
        return false;
    }

    public RelNode createExistsPlanSingleRow(RelNode child, boolean neg)
    {
        RelOptCluster cluster = child.getCluster();
        RelDataType countType =
            SqlStdOperatorTable.countOperator.inferReturnType(
                cluster.getTypeFactory(),
                new RelDataType[0]);
        final List<Integer> argList = Collections.emptyList();
        final AggregateCall countCall =
            new AggregateCall(
                SqlStdOperatorTable.countOperator,
                false,
                argList,
                countType,
                null);
        AggregateRel aggregateRel =
            new AggregateRel(
                cluster,
                child,
                1,
                Collections.singletonList(countCall));
        SqlOperator op;
        if (neg) {
            op = SqlStdOperatorTable.equalsOperator;
        } else {
            op = SqlStdOperatorTable.greaterThanOperator;
        }
        RexNode expr0 = RelOptUtil.createInputRef(aggregateRel, -1);
        RexNode [] whenThenElse =
        {
            // when
            cluster.getRexBuilder().makeCall(
                op,
                expr0,
                cluster.getRexBuilder().makeExactLiteral(
                    new BigDecimal(BigInteger.ZERO))),

            // then
            cluster.getRexBuilder().makeLiteral(true),

            // else
            cluster.getRexBuilder().makeLiteral(false)
        };
        RexNode caseRexNode =
            cluster.getRexBuilder().makeCall(
                SqlStdOperatorTable.caseOperator,
                whenThenElse);
        RelNode caseRel =
            CalcRel.createProject(
                aggregateRel,
                new RexNode[] { caseRexNode },
                new String[] { "case" });
        return caseRel;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Unmix mixing between multisets and non-multisets
     */
    private class MultisetRelSplitter
        extends CalcRelSplitter
    {
        private MultisetRelSplitter(CalcRel calc)
        {
            super(
                calc,
                new RelType[] {
                    new CalcRelSplitter.RelType("REL_TYPE_MULTISET") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            // Field access rex nodes are not multisets
                            return false;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            // Dynamic param rex nodes are not multisets
                            return false;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return RexMultisetUtil.containsMultiset(
                                call,
                                false);
                        }
                    }
                    ,
                    new CalcRelSplitter.RelType("REL_TYPE_NOT_MULTISET") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return !RexMultisetUtil.containsMultiset(
                                call,
                                false);
                        }
                    }
                });
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
            super(
                calc,
                new RelType[] {
                    new CalcRelSplitter.RelType("REL_TYPE_NESTED") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return false;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return containsNestedMultiset(call, false);
                        }
                    }
                    ,
                    new CalcRelSplitter.RelType("REL_TYPE_NOT_NESTED") {
                        protected boolean canImplement(RexFieldAccess field)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexDynamicParam param)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexLiteral literal)
                        {
                            return true;
                        }

                        protected boolean canImplement(RexCall call)
                        {
                            return !containsNestedMultiset(call, false);
                        }
                    }
                });
        }
    }
}

// End FarragoMultisetSplitterRule.java

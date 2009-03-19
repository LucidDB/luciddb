/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2002-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2003-2007 John V. Sichi
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

import java.util.*;

import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.*;


/**
 * <code>SwapJoinRule</code> permutes the inputs to a join. Outer joins cannot
 * be permuted.
 *
 * @author jhyde
 * @version $Id$
 * @since Nov 26, 2003
 */
public class SwapJoinRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singleton
     */
    public static final SwapJoinRule instance = new SwapJoinRule();

    //~ Constructors -----------------------------------------------------------

    public SwapJoinRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Returns a relational expression with the inputs switched round. Does not
     * modify <code>join</code>. Returns null if the join cannot be swapped (for
     * example, because it is an outer join).
     */
    public static RelNode swap(JoinRel join)
    {
        return swap(join, false);
    }

    /**
     * @param join join to be swapped
     * @param swapOuterJoins whether outer joins should be swapped
     *
     * @return swapped join if swapping possible; else null
     */
    public static RelNode swap(JoinRel join, boolean swapOuterJoins)
    {
        JoinRelType joinType = join.getJoinType();
        switch (joinType) {
        case LEFT:
            if (!swapOuterJoins) {
                return null;
            }
            joinType = JoinRelType.RIGHT;
            break;
        case RIGHT:
            if (!swapOuterJoins) {
                return null;
            }
            joinType = JoinRelType.LEFT;
            break;
        }
        final RexBuilder rexBuilder = join.getCluster().getRexBuilder();
        final RelDataType leftRowType = join.getLeft().getRowType();
        final RelDataType rightRowType = join.getRight().getRowType();
        final VariableReplacer variableReplacer =
            new VariableReplacer(rexBuilder, leftRowType, rightRowType);
        final RexNode oldCondition = join.getCondition().clone();
        RexNode condition = variableReplacer.go(oldCondition);

        // NOTE jvs 14-Mar-2006: We preserve attribute semiJoinDone after the
        // swap.  This way, we will generate one semijoin for the original
        // join, and one for the swapped join, and no more.  This
        // doesn't prevent us from seeing any new combinations assuming
        // that the planner tries the desired order (semijoins after swaps).

        JoinRel newJoin =
            new JoinRel(
                join.getCluster(),
                join.getRight(),
                join.getLeft(),
                condition,
                joinType,
                Collections.<String>emptySet(),
                join.isSemiJoinDone());
        if (!join.getVariablesStopped().isEmpty()) {
            newJoin.setVariablesStopped(
                new HashSet<String>(join.getVariablesStopped()));
        }
        final RexNode [] exps =
            RelOptUtil.createSwappedJoinExprs(newJoin, join, true);
        return CalcRel.createProject(
            newJoin,
            exps,
            RelOptUtil.getFieldNames(join.getRowType()),
            true);
    }

    public void onMatch(final RelOptRuleCall call)
    {
        JoinRel join = (JoinRel) call.rels[0];

        final RelNode swapped = swap(join);
        if (swapped == null) {
            return;
        }

        // The result is either a Project or, if the project is trivial, a
        // raw Join.
        final JoinRel newJoin =
            (swapped instanceof JoinRel) ? (JoinRel) swapped
            : (JoinRel) swapped.getInput(0);

        call.transformTo(swapped);

        // We have converted join='a join b' into swapped='select
        // a0,a1,a2,b0,b1 from b join a'. Now register that project='select
        // b0,b1,a0,a1,a2 from (select a0,a1,a2,b0,b1 from b join a)' is the
        // same as 'b join a'. If we didn't do this, the swap join rule
        // would fire on the new join, ad infinitum.
        final RexNode [] exps =
            RelOptUtil.createSwappedJoinExprs(
                newJoin,
                join,
                false);
        RelNode project =
            CalcRel.createProject(
                swapped,
                exps,
                RelOptUtil.getFieldNames(newJoin.getRowType()));

        // Make sure extra traits are carried over from the original rel
        project =
            RelOptRule.convert(
                project,
                swapped.getTraits());

        RelNode rel = call.getPlanner().ensureRegistered(project, newJoin);
        Util.discard(rel);
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Walks over an expression, replacing references to fields of the left and
     * right inputs.
     *
     * <p>If the field index is less than leftFieldCount, it must be from the
     * left, and so has rightFieldCount added to it; if the field index is
     * greater than leftFieldCount, it must be from the right, so we subtract
     * leftFieldCount from it.</p>
     */
    private static class VariableReplacer
    {
        private final RexBuilder rexBuilder;
        private final RelDataTypeField [] leftFields;
        private final RelDataTypeField [] rightFields;

        VariableReplacer(
            RexBuilder rexBuilder,
            RelDataType leftType,
            RelDataType rightType)
        {
            this.rexBuilder = rexBuilder;
            this.leftFields = leftType.getFields();
            this.rightFields = rightType.getFields();
        }

        public RexNode go(RexNode rex)
        {
            if (rex instanceof RexCall) {
                RexNode [] operands = ((RexCall) rex).operands;
                for (int i = 0; i < operands.length; i++) {
                    RexNode operand = operands[i];
                    operands[i] = go(operand);
                }
                return rex;
            } else if (rex instanceof RexInputRef) {
                RexInputRef var = (RexInputRef) rex;
                int index = var.getIndex();
                if (index < leftFields.length) {
                    // Field came from left side of join. Move it to the right.
                    return rexBuilder.makeInputRef(
                        leftFields[index].getType(),
                        rightFields.length + index);
                }
                index -= leftFields.length;
                if (index < rightFields.length) {
                    // Field came from right side of join. Move it to the left.
                    return rexBuilder.makeInputRef(
                        rightFields[index].getType(),
                        index);
                }
                throw Util.newInternal(
                    "Bad field offset: index="
                    + var.getIndex()
                    + ", leftFieldCount=" + leftFields.length
                    + ", rightFieldCount=" + rightFields.length);
            } else {
                return rex;
            }
        }
    }
}

// End SwapJoinRule.java

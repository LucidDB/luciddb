/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2002-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2003-2005 John V. Sichi
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

import java.util.Collections;
import java.util.HashSet;

import org.eigenbase.relopt.RelOptRule;
import org.eigenbase.relopt.RelOptRuleCall;
import org.eigenbase.relopt.RelOptRuleOperand;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;
import org.eigenbase.util.Util;


/**
 * <code>SwapJoinRule</code> permutes the inputs to a join. Outer joins cannot
 * be permuted.
 *
 * @author jhyde
 * @since Nov 26, 2003
 * @version $Id$
 */
public class SwapJoinRule extends RelOptRule
{
    //~ Constructors ----------------------------------------------------------

    public SwapJoinRule()
    {
        super(new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(RelNode.class, null),
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    /**
     * Returns a relational expression with the inputs switched round. Does
     * not modify <code>join</code>. Returns null if the join cannot be
     * swapped (for example, because it is an outer join).
     */
    public static ProjectRel swap(JoinRel join)
    {
        // We cannot swap an asymmetric join
        switch (join.getJoinType()) {
        case JoinRel.JoinType.LEFT:
        case JoinRel.JoinType.RIGHT:
            return null;
        }
        final RexBuilder rexBuilder = join.cluster.rexBuilder;
        final RelDataType leftRowType = join.getLeft().getRowType();
        final RelDataType rightRowType = join.getRight().getRowType();
        final VariableReplacer variableReplacer =
            new VariableReplacer(rexBuilder, leftRowType, rightRowType);
        final RexNode oldCondition = RexUtil.clone(join.getCondition());
        RexNode condition = variableReplacer.go(oldCondition);
        JoinRel newJoin =
            new JoinRel(
                join.getCluster(),
                join.getRight(),
                join.getLeft(),
                condition,
                join.getJoinType(),
                Collections.EMPTY_SET);
        if (!join.getVariablesStopped().isEmpty()) {
            newJoin.setVariablesStopped(
                new HashSet(join.getVariablesStopped()));
        }
        final RelDataTypeField [] newJoinFields =
            newJoin.getRowType().getFields();
        final RexNode [] exps = new RexNode[newJoinFields.length];
        for (int i = 0; i < exps.length; i++) {
            int source = (i + rightRowType.getFieldList().size()) % exps.length;
            exps[i] =
                rexBuilder.makeInputRef(
                    newJoinFields[source].getType(),
                    source);
        }
        final String [] fieldNames = getFieldNames(join.getRowType());
        return new ProjectRel(
            join.getCluster(),
            newJoin,
            exps,
            fieldNames,
            ProjectRel.Flags.Boxed);
    }

    private static String [] getFieldNames(RelDataType rowType)
    {
        final RelDataTypeField [] fields = rowType.getFields();
        final String [] fieldNames = new String[fields.length];
        for (int i = 0; i < fields.length; i++) {
            fieldNames[i] = fields[i].getName();
        }
        return fieldNames;
    }

    public void onMatch(final RelOptRuleCall call)
    {
        JoinRel join = (JoinRel) call.rels[0];
        if (join instanceof CorrelatorRel) {
            return;
        }

        final ProjectRel swapped = swap(join);
        if (swapped != null) {
            final JoinRel newJoin = (JoinRel) swapped.getInput(0);
            call.transformTo(swapped);

            // We have converted join='a join b' into
            // swapped='select a0,a1,a2,b0,b1 from b join a'.
            // Now register that
            // project='select b0,b1,a0,a1,a2 from (select a0,a1,a2,b0,b1 from b join a)' is
            // the same as 'b join a'.
            // If we didn't do this, the swap join rule would fire on the new
            // join, ad infinitum.
            final RexBuilder rexBuilder = swapped.cluster.rexBuilder;
            final RelDataType newJoinRowType = newJoin.getRowType();
            final RelDataTypeField [] newJoinFields =
                newJoinRowType.getFields();
            final RexNode [] exps = new RexNode[newJoinFields.length];
            final String [] fieldNames = new String[newJoinFields.length];
            for (int i = 0; i < exps.length; i++) {
                int source =
                    (i + join.getLeft().getRowType().getFieldList().size()) % exps.length;
                final RelDataTypeField newJoinField = newJoinFields[i];
                exps[i] =
                    rexBuilder.makeInputRef(
                        newJoinField.getType(),
                        source);
                fieldNames[i] = newJoinField.getName();
            }
            ProjectRel project =
                new ProjectRel(
                    swapped.getCluster(),
                    swapped,
                    exps,
                    fieldNames,
                    ProjectRel.Flags.Boxed);
            RelNode rel = call.planner.register(project, newJoin);
            Util.discard(rel);
        }
    }

    //~ Inner Classes ---------------------------------------------------------

    /**
     * Walks over an expression, replacing references to fields of the left and
     * right inputs.
     *
     * <p>If the field index is less than leftFieldCount, it must
     * be from the left, and so has rightFieldCount added to it; if the field
     * index is greater than leftFieldCount, it must be from the right, so
     * we subtract leftFieldCount from it.</p>
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
                int index = var.index;
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
                throw Util.newInternal("Bad field offset: index=" + var.index
                    + ", leftFieldCount=" + leftFields.length
                    + ", rightFieldCount=" + rightFields.length);
            } else {
                return rex;
            }
        }
    }
}


// End SwapJoinRule.java

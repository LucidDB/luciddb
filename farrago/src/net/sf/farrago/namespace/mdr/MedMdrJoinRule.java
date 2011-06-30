/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2003 John V. Sichi
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
package net.sf.farrago.namespace.mdr;

import java.util.*;

import javax.jmi.model.*;

import org.eigenbase.jmi.*;
import org.eigenbase.rel.*;
import org.eigenbase.rel.rules.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.fun.SqlStdOperatorTable;

/**
 * MedMdrJoinRule is a rule for converting a JoinRel into a MedMdrJoinRel when
 * the join condition navigates an association.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class MedMdrJoinRule
    extends RelOptRule
{
    // TODO:  allow join to work on other inputs (e.g. filters, other joins)

    /**
     * Instance of the rule that converts {@code JoinRel(any,
     * MedMdrClassExtentRel)} to {@code MedMdrJoinRel} with the same inputs.
     */
    public static final MedMdrJoinRule INSTANCE =
        new MedMdrJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                RelOptRuleOperand.hasSystemFields(false),
                false,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(MedMdrClassExtentRel.class, ANY)),
            "MedMdrJoinRule:Vanilla");

    /**
     * Instance of the rule that converts
     * {@code JoinRel(any, ProjectRel(MedMdrClassExtentRel))}
     * to
     * {@code ProjectRel(MedMdrJoinRel(any, MedMdrClassExtentRel))},
     * provided that ProjectRel projects the required fields.
     */
    public static final MedMdrJoinRule PROJECT_INSTANCE =
        new MedMdrJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                RelOptRuleOperand.hasSystemFields(false),
                false,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(MedMdrClassExtentRel.class, ANY))),
            "MedMdrJoinRule:Project");

    //~ Constructors -----------------------------------------------------------

    /**
     * Intentionally private; use singleton.
     *
     * @param operand Rule operand
     * @param description Description
     */
    private MedMdrJoinRule(RelOptRuleOperand operand, String description)
    {
        super(operand, description);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return CallingConvention.ITERATOR;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        RelNode leftRel = call.rels[1];
        ProjectRel projectRel;
        MedMdrClassExtentRel rightRel;
        final JoinRelType joinType = joinRel.getJoinType();
        if (call.rels[2] instanceof ProjectRel) {
            projectRel = (ProjectRel) call.rels[2];
            rightRel = (MedMdrClassExtentRel) call.rels[3];

            if (!canPullProjectExprs(
                    projectRel.getChildExps(),
                    joinType.generatesNullsOnRight()))
            {
                call.failed(
                    "cannot pull non-trivial projection through "
                    + "null-generating side of join");
                return;
            }
        } else {
            projectRel = null;
            rightRel = (MedMdrClassExtentRel) call.rels[2];
        }

        if (!joinRel.getVariablesStopped().isEmpty()) {
            call.failed("variables are stopped");
            return;
        }

        assert joinRel.getSystemFieldList().isEmpty()
            : "Operand predicate should ensure no sys fields";

        switch (joinType) {
        case INNER:
        case LEFT:
            break;
        default:
            call.failed("rule does not apply to join type " + joinType);
            return;
        }

        int [] joinFieldOrdinals = new int[2];
        if (!RelOptUtil.analyzeSimpleEquiJoin(joinRel, joinFieldOrdinals)) {
            return;
        }
        int leftOrdinal = joinFieldOrdinals[0];
        int rightOrdinal = joinFieldOrdinals[1];

        if (projectRel != null) {
            rightOrdinal = projectRel.getSourceField(rightOrdinal);
            if (rightOrdinal < 0) {
                return;
            }
        }

        // on right side, must join to reference field which refers to
        // left side type
        List<StructuralFeature> features =
            JmiObjUtil.getFeatures(
                rightRel.mdrClassExtent.refClass,
                StructuralFeature.class,
                false);
        Reference reference;
        if (rightOrdinal == features.size()) {
            // join to mofId: this is a many-to-one join (primary key lookup on
            // right hand side), which we will represent with a null reference
            reference = null;
        } else {
            if (rightOrdinal > features.size()) {
                // Pseudocolumn such as mofClassName:  can't join.
                return;
            }
            StructuralFeature feature = features.get(rightOrdinal);
            if (!(feature instanceof Reference)) {
                return;
            }
            reference = (Reference) feature;
        }

        // TODO:  verify that leftOrdinal specifies a MOFID of an
        // appropriate type; also, verify that left and right
        // are from same repository

        /*
        Classifier referencedType = reference.getReferencedEnd().getType();
         Classifier leftType = (Classifier)
         leftRel.mdrClassExtent.refClass.refMetaObject(); if
         (!leftType.equals(referencedType) &&
         !leftType.allSupertypes().contains(referencedType)) { // REVIEW: we now
         know this is a bogus join; could optimize it by // skipping querying
         altogether, but a warning of some kind would // be friendlier return; }
         */
        RelNode iterLeft =
            convert(
                leftRel,
                joinRel.getTraits().plus(CallingConvention.ITERATOR));
        if (iterLeft == null) {
            return;
        }

        RelNode iterRight =
            convert(
                rightRel,
                joinRel.getTraits().plus(CallingConvention.ITERATOR));
        if (iterRight == null) {
            return;
        }

        final RelNode newRel;
        if (projectRel != null) {
            // Dummy join relational expression, mainly to serve as a factory
            // for a correctly-wired MedMdrJoinRel.
            MedMdrJoinRel dummyJoin =
                new MedMdrJoinRel(
                    joinRel.getCluster(),
                    leftRel,
                    projectRel,
                    joinRel.getCondition(), joinType,
                    leftOrdinal,
                    reference);
            newRel =
                PullUpProjectsAboveJoinRule.createJoin(
                    dummyJoin, null, projectRel, leftRel, rightRel);
        } else {
            newRel =
                new MedMdrJoinRel(
                    joinRel.getCluster(),
                    iterLeft,
                    iterRight,
                    joinRel.getCondition(), joinType,
                    leftOrdinal,
                    reference);
        }

        call.transformTo(newRel);
    }

    /**
     * Returns whether the given project expressions can be pulled a join. If
     * the join generates nulls, only trivial expressions can be pulled up.
     *
     * <p>Consider:
     *
     * <blockquote><code>
     * select dept.name, e.x from dept<br/>
     * join (select deptno, empno + sal as x) as e<br/>
     * using (deptno)
     * </code></blockquote>
     *
     * is equivalent to
     *
     * <blockquote><code>
     * select dept.name, e.empno + e.sal as x from dept<br/>
     * join (select deptno, empno, sal) as e<br/>
     * using (deptno)
     * </code></blockquote>
     *
     * because for any null row generated from the right-hand side of the join,
     * empno and sal will be null, and therefore empno + sal will be null. This
     * property holds for raw column references, and expressions made up of
     * operators (such as '+' in this case) that evaluate to null if any of
     * their inputs are null.
     *
     * <p>Literal values do not have this property, nor do expressions involving
     * non-null-preserving operators.
     *
     * @param exprs Expressions being projected by input to the join
     * @param generatesNull Whether this side of the join generates NULL rows
     * @return Whether the projections can be pulled up through the join
     */
    private boolean canPullProjectExprs(
        RexNode[] exprs,
        boolean generatesNull)
    {
        return !generatesNull
            || RexVisitorImpl.visitArrayAnd(
                new NullPreservationShuttle(), exprs);
    }

    /**
     * Shuttle that detects whether an expression preserves nulls; that is,
     * returns null if and only if its inputs are null.
     */
    private static class NullPreservationShuttle
        extends RexVisitorImpl<Boolean>
    {
        protected NullPreservationShuttle()
        {
            super(true);
        }

        @Override
        public Boolean visitInputRef(RexInputRef inputRef)
        {
            return true;
        }

        @Override
        public Boolean visitLocalRef(RexLocalRef localRef)
        {
            return true;
        }

        @Override
        public Boolean visitLiteral(RexLiteral literal)
        {
            return false;
        }

        @Override
        public Boolean visitOver(RexOver over)
        {
            return false;
        }

        @Override
        public Boolean visitCorrelVariable(RexCorrelVariable correlVariable)
        {
            return false;
        }

        @Override
        public Boolean visitCall(RexCall call)
        {
            return call.getOperator() == SqlStdOperatorTable.plusOperator
                && visitArrayAnd(this, call.getOperands());
        }

        @Override
        public Boolean visitDynamicParam(RexDynamicParam dynamicParam)
        {
            return false;
        }

        @Override
        public Boolean visitRangeRef(RexRangeRef rangeRef)
        {
            return false;
        }

        @Override
        public Boolean visitFieldAccess(RexFieldAccess fieldAccess)
        {
            return true;
        }
    }
}

// End MedMdrJoinRule.java

/*
// $Id$
// Package org.eigenbase is a class library of data management components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2002 SQLstream, Inc.
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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.util.Pair;


/**
 * PullUpProjectsAboveJoinRule implements the rule for pulling {@link
 * ProjectRel}s beneath a {@link JoinRel} above the {@link JoinRel}. Projections
 * are pulled up if the {@link ProjectRel} doesn't originate from a null
 * generating input in an outer join.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PullUpProjectsAboveJoinRule
    extends RelOptRule
{
    // ~ Static fields/initializers --------------------------------------------

    //~ Static fields/initializers ---------------------------------------------

    public static final PullUpProjectsAboveJoinRule instanceTwoProjectChildren =
        new PullUpProjectsAboveJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(ProjectRel.class, ANY),
                new RelOptRuleOperand(ProjectRel.class, ANY)),
            "PullUpProjectsAboveJoinRule: with two ProjectRel children");

    public static final PullUpProjectsAboveJoinRule instanceLeftProjectChild =
        new PullUpProjectsAboveJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(ProjectRel.class, ANY)),
            "PullUpProjectsAboveJoinRule: with ProjectRel on left");

    public static final PullUpProjectsAboveJoinRule instanceRightProjectChild =
        new PullUpProjectsAboveJoinRule(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(ProjectRel.class, ANY)),
            "PullUpProjectsAboveJoinRule: with ProjectRel on right");

    //~ Constructors -----------------------------------------------------------

    public PullUpProjectsAboveJoinRule(
        RelOptRuleOperand operand,
        String description)
    {
        super(operand, description);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];
        JoinRelType joinType = joinRel.getJoinType();

        ProjectRel leftProj;
        ProjectRel rightProj;
        RelNode leftJoinChild;
        RelNode rightJoinChild;

        // see if at least one input's projection doesn't generate nulls
        if (hasLeftChild(call) && !joinType.generatesNullsOnLeft()) {
            leftProj = (ProjectRel) call.rels[1];
            leftJoinChild = getProjectChild(call, leftProj, true);
        } else {
            leftProj = null;
            leftJoinChild = call.rels[1];
        }
        if (hasRightChild(call) && !joinType.generatesNullsOnRight()) {
            rightProj = getRightChild(call);
            rightJoinChild = getProjectChild(call, rightProj, false);
        } else {
            rightProj = null;
            rightJoinChild = joinRel.getRight();
        }
        if ((leftProj == null) && (rightProj == null)) {
            return;
        }
        RelNode newProjRel =
            createJoin(
                joinRel, leftProj, rightProj, leftJoinChild, rightJoinChild);

        call.transformTo(newProjRel);
    }

    public static RelNode createJoin(
        JoinRelBase joinRel,
        ProjectRel leftProj,
        ProjectRel rightProj,
        RelNode leftJoinChild,
        RelNode rightJoinChild)
    {
        // Construct two RexPrograms and combine them.  The bottom program
        // is a join of the projection expressions from the left and/or
        // right projects that feed into the join.  The top program contains
        // the join condition.

        // Create a row type representing a concatentation of the inputs
        // underneath the projects that feed into the join.  This is the input
        // into the bottom RexProgram.  Note that the join type is an inner
        // join because the inputs haven't actually been joined yet.
        RelDataType joinChildrenRowType =
            JoinRel.deriveJoinRowType(
                leftJoinChild.getRowType(),
                rightJoinChild.getRowType(),
                JoinRelType.INNER,
                joinRel.getCluster().getTypeFactory(),
                null,
                Collections.<RelDataTypeField>emptyList());

        // Create projection expressions, combining the projection expressions
        // from the projects that feed into the join.  For the RHS projection
        // expressions, shift them to the right by the number of fields on
        // the LHS.  If the join input was not a projection, simply create
        // references to the inputs.
        List<RexNode> projExprs = new ArrayList<RexNode>();
        List<Pair<String, RelDataType>> fields =
            new ArrayList<Pair<String, RelDataType>>();
        RexBuilder rexBuilder = joinRel.getCluster().getRexBuilder();

        createProjectExprs(
            leftProj,
            leftJoinChild,
            0,
            rexBuilder,
            joinChildrenRowType.getFields(),
            projExprs,
            fields);

        RelDataTypeField [] leftFields = leftJoinChild.getRowType().getFields();
        int nFieldsLeft = leftFields.length;
        createProjectExprs(
            rightProj,
            rightJoinChild,
            nFieldsLeft,
            rexBuilder,
            joinChildrenRowType.getFields(),
            projExprs,
            fields);

        RelDataType projRowType =
            rexBuilder.getTypeFactory().createStructType(fields);

        // create the RexPrograms and merge them
        RexProgram bottomProgram =
            RexProgram.create(
                joinChildrenRowType,
                projExprs.toArray(new RexNode[projExprs.size()]),
                null,
                projRowType,
                rexBuilder);
        RexProgramBuilder topProgramBuilder =
            new RexProgramBuilder(
                projRowType,
                rexBuilder);
        topProgramBuilder.addIdentity();
        topProgramBuilder.addCondition(joinRel.getCondition());
        RexProgram topProgram = topProgramBuilder.getProgram();
        RexProgram mergedProgram =
            RexProgramBuilder.mergePrograms(
                topProgram,
                bottomProgram,
                rexBuilder);

        // expand out the join condition and construct a new JoinRel that
        // directly references the join children without the intervening
        // ProjectRels
        RexNode newCondition =
            mergedProgram.expandLocalRef(
                mergedProgram.getCondition());
        JoinRelBase newJoinRel =
            joinRel.copy(
                newCondition,
                joinRel.getSystemFieldList(),
                convert(
                    leftJoinChild,
                    joinRel.getTraits().plus(CallingConvention.ITERATOR)),
                rightJoinChild);

        // expand out the new projection expressions; if the join is an
        // outer join, modify the expressions to reference the join output
        List<RexNode> newProjExprs = new ArrayList<RexNode>();
        List<RelDataTypeField> newJoinFields =
            newJoinRel.getRowType().getFieldList();
        int nJoinFields = newJoinFields.size();
        int [] adjustments = new int[nJoinFields];
        for (RexLocalRef proj : mergedProgram.getProjectList()) {
            newProjExprs.add(mergedProgram.expandLocalRef(proj));
        }
        if (joinRel.getJoinType() != JoinRelType.INNER) {
            RelOptUtil.RexInputConverter inputConverter =
                new RelOptUtil.RexInputConverter(
                    rexBuilder,
                    joinChildrenRowType.getFieldList(),
                    newJoinFields,
                    adjustments);
            for (int i = 0; i < newProjExprs.size(); i++) {
                newProjExprs.set(i, newProjExprs.get(i).accept(inputConverter));
            }
        }

        // finally, create the projection on top of the join
        return CalcRel.createProject(
            newJoinRel,
            newProjExprs,
            RelOptUtil.getFieldNameList(projRowType));
    }

    /**
     * @param call RelOptRuleCall
     *
     * @return true if the rule was invoked with a left project child
     */
    protected boolean hasLeftChild(RelOptRuleCall call)
    {
        return (call.rels[1] instanceof ProjectRel);
    }

    /**
     * @param call RelOptRuleCall
     *
     * @return true if the rule was invoked with 2 children
     */
    protected boolean hasRightChild(RelOptRuleCall call)
    {
        return call.rels.length == 3;
    }

    /**
     * @param call RelOptRuleCall
     *
     * @return ProjectRel corresponding to the right child
     */
    protected ProjectRel getRightChild(RelOptRuleCall call)
    {
        return (ProjectRel) call.rels[2];
    }

    /**
     * Returns the child of the project that will be used as input into the new
     * JoinRel once the projects are pulled above the JoinRel.
     *
     * @param call RelOptRuleCall
     * @param project project RelNode
     * @param leftChild true if the project corresponds to the left projection
     * @return child of the project that will be used as input into the new
     * JoinRel once the projects are pulled above the JoinRel
     */
    protected RelNode getProjectChild(
        RelOptRuleCall call,
        ProjectRel project,
        boolean leftChild)
    {
        return project.getChild();
    }

    /**
     * Creates projection expressions corresponding to one of the inputs into
     * the join
     *
     * @param projRel the projection input into the join (if it exists)
     * @param joinChild the child of the projection input (if there is a
     * projection); otherwise, this is the join input
     * @param adjustmentAmount the amount the expressions need to be shifted by
     * @param rexBuilder rex builder
     * @param joinChildrenFields concatentation of the fields from the left and
     * right join inputs (once the projections have been removed)
     * @param projExprs List of projection expressions to be created
     * @param fieldNames List of names/types of the projection fields
     */
    private static void createProjectExprs(
        ProjectRel projRel,
        RelNode joinChild,
        int adjustmentAmount,
        RexBuilder rexBuilder,
        RelDataTypeField [] joinChildrenFields,
        List<RexNode> projExprs,
        List<Pair<String, RelDataType>> fieldNames)
    {
        List<RelDataTypeField> childFields =
            joinChild.getRowType().getFieldList();
        if (projRel != null) {
            RexNode [] origProjExprs = projRel.getProjectExps();
            RelDataTypeField [] projFields = projRel.getRowType().getFields();
            int nChildFields = childFields.size();
            int [] adjustments = new int[nChildFields];
            for (int i = 0; i < nChildFields; i++) {
                adjustments[i] = adjustmentAmount;
            }
            for (int i = 0; i < origProjExprs.length; i++) {
                RexNode expr;
                if (adjustmentAmount == 0) {
                    expr = origProjExprs[i];
                } else {
                    // shift the references by the adjustment amount
                    expr =
                        origProjExprs[i].accept(
                            new RelOptUtil.RexInputConverter(
                                rexBuilder,
                                childFields,
                                Arrays.asList(joinChildrenFields),
                                adjustments));
                }
                projExprs.add(expr);
                fieldNames.add(
                    Pair.of(
                        projFields[i].getName(),
                        expr.getType()));
            }
        } else {
            // no projection; just create references to the inputs
            for (int i = 0; i < childFields.size(); i++) {
                final RelDataTypeField field = childFields.get(i);
                RexNode expr =
                    rexBuilder.makeInputRef(
                        field.getType(),
                        i + adjustmentAmount);
                projExprs.add(expr);
                fieldNames.add(
                    Pair.of(
                        field.getName(),
                        expr.getType()));
            }
        }
    }
}

// End PullUpProjectsAboveJoinRule.java

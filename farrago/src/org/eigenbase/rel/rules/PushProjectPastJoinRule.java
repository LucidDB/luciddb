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
package org.eigenbase.rel.rules;

import java.util.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;
import org.eigenbase.sql.*;


/**
 * PushProjectPastJoinRule implements the rule for pushing a projection past a
 * join by splitting the projection into a projection on top of each child of
 * the join.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushProjectPastJoinRule
    extends RelOptRule
{

    //~ Instance fields --------------------------------------------------------

    /**
     * Expressions that should be preserved in the projection
     */
    private Set<SqlOperator> preserveExprs;

    //~ Constructors -----------------------------------------------------------

    //  ~ Constructors ---------------------------------------------------------

    public PushProjectPastJoinRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(JoinRel.class, null)
                }));
        this.preserveExprs = Collections.EMPTY_SET;
    }

    public PushProjectPastJoinRule(Set<SqlOperator> preserveExprs)
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(JoinRel.class, null)
                }));
        this.preserveExprs = preserveExprs;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel origProj = (ProjectRel) call.rels[0];
        JoinRel joinRel = (JoinRel) call.rels[1];

        RelDataTypeField [] joinFields = joinRel.getRowType().getFields();
        RelDataTypeField [] leftFields =
            joinRel.getLeft().getRowType().getFields();
        RelDataTypeField [] rightFields =
            joinRel.getRight().getRowType().getFields();
        int nFieldsLeft = leftFields.length;
        int nFieldsRight = rightFields.length;
        int nTotalFields = nFieldsLeft + nFieldsRight;

        RexNode [] origProjFields = origProj.getChildExps();

        // locate all fields referenced in the projection and join condition
        BitSet projRefs = new BitSet(nTotalFields);
        BitSet leftBitmap = new BitSet(nFieldsLeft);
        BitSet rightBitmap = new BitSet(nFieldsRight);
        RelOptUtil.setRexInputBitmap(leftBitmap, 0, nFieldsLeft);
        RelOptUtil.setRexInputBitmap(rightBitmap, nFieldsLeft, nTotalFields);
        List<RexNode> preserveLeft = new ArrayList<RexNode>();
        List<RexNode> preserveRight = new ArrayList<RexNode>();

        PushProjector pushProject = new PushProjector();
        pushProject.locateAllRefs(
            origProjFields,
            joinRel.getCondition(),
            projRefs,
            leftBitmap,
            rightBitmap,
            preserveExprs,
            preserveLeft,
            preserveRight);

        // if all fields are being projected and there are no special
        // expressions, no point in proceeding any further
        if ((projRefs.cardinality() == nTotalFields)
            && (preserveLeft.size() == 0)
            && (preserveRight.size() == 0)) {
            return;
        }

        // determine how many fields on projected from the left vs right
        int nLeftProject = 0;
        for (int bit = projRefs.nextSetBit(0);
            (bit >= 0)
            && (bit < nFieldsLeft); bit = projRefs.nextSetBit(bit + 1)) {
            nLeftProject++;
        }
        int nRightProject = projRefs.cardinality() - nLeftProject;

        // create left and right projections, projecting only those
        // fields referenced on each side
        RexBuilder rexBuilder = origProj.getCluster().getRexBuilder();

        // if nothing is projected from join child, arbitrarily project
        // the first column unless there is only one column in the child;
        // this is necessary since Fennel doesn't handle 0-column projections
        if ((nLeftProject == 0) && (preserveLeft.size() == 0)) {
            if (nFieldsLeft == 1 && preserveRight.size() == 0) {
                return;
            }
            projRefs.set(0);
            nLeftProject = 1;
        }
        RelNode leftProjRel =
            pushProject.createProjectRefsAndExprs(
                rexBuilder,
                projRefs,
                leftFields,
                null,
                0,
                nLeftProject,
                preserveLeft,
                joinRel.getLeft());
        if ((nRightProject == 0) && (preserveRight.size() == 0)) {
            if (nFieldsRight == 1 && preserveLeft.size() == 0) {
                return;
            }
            projRefs.set(nFieldsLeft);
            nRightProject = 1;
        }
        RelNode rightProjRel =
            pushProject.createProjectRefsAndExprs(
                rexBuilder,
                projRefs,
                rightFields,
                joinFields,
                nFieldsLeft,
                nRightProject,
                preserveRight,
                joinRel.getRight());

        // convert the join condition to reference the projected columns
        RexNode newJoinFilter = null;

        int [] adjustments =
            pushProject.getAdjustments(
                joinFields,
                projRefs,
                nFieldsLeft,
                preserveLeft.size());
        if (joinRel.getCondition() != null) {
            RelDataTypeField [] projLeftFields =
                leftProjRel.getRowType().getFields();
            RelDataTypeField [] projRightFields =
                rightProjRel.getRowType().getFields();
            RelDataTypeField [] projJoinFields =
                new RelDataTypeField[projLeftFields.length
                + projRightFields.length];
            System.arraycopy(
                leftProjRel.getRowType().getFields(),
                0,
                projJoinFields,
                0,
                projLeftFields.length);
            System.arraycopy(
                rightProjRel.getRowType().getFields(),
                0,
                projJoinFields,
                projLeftFields.length,
                projRightFields.length);
            newJoinFilter =
                pushProject.convertRefsAndExprs(
                    rexBuilder,
                    joinRel.getCondition(),
                    joinFields,
                    adjustments,
                    preserveLeft,
                    nLeftProject,
                    preserveRight,
                    nLeftProject + preserveLeft.size() + nRightProject,
                    projJoinFields);
        }

        // create a new joinrel with the projected children
        JoinRel newJoinRel =
            new JoinRel(
                joinRel.getCluster(),
                leftProjRel,
                rightProjRel,
                newJoinFilter,
                joinRel.getJoinType(),
                Collections.EMPTY_SET,
                joinRel.isSemiJoinDone(),
                joinRel.isMultiJoinDone());

        // put the original project on top of the join, converting it to
        // reference the modified projection list
        ProjectRel topProject =
            pushProject.createNewProject(
                origProj,
                joinFields,
                adjustments,
                preserveLeft,
                nLeftProject,
                preserveRight,
                nLeftProject + preserveLeft.size() + nRightProject,
                rexBuilder,
                newJoinRel);

        call.transformTo(topProject);
    }
}

// End PushProjectPastJoinRule.java

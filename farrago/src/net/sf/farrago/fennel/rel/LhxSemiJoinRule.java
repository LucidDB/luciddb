/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * LcsSemiJoinRule implements the rule for converting a join (which evaluates a
 * semi join) expression into the a hash semi join.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxSemiJoinRule
    extends RelOptRule
{
    public static final LhxSemiJoinRule instance =
        new LhxSemiJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LhxSemiJoinRule.
     */
    private LhxSemiJoinRule()
    {
        super(
            new RelOptRuleOperand(
                ProjectRel.class,
                new RelOptRuleOperand(
                    JoinRel.class,
                    new RelOptRuleOperand(RelNode.class, ANY),
                    new RelOptRuleOperand(RelNode.class, ANY))));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        ProjectRel projRel = (ProjectRel) call.rels[0];
        JoinRel joinRel = (JoinRel) call.rels[1];
        RelNode leftRel = call.rels[2];
        RelNode rightRel = call.rels[3];

        if (joinRel.getJoinType() != JoinRelType.INNER) {
            // This rule only applies to inner joins.
            return;
        }

        if (!(leftRel instanceof AggregateRel)
            && !(rightRel instanceof AggregateRel))
        {
            return;
        }

        RexNode residualCondition = null;

        // determine if we have a valid join condition
        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();

        // splitJoinCondition does not need to be concerned about null
        // filtering property of a join key position: null values will not
        // match for any key pos.
        residualCondition =
            RelOptUtil.splitJoinCondition(
                Collections.<RelDataTypeField>emptyList(),
                leftRel,
                rightRel,
                joinRel.getCondition(),
                leftJoinKeys,
                rightJoinKeys,
                null,
                null);

        // valid join keys should only reference input fields directly, i.e.
        // no residualCondition
        if ((leftJoinKeys.size() == 0)
            || (rightJoinKeys.size() == 0)
            || (residualCondition != null))
        {
            return;
        }

        // First determine which side is producing the distinct
        // with the other side projected after the join
        RexNode [] projExprs = projRel.getProjectExps();
        int leftFieldCount = leftRel.getRowType().getFieldCount();
        int rightFieldCount = rightRel.getRowType().getFieldCount();

        BitSet projRefs = new BitSet(leftFieldCount + rightFieldCount);

        RelOptUtil.InputFinder inputFinder =
            new RelOptUtil.InputFinder(projRefs);

        inputFinder.apply(projExprs, null);

        int leftRefCount = 0;
        int rightRefCount = 0;
        for (
            int bit = projRefs.nextSetBit(0);
            bit >= 0;
            bit = projRefs.nextSetBit(bit + 1))
        {
            if (bit >= leftFieldCount) {
                rightRefCount++;
            } else {
                leftRefCount++;
            }
        }

        boolean outputLeft;
        AggregateRel aggRel;
        if (rightRefCount == 0) {
            outputLeft = true;
            if (rightRel instanceof AggregateRel) {
                aggRel = (AggregateRel) rightRel;
            } else {
                return;
            }
        } else if (leftRefCount == 0) {
            outputLeft = false;
            if (leftRel instanceof AggregateRel) {
                aggRel = (AggregateRel) leftRel;
            } else {
                return;
            }
        } else {
            return;
        }

        if (aggRel.getAggCallList().size() != 0) {
            // not guaranteed to be distinct
            return;
        }

        // then check if aggregate(distinct) keys are covered by the join keys
        int numGroupByKeys = aggRel.getGroupCount();
        List<RexNode> aggJoinKeys = null;

        if (outputLeft) {
            aggJoinKeys = rightJoinKeys;
        } else {
            aggJoinKeys = leftJoinKeys;
        }

        BitSet inputRefBitset =
            new BitSet(aggRel.getChild().getRowType().getFieldCount());

        RelOptUtil.InputFinder aggJoinInputFinder =
            new RelOptUtil.InputFinder(inputRefBitset);

        for (RexNode expr : aggJoinKeys) {
            expr.accept(aggJoinInputFinder);
        }

        // group by key positions are 0 ...numGroupByKeys -1
        for (int i = 0; i < numGroupByKeys; i++) {
            if (inputRefBitset.nextSetBit(i) != i) {
                return;
            }
        }

        // now we can replace the original Join(A, distinct(B)) with
        // LeftSemiJoin(A, B), or Join(distinct(A), B) with RightSemiJoin(A, B)
        List<Integer> outputProj = new ArrayList<Integer>();

        RelNode [] inputRels;

        if (outputLeft) {
            inputRels = new RelNode[] { leftRel, aggRel.getChild() };
        } else {
            inputRels = new RelNode[] { aggRel.getChild(), rightRel };
        }

        List<Integer> newLeftJoinKeyPos = new ArrayList<Integer>();
        List<Integer> newRightJoinKeyPos = new ArrayList<Integer>();
        List<Integer> filterNulls = new ArrayList<Integer>();

        RelOptUtil.projectJoinInputs(
            inputRels,
            leftJoinKeys,
            rightJoinKeys,
            0,
            newLeftJoinKeyPos,
            newRightJoinKeyPos,
            outputProj);

        // the new leftRel and new rightRel, after projection is added.
        RelNode newLeftRel = inputRels[0];
        RelNode newRightRel = inputRels[1];

        RelNode fennelLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                newLeftRel);

        if (fennelLeft == null) {
            return;
        }

        RelNode fennelRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                newRightRel);

        if (fennelRight == null) {
            return;
        }

        Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);

        // Derive cardinality of RHS join keys.
        Double cndBuildKey;
        BitSet joinKeyMap = new BitSet();

        for (int i = 0; i < newRightJoinKeyPos.size(); i++) {
            joinKeyMap.set(newRightJoinKeyPos.get(i));

            // null values will not match for any join key pos
            filterNulls.add(i);
        }

        cndBuildKey =
            RelMetadataQuery.getPopulationSize(
                fennelRight,
                joinKeyMap);

        if (cndBuildKey == null) {
            cndBuildKey = -1.0;
        }

        boolean isSetop = false;
        List<String> newJoinOutputNames;
        LhxJoinRelType joinType;

        if (outputLeft) {
            newJoinOutputNames =
                RelOptUtil.getFieldNameList(leftRel.getRowType());
            joinType = LhxJoinRelType.LEFTSEMI;
        } else {
            newJoinOutputNames =
                RelOptUtil.getFieldNameList(rightRel.getRowType());
            joinType = LhxJoinRelType.RIGHTSEMI;
        }

        RelNode rel =
            new LhxJoinRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                joinType,
                isSetop,
                newLeftJoinKeyPos,
                newRightJoinKeyPos,
                filterNulls,
                newJoinOutputNames,
                numBuildRows.longValue(),
                cndBuildKey.longValue());

        RexNode [] newProjExprs;

        if (!outputLeft) {
            // right semi join. need to convert input to the project to
            // reference the output of the new join

            RelDataTypeField [] projInputFields =
                joinRel.getRowType().getFields();
            RelDataTypeField [] newProjInputFields =
                rel.getRowType().getFields();
            int [] adjustments = new int[projInputFields.length];
            assert (projInputFields.length
                == (leftFieldCount + rightFieldCount));

            int i;
            for (i = 0; i < leftFieldCount; i++) {
                adjustments[i] = 0;
            }

            for (; i < projInputFields.length; i++) {
                adjustments[i] = -leftFieldCount;
            }

            // right semi join. need to convert input to the project to
            // reference the output of the join
            int projLength = projExprs.length;
            newProjExprs = new RexNode[projLength];
            for (int j = 0; j < projLength; j++) {
                newProjExprs[j] =
                    projExprs[j].accept(
                        new RelOptUtil.RexInputConverter(
                            rel.getCluster().getRexBuilder(),
                            projInputFields,
                            newProjInputFields,
                            adjustments));
            }
        } else {
            // This is a left semi join, so only fields from the left side is
            // projected. There is no need to project the new output
            // (left+key+right+key) to the original join output(left+right)
            // before applying the original project.
            newProjExprs = projExprs;
        }

        rel =
            CalcRel.createProject(
                rel,
                newProjExprs,
                RelOptUtil.getFieldNames(projRel.getRowType()));

        call.transformTo(rel);
    }
}

// End LhxSemiJoinRule.java

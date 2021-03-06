/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/
package net.sf.farrago.fennel.rel;

import java.util.*;

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * FennelCartesianJoinRule is a rule for converting an INNER JoinRel with no
 * join condition into a FennelCartesianProductRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelCartesianJoinRule
    extends RelOptRule
{
    public static final FennelCartesianJoinRule instance =
        new FennelCartesianJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelCartesianJoinRule.
     */
    private FennelCartesianJoinRule()
    {
        super(
            new RelOptRuleOperand(
                JoinRel.class,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(RelNode.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        JoinRel joinRel = (JoinRel) call.rels[0];

        if (!joinRel.getSystemFieldList().isEmpty()) {
            // Cannot convert joins that generate system fields.
            return;
        }

        RelNode leftRel = call.rels[1];
        RelNode rightRel = call.rels[2];

        /*
         * Joins that can use CartesianProduct will have only TRUE condition
         * in JoinRel. Any other join conditions have to be extracted out
         * already. This implies that only ON TRUE condition is suported for
         * LeftOuterJoins or RightOuterJoins.
         */
        JoinRelType joinType = joinRel.getJoinType();
        boolean joinTypeFeasible = !(joinType == JoinRelType.FULL);
        boolean swapped = false;
        if (joinType == JoinRelType.RIGHT) {
            swapped = true;
            RelNode tmp = leftRel;
            leftRel = rightRel;
            rightRel = tmp;
            joinType = JoinRelType.LEFT;
        }

        /*
         * CartesianProduct relies on a post filter to do the join filtering.
         * If the join condition is not extracted to a post filter(and is still
         * in JoinRel), CartesianProduct can not be used.
         */
        boolean joinConditionFeasible =
            joinRel.getCondition().equals(
                joinRel.getCluster().getRexBuilder().makeLiteral(true));

        if (!joinTypeFeasible || !joinConditionFeasible) {
            return;
        }

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        leftRel =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                leftRel);
        if (leftRel == null) {
            return;
        }

        rightRel =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                rightRel);
        if (rightRel == null) {
            return;
        }

        // see if it makes sense to buffer the existing RHS; if not, try
        // the LHS, swapping the join operands if it does make sense to buffer
        // the LHS; but only if the join isn't a left outer join (since we
        // can't do cartesian right outer joins)
        FennelBufferRel bufRel = FennelRelUtil.bufferRight(leftRel, rightRel);
        if (bufRel != null) {
            rightRel = bufRel;
        } else if (joinType != JoinRelType.LEFT) {
            bufRel = FennelRelUtil.bufferRight(rightRel, leftRel);
            if (bufRel != null) {
                swapped = true;
                leftRel = rightRel;
                rightRel = bufRel;
            }
        }

        RelDataType joinRowType;
        if (swapped) {
            joinRowType =
                JoinRel.deriveJoinRowType(
                    leftRel.getRowType(),
                    rightRel.getRowType(),
                    joinType,
                    joinRel.getCluster().getTypeFactory(),
                    null,
                    Collections.<RelDataTypeField>emptyList());
        } else {
            joinRowType = joinRel.getRowType();
        }
        FennelCartesianProductRel productRel =
            new FennelCartesianProductRel(
                joinRel.getCluster(),
                leftRel,
                rightRel,
                joinType,
                RelOptUtil.getFieldNameList(joinRowType));

        RelNode newRel;
        if (swapped) {
            // if the join inputs were swapped, create a CalcRel on top of
            // the new cartesian join that reflects the original join
            // projection
            final RexNode [] exps =
                RelOptUtil.createSwappedJoinExprs(
                    productRel,
                    joinRel,
                    true);
            final RexProgram program =
                RexProgram.create(
                    productRel.getRowType(),
                    exps,
                    null,
                    joinRel.getRowType(),
                    productRel.getCluster().getRexBuilder());
            newRel =
                new CalcRel(
                    productRel.getCluster(),
                    RelOptUtil.clone(joinRel.getTraits()),
                    productRel,
                    joinRel.getRowType(),
                    program,
                    Collections.<RelCollation>emptyList());
        } else {
            newRel = productRel;
        }
        call.transformTo(newRel);
    }
}

// End FennelCartesianJoinRule.java

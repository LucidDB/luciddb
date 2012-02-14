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
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.*;


/**
 * LhxJoinRule implements the planner rule for converting a JoinRel with join
 * condition into a LhxJoinRel (hash join).
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxJoinRule
    extends RelOptRule
{
    public static final LhxJoinRule instance =
        new LhxJoinRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LhxJoinRule.
     */
    private LhxJoinRule()
    {
        super(new RelOptRuleOperand(JoinRel.class, ANY));
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

        RelNode leftRel = joinRel.getLeft();
        RelNode rightRel = joinRel.getRight();
        RexNode nonEquiCondition = null;

        // determine if we have a valid join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();

        List<RexNode> leftJoinKeys = new ArrayList<RexNode>();
        List<RexNode> rightJoinKeys = new ArrayList<RexNode>();
        List<Integer> filterNulls = new ArrayList<Integer>();

        nonEquiCondition =
            RelOptUtil.splitJoinCondition(
                Collections.<RelDataTypeField>emptyList(),
                leftRel,
                rightRel,
                joinRel.getCondition(),
                leftJoinKeys,
                rightJoinKeys,
                filterNulls,
                null);

        if ((nonEquiCondition != null)
            && (joinRel.getJoinType() != JoinRelType.INNER))
        {
            // Cannot use hash outer join types if there're non-equi join
            // conditions.
            // Note this type of join cannot be implemented by cartesian
            // product either.
            return;
        }

        if (!joinRel.getVariablesStopped().isEmpty()) {
            return;
        }

        if (leftJoinKeys.size() == 0) {
            // should use cartesian product instead of hash join
            return;
        }

        List<Integer> outputProj = new ArrayList<Integer>();

        RelNode [] inputRels = new RelNode[] { leftRel, rightRel };

        RelOptUtil.projectJoinInputs(
            inputRels,
            leftJoinKeys,
            rightJoinKeys,
            0,
            leftKeys,
            rightKeys,
            outputProj);

        // the new leftRel and new rightRel, after projection is added.
        leftRel = inputRels[0];
        rightRel = inputRels[1];

        RelNode fennelLeft =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                leftRel);

        if (fennelLeft == null) {
            return;
        }

        RelNode fennelRight =
            mergeTraitsAndConvert(
                joinRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                rightRel);

        if (fennelRight == null) {
            return;
        }

        Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);
        if (numBuildRows == null) {
            numBuildRows = -1.0;
        }

        // Derive cardinality of RHS join keys.
        Double cndBuildKey;
        BitSet joinKeyMap = new BitSet();

        // since rightJoinKeys can be more than simply inputrefs
        // assume the cardinality of the key to be the cardinality of all
        // referenced fields.

        for (int i = 0; i < rightKeys.size(); i++) {
            joinKeyMap.set(rightKeys.get(i));
        }

        cndBuildKey =
            RelMetadataQuery.getPopulationSize(
                fennelRight,
                joinKeyMap);

        if (cndBuildKey == null) {
            cndBuildKey = -1.0;
        }

        boolean isSetop = false;

        // pass in null for the fieldNameList so proper names can be derived
        // when the left and right hand side have overlapping names
        RelNode rel =
            new LhxJoinRel(
                joinRel.getCluster(),
                fennelLeft,
                fennelRight,
                LhxJoinRelType.getLhxJoinType(joinRel.getJoinType()),
                isSetop,
                leftKeys,
                rightKeys,
                filterNulls,
                null,
                numBuildRows.longValue(),
                cndBuildKey.longValue());

        // Need to project the new output(left+key+right+key) to the original
        // join output(left+right).
        // The projection needs to happen before additional filtering since
        // filtering condition references the original output ordinals.
        rel = RelOptUtil.createProjectJoinRel(outputProj, rel);

        transformCall(call, rel, nonEquiCondition);
    }

    private void transformCall(
        RelOptRuleCall call,
        RelNode rel,
        RexNode extraFilter)
    {
        if (extraFilter != null) {
            rel =
                new FilterRel(
                    rel.getCluster(),
                    rel,
                    extraFilter);
        }
        call.transformTo(rel);
    }
}

// End LhxJoinRule.java

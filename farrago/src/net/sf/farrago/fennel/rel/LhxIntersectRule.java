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


/**
 * LhxIntersectRule is a rule for transforming {@link IntersectRel} to {@link
 * LhxJoinRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxIntersectRule
    extends RelOptRule
{
    public static final LhxIntersectRule instance =
        new LhxIntersectRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LhxIntersectRule.
     */
    private LhxIntersectRule()
    {
        super(
            new RelOptRuleOperand(
                IntersectRel.class,
                ANY));
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
        IntersectRel intersectRel = (IntersectRel) call.rels[0];

        // TODO: intersect all
        assert (intersectRel.isDistinct());

        RelNode leftRel = intersectRel.getInputs()[0];
        List<String> newJoinOutputNames =
            RelOptUtil.getFieldNameList(leftRel.getRowType());

        // make up the join condition
        List<Integer> leftKeys = new ArrayList<Integer>();
        List<Integer> rightKeys = new ArrayList<Integer>();

        // an empty array means no filtering of null values
        // i.e. a null value is considered to match another null value
        List<Integer> filterNulls = new ArrayList<Integer>();

        for (int i = 0; i < leftRel.getRowType().getFieldCount(); i++) {
            leftKeys.add(i);
            rightKeys.add(i);
        }

        for (
            int inputNo = 1;
            inputNo < intersectRel.getInputs().length;
            inputNo++)
        {
            // perform pair-wise intersect
            RelNode rightRel = intersectRel.getInputs()[inputNo];

            // TODO: casting
            assert (leftRel.getRowType() == rightRel.getRowType());

            RelNode fennelLeft =
                mergeTraitsAndConvert(
                    intersectRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    leftRel);

            if (fennelLeft == null) {
                return;
            }

            RelNode fennelRight =
                mergeTraitsAndConvert(
                    intersectRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    rightRel);

            if (fennelRight == null) {
                return;
            }

            Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);

            // Derive cardinality of build side(RHS) join keys.
            Double cndBuildKey;
            BitSet joinKeyMap = new BitSet();

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

            boolean isSetop = true;

            leftRel =
                new LhxJoinRel(
                    intersectRel.getCluster(),
                    fennelLeft,
                    fennelRight,
                    LhxJoinRelType.LEFTSEMI,
                    isSetop,
                    leftKeys,
                    rightKeys,
                    filterNulls,
                    newJoinOutputNames,
                    numBuildRows.longValue(),
                    cndBuildKey.longValue());
        }
        call.transformTo(leftRel);
    }
}

// End LhxIntersectRule.java

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
 * LhxMinusRule is a rule for transforming {@link MinusRel} to {@link
 * LhxJoinRel}.
 *
 * @author Rushan Chen
 * @version $Id$
 */
public class LhxMinusRule
    extends RelOptRule
{
    public static final LhxMinusRule instance = new LhxMinusRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LhxMinusRule.
     */
    private LhxMinusRule()
    {
        super(
            new RelOptRuleOperand(
                MinusRel.class,
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
        MinusRel minusRel = (MinusRel) call.rels[0];

        // TODO: minus all
        assert (minusRel.isDistinct());

        // implement minus as a "right anti" join;
        // switch the input sides here.
        RelNode leftRel = minusRel.getInputs()[0];
        List<String> newJoinOutputNames = new ArrayList<String>();
        newJoinOutputNames.addAll(
            RelOptUtil.getFieldNameList(leftRel.getRowType()));

        // make up the condition
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
            inputNo < minusRel.getInputs().length;
            inputNo++)
        {
            // perform pair-wise minus
            RelNode rightRel = minusRel.getInputs()[inputNo];

            // TODO: casting
            assert (leftRel.getRowType() == rightRel.getRowType());

            RelNode fennelLeft =
                mergeTraitsAndConvert(
                    minusRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    leftRel);

            if (fennelLeft == null) {
                return;
            }

            RelNode fennelRight =
                mergeTraitsAndConvert(
                    minusRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    rightRel);

            if (fennelRight == null) {
                return;
            }

            Double numBuildRows = RelMetadataQuery.getRowCount(fennelRight);

            // implement minus as a "right anti" join;
            // derive cardinality of builde side(LHS) join keys.
            Double cndBuildKey;
            BitSet joinKeyMap = new BitSet();

            for (int i = 0; i < leftKeys.size(); i++) {
                joinKeyMap.set(leftKeys.get(i));
            }

            cndBuildKey =
                RelMetadataQuery.getPopulationSize(
                    fennelLeft,
                    joinKeyMap);

            if (cndBuildKey == null) {
                cndBuildKey = -1.0;
            }

            // implement minus as a "right anti" join;
            // switch the input sides here.
            boolean isSetop = true;
            leftRel =
                new LhxJoinRel(
                    minusRel.getCluster(),
                    fennelRight,
                    fennelLeft,
                    LhxJoinRelType.RIGHTANTI,
                    isSetop,
                    rightKeys,
                    leftKeys,
                    filterNulls,
                    newJoinOutputNames,
                    numBuildRows.longValue(),
                    cndBuildKey.longValue());
        }
        call.transformTo(leftRel);
    }
}

// End LhxMinusRule.java

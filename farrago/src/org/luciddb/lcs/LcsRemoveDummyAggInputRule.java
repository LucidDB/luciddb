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
package org.luciddb.lcs;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

/**
 * LcsRemoveDummyAggInputRule removes the dummy project(TRUE) added by
 * SqlToRelConverter.createAggImpl in the case of COUNT(*).  This
 * is needed in order for LcsIndexAggRule to fire.
 *
 * @author John Sichi
 * @version $Id$
 */
public class LcsRemoveDummyAggInputRule
    extends RelOptRule
{
    public static final LcsRemoveDummyAggInputRule instance =
        new LcsRemoveDummyAggInputRule(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        LcsRowScanRel.class, ANY))));

    public LcsRemoveDummyAggInputRule(
        RelOptRuleOperand operand)
    {
        super(operand);
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel agg = (AggregateRel) call.rels[0];
        ProjectRel project = (ProjectRel) call.rels[1];
        LcsRowScanRel rowScan = (LcsRowScanRel) call.rels[2];
        // We're looking for a single projection which is the constant TRUE.
        if (project.getProjectExps().length != 1) {
            return;
        }
        RexNode rexNode = project.getProjectExps()[0];
        if (!rexNode.isAlwaysTrue()) {
            return;
        }
        // And, we don't want any of the aggregates to reference it.
        for (AggregateCall aggCall : agg.getAggCallList()) {
            if (aggCall.getArgList().size() > 0) {
                return;
            }
        }
        AggregateRel newAgg = new AggregateRel(
            agg.getCluster(),
            rowScan,
            agg.getGroupCount(),
            agg.getAggCallList());
        call.transformTo(newAgg);
    }
}

// End LcsRemoveDummyAggInputRule.java

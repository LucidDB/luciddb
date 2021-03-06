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
package org.eigenbase.rel.rules;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;
import org.eigenbase.rex.*;


/**
 * PushFilterPastSetOpRule implements the rule for pushing a {@link FilterRel}
 * past a {@link SetOpRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class PushFilterPastSetOpRule
    extends RelOptRule
{
    public static final PushFilterPastSetOpRule instance =
        new PushFilterPastSetOpRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a PushFilterPastSetOpRule.
     */
    private PushFilterPastSetOpRule()
    {
        super(
            new RelOptRuleOperand(
                FilterRel.class,
                new RelOptRuleOperand(SetOpRel.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FilterRel filterRel = (FilterRel) call.rels[0];
        SetOpRel setOpRel = (SetOpRel) call.rels[1];

        RelNode [] setOpInputs = setOpRel.getInputs();
        int nSetOpInputs = setOpInputs.length;
        RelNode [] newSetOpInputs = new RelNode[nSetOpInputs];
        RelOptCluster cluster = setOpRel.getCluster();
        RexNode condition = filterRel.getCondition();

        // create filters on top of each setop child, modifying the filter
        // condition to reference each setop child
        RexBuilder rexBuilder = filterRel.getCluster().getRexBuilder();
        RelDataTypeField [] origFields = setOpRel.getRowType().getFields();
        int [] adjustments = new int[origFields.length];
        for (int i = 0; i < nSetOpInputs; i++) {
            RexNode newCondition =
                condition.accept(
                    new RelOptUtil.RexInputConverter(
                        rexBuilder,
                        origFields,
                        setOpInputs[i].getRowType().getFields(),
                        adjustments));
            newSetOpInputs[i] =
                new FilterRel(cluster, setOpInputs[i], newCondition);
        }

        // create a new setop whose children are the filters created above
        SetOpRel newSetOpRel =
            RelOptUtil.createNewSetOpRel(setOpRel, newSetOpInputs);

        call.transformTo(newSetOpRel);
    }
}

// End PushFilterPastSetOpRule.java

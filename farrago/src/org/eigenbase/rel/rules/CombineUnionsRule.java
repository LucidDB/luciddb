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


/**
 * CombineUnionsRule implements the rule for combining two non-distinct {@link
 * UnionRel}s into a single {@link UnionRel}.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class CombineUnionsRule
    extends RelOptRule
{
    public static final CombineUnionsRule instance =
        new CombineUnionsRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a CombineUnionsRule.
     */
    private CombineUnionsRule()
    {
        super(
            new RelOptRuleOperand(
                UnionRel.class,
                new RelOptRuleOperand(RelNode.class, ANY),
                new RelOptRuleOperand(RelNode.class, ANY)));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        UnionRel topUnionRel = (UnionRel) call.rels[0];
        UnionRel bottomUnionRel;

        // We want to combine the UnionRel that's in the second input first.
        // Hence, that's why the rule pattern matches on generic RelNodes
        // rather than explicit UnionRels.  By doing so, and firing this rule
        // in a bottom-up order, it allows us to only specify a single
        // pattern for this rule.
        if (call.rels[2] instanceof UnionRel) {
            bottomUnionRel = (UnionRel) call.rels[2];
        } else if (call.rels[1] instanceof UnionRel) {
            bottomUnionRel = (UnionRel) call.rels[1];
        } else {
            return;
        }

        // If distincts haven't been removed yet, defer invoking this rule
        if (topUnionRel.isDistinct() || bottomUnionRel.isDistinct()) {
            return;
        }

        // Combine the inputs from the bottom union with the other inputs from
        // the top union
        int nBottomUnionInputs = bottomUnionRel.getInputs().length;
        int nTopUnionInputs = topUnionRel.getInputs().length;
        RelNode [] unionInputs =
            new RelNode[nBottomUnionInputs + nTopUnionInputs - 1];
        if (call.rels[2] instanceof UnionRel) {
            assert (nTopUnionInputs == 2);
            unionInputs[0] = topUnionRel.getInput(0);
            System.arraycopy(
                bottomUnionRel.getInputs(),
                0,
                unionInputs,
                1,
                nBottomUnionInputs);
        } else {
            System.arraycopy(
                bottomUnionRel.getInputs(),
                0,
                unionInputs,
                0,
                nBottomUnionInputs);
            System.arraycopy(
                topUnionRel.getInputs(),
                1,
                unionInputs,
                nBottomUnionInputs,
                nTopUnionInputs - 1);
        }
        UnionRel newUnionRel =
            new UnionRel(
                topUnionRel.getCluster(),
                unionInputs,
                true);

        call.transformTo(newUnionRel);
    }
}

// End CombineUnionsRule.java

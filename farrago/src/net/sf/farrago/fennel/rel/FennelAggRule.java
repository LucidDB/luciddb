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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * FennelAggRule is a rule for transforming {@link AggregateRel} to {@link
 * FennelAggRel}.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelAggRule
    extends RelOptRule
{
    public static final FennelAggRule instance =
        new FennelAggRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelAggRule.
     */
    private FennelAggRule()
    {
        super(
            new RelOptRuleOperand(
                AggregateRel.class,
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
        AggregateRel aggRel = (AggregateRel) call.rels[0];

        for (AggregateCall aggCall : aggRel.getAggCallList()) {
            if (aggCall.isDistinct()) {
                // AGG(DISTINCT x) must be rewritten before this rule
                // can apply
                return;
            }

            // TODO jvs 5-Oct-2005:  find a better way of detecting
            // whether the aggregate function is one of the builtins supported
            // by Fennel; also test whether we can handle input datatype
            try {
                FennelRelUtil.lookupAggFunction(aggCall);
            } catch (IllegalArgumentException ex) {
                return;
            }
        }

        RelNode relInput = aggRel.getChild();
        RelNode fennelInput;

        if (aggRel.getGroupCount() > 0) {
            // add a FennelSortRel node beneath AggRel with sort keys
            // corresponding to the group by keys
            RelNode sortInput =
                mergeTraitsAndConvert(
                    aggRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    relInput);
            if (sortInput == null) {
                return;
            }

            Integer [] keyProjection = new Integer[aggRel.getGroupCount()];
            for (int i = 0; i < keyProjection.length; ++i) {
                keyProjection[i] = i;
            }

            boolean discardDuplicates = false;
            FennelSortRel fennelSortRel =
                new FennelSortRel(
                    aggRel.getCluster(),
                    sortInput,
                    keyProjection,
                    discardDuplicates);
            fennelInput = fennelSortRel;
        } else {
            fennelInput =
                mergeTraitsAndConvert(
                    aggRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    relInput);
            if (fennelInput == null) {
                return;
            }
        }

        FennelAggRel fennelAggRel =
            new FennelAggRel(
                aggRel.getCluster(),
                fennelInput,
                aggRel.getGroupCount(),
                aggRel.getAggCallList());
        call.transformTo(fennelAggRel);
    }
}

// End FennelAggRule.java

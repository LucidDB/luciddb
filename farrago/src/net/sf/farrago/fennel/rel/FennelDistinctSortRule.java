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
 * FennelDistinctSortRule is a rule for implementing DISTINCT via a Fennel sort.
 *
 * <p>A DISTINCT is recognized as an {@link AggregateRel} with no
 * {@link org.eigenbase.rel.AggregateCall}s and the same number
 * of outputs as inputs.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelDistinctSortRule
    extends RelOptRule
{
    public static final FennelDistinctSortRule instance =
        new FennelDistinctSortRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelDistinctSortRule.
     */
    private FennelDistinctSortRule()
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
        AggregateRel agg = (AggregateRel) call.rels[0];
        if (agg.getAggCallList().size() > 0) {
            return;
        }
        RelNode relInput = agg.getChild();
        int n = relInput.getRowType().getFieldList().size();
        if (agg.getGroupCount() < n) {
            return;
        }

        RelNode fennelInput =
            mergeTraitsAndConvert(
                agg.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        Integer [] keyProjection = FennelRelUtil.newIotaProjection(n);

        // REVIEW:  should cluster be from agg or relInput?
        boolean discardDuplicates = true;
        FennelSortRel sort =
            new FennelSortRel(
                agg.getCluster(),
                fennelInput,
                keyProjection,
                discardDuplicates);
        call.transformTo(sort);
    }
}

// End FennelDistinctSortRule.java

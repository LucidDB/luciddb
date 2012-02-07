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
import org.eigenbase.rel.metadata.*;
import org.eigenbase.relopt.*;


/**
 * FennelBufferCommonRelSubExprRule is a rule that places a Fennel buffering
 * node on top of  a common relational subexpression, provided it makes sense
 * to do so from a cost perspective.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class FennelBufferCommonRelSubExprRule
    extends CommonRelSubExprRule
{
    public static final FennelBufferCommonRelSubExprRule instance =
        new FennelBufferCommonRelSubExprRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * @deprecated use {@link #instance} instead
     */
    public FennelBufferCommonRelSubExprRule()
    {
        super(
            new RelOptRuleOperand(
                RelNode.class,
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
        RelNode rel = call.rels[0];
        if (rel instanceof FennelMultiUseBufferRel) {
            return;
        }

        // Compare the cost of not buffering vs buffering.
        //
        // Non Buffering Cost = getCumulativeCost(rel) * (# common subexprs)
        // Buffering Cost = getCumulativeCost(rel) +
        //    N * (cost of writing buffer) +
        //    (# common subexprs) * (cost of reading buffer)
        // Cost of reading and writing buffer = getNonCumulativeCost(rel)
        // N represents the overhead of caching, and is arbitrarily set to the
        // value 2.
        //
        // REVIEW zfong 3/19/09 - Should we avoid using buffering if the size
        // of the buffered result exceeds some threshold?
        FennelMultiUseBufferRel bufRel =
            new FennelMultiUseBufferRel(rel.getCluster(), rel, false);
        RelOptCost bufferCost = RelMetadataQuery.getNonCumulativeCost(bufRel);

        int nCommonSubExprs = call.getParents().size();
        RelOptCost commonSubExprCost = RelMetadataQuery.getCumulativeCost(rel);

        RelOptCost bufferPlanCost = bufferCost.multiplyBy(nCommonSubExprs + 2);
        RelOptCost nonBufferPlanCost =
            commonSubExprCost.multiplyBy(nCommonSubExprs - 1);
        if (nonBufferPlanCost.isLt(bufferPlanCost)) {
            return;
        }

        call.transformTo(bufRel);
    }
}

// End FennelBufferCommonRelSubExprRule.java

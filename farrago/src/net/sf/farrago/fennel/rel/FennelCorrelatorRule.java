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


/**
 * FennelCorrelatorRule is a rule to implement the join of two correlated
 * streams.
 *
 * @author Wael Chatila
 * @version $Id$
 * @since Feb 1, 2005
 */
public class FennelCorrelatorRule
    extends RelOptRule
{
    public static final FennelCorrelatorRule instance =
        new FennelCorrelatorRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelCorrelatorRule.
     */
    private FennelCorrelatorRule()
    {
        super(
            new RelOptRuleOperand(
                CorrelatorRel.class,
                ANY));
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    public void onMatch(RelOptRuleCall call)
    {
        CorrelatorRel correlatorRel = (CorrelatorRel) call.rels[0];
        if (correlatorRel.getJoinType() != JoinRelType.INNER) {
            // TODO: FennelPullCorrelatorRel could potentially also support
            // LEFT JOIN, but does not at present.
            return;
        }
        RelNode relLeftInput = correlatorRel.getLeft();
        RelNode fennelLeftInput =
            mergeTraitsAndConvert(
                correlatorRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relLeftInput);
        if (fennelLeftInput == null) {
            return;
        }

        RelNode relRightInput = correlatorRel.getRight();
        RelNode fennelRightInput =
            mergeTraitsAndConvert(
                correlatorRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relRightInput);
        if (fennelRightInput == null) {
            return;
        }

        FennelPullCorrelatorRel fennelPullCorrelatorRel =
            new FennelPullCorrelatorRel(
                correlatorRel.getCluster(),
                fennelLeftInput,
                fennelRightInput,
                new ArrayList<CorrelatorRel.Correlation>(
                    correlatorRel.getCorrelations()));
        call.transformTo(fennelPullCorrelatorRel);
    }
}

// End FennelCorrelatorRule.java

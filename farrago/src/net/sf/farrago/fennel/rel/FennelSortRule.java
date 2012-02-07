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
 * FennelSortRule is a rule for implementing SortRel via a Fennel sort.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelSortRule
    extends RelOptRule
{
    public static final FennelSortRule instance = new FennelSortRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FennelSortRule.
     */
    private FennelSortRule()
    {
        super(
            new RelOptRuleOperand(
                SortRel.class,
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
        SortRel sortRel = (SortRel) call.rels[0];
        RelNode relInput = sortRel.getChild();
        RelNode fennelInput =
            mergeTraitsAndConvert(
                sortRel.getTraits(),
                FennelRel.FENNEL_EXEC_CONVENTION,
                relInput);
        if (fennelInput == null) {
            return;
        }

        boolean discardDuplicates = false;
        FennelSortRel fennelSortRel =
            new FennelSortRel(
                sortRel.getCluster(),
                fennelInput,
                sortRel.getCollations(),
                discardDuplicates);
        call.transformTo(fennelSortRel);
    }
}

// End FennelSortRule.java

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
 * FennelRemoveRedundantSortRule removes instances of SortRel which are already
 * satisfied by the physical ordering produced by an underlying FennelRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
public class FennelRemoveRedundantSortRule
    extends RelOptRule
{
    //~ Constructors -----------------------------------------------------------

    public FennelRemoveRedundantSortRule()
    {
        super(
            new RelOptRuleOperand(
                FennelSortRel.class,
                new RelOptRuleOperand(FennelRel.class, ANY)));
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
        FennelSortRel sortRel = (FennelSortRel) call.rels[0];
        FennelRel inputRel = (FennelRel) call.rels[1];

        if (!isSortRedundant(sortRel, inputRel)) {
            return;
        }

        if (inputRel instanceof FennelSortRel) {
            RelNode newRel =
                mergeTraitsAndConvert(
                    sortRel.getTraits(),
                    FennelRel.FENNEL_EXEC_CONVENTION,
                    inputRel);
            if (newRel == null) {
                return;
            }
            call.transformTo(newRel);
        } else {
            // REVIEW: don't blindly eliminate sort without know what aspects of
            // the input we're relying on?
        }
    }

    public static boolean isSortRedundant(
        FennelSortRel sortRel,
        FennelRel inputRel)
    {
        if (sortRel.isDiscardDuplicates()) {
            // TODO:  once we can obtain the key for a RelNode, check
            // that
            return false;
        }

        RelFieldCollation [] inputCollationArray = inputRel.getCollations();
        RelFieldCollation [] outputCollationArray = sortRel.getCollations();
        if (outputCollationArray.length > inputCollationArray.length) {
            // no way input more specific order can be satisfied by less
            // specific
            return false;
        }

        List<RelFieldCollation> inputCollationList =
            Arrays.asList(inputCollationArray);
        List<RelFieldCollation> outputCollationList =
            Arrays.asList(outputCollationArray);
        if (outputCollationArray.length < inputCollationArray.length) {
            // truncate for prefix comparison
            inputCollationList =
                inputCollationList.subList(0, outputCollationArray.length);
        }
        return inputCollationList.equals(outputCollationList);
    }
}

// End FennelRemoveRedundantSortRule.java

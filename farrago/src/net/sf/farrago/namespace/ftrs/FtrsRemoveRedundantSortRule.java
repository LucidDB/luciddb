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
package net.sf.farrago.namespace.ftrs;

import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.relopt.*;


/**
 * FtrsRemoveRedundantSortRule removes instances of SortRel which are already
 * satisfied by the physical ordering produced by an underlying
 * FtrsIndexScanRel.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FtrsRemoveRedundantSortRule
    extends RelOptRule
{
    public static final FtrsRemoveRedundantSortRule instance =
        new FtrsRemoveRedundantSortRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a FtrsRemoveRedundantSortRule.
     */
    private FtrsRemoveRedundantSortRule()
    {
        super(
            new RelOptRuleOperand(
                FennelSortRel.class,
                new RelOptRuleOperand(FtrsIndexScanRel.class, ANY)));
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
        FtrsIndexScanRel scanRel = (FtrsIndexScanRel) call.rels[1];

        if (!FennelRemoveRedundantSortRule.isSortRedundant(sortRel, scanRel)) {
            return;
        }

        // make sure scan order is preserved, since now we're relying
        // on it
        FtrsIndexScanRel sortedScanRel =
            new FtrsIndexScanRel(
                scanRel.getCluster(),
                scanRel.ftrsTable,
                scanRel.index,
                scanRel.getConnection(),
                scanRel.projectedColumns,
                true);
        call.transformTo(sortedScanRel);
    }
}

// End FtrsRemoveRedundantSortRule.java

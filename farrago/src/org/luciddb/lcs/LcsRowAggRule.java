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

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

import org.luciddb.session.*;

/**
 * A rule for directly aggregating during row scans.  Currently
 * only COUNT(*) in isolation is supported.
 *
 * @author John Sichi
 * @version $Id$
 */
public class LcsRowAggRule
    extends RelOptRule
{
    public final static LcsRowAggRule instance =
        new LcsRowAggRule(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(
                    LcsRowScanRel.class)));

    public LcsRowAggRule(
        RelOptRuleOperand operand)
    {
        super(operand);
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = (AggregateRel) call.rels[0];
        if (aggRel.getGroupCount() > 0) {
            // GROUP BY is not supported yet
            return;
        }
        if (aggRel.getAggCallList().size() != 1) {
            return;
        }
        // If any agg call references any column of the input,
        // then we have something other than a single COUNT(*), in which case
        // the LDB-225 special case does not apply.
        for (AggregateCall aggCall : aggRel.getAggCallList()) {
            if (aggCall.getArgList().size() > 0) {
                return;
            }
        }
        LcsRowScanRel rowScanRel = (LcsRowScanRel) call.rels[1];
        Integer [] proj = new Integer[1];
        // Whatever we were counting does not matter, since it wasn't
        // referenced by the COUNT(*) agg; replace it with RID since
        // that's what Fennel expects.

        proj[0] =
            LucidDbOperatorTable.ldbInstance().getSpecialOpColumnId(
                LucidDbOperatorTable.lcsRidFunc);
        LcsRowAggRel rowAggRel =
            new LcsRowAggRel(
                rowScanRel.getCluster(),
                rowScanRel.getInputs(),
                rowScanRel.getLcsTable(),
                rowScanRel.getClusteredIndexes(),
                rowScanRel.getConnection(),
                proj,
                rowScanRel.isFullScan(),
                rowScanRel.getResidualColumns(),
                rowScanRel.getInputSelectivity());
        call.transformTo(rowAggRel);
    }
}

// End LcsRowAggRule.java

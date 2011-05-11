/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
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

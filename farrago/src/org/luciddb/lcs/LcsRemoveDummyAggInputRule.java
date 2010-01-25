/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2009-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2009-2009 LucidEra, Inc.
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;

/**
 * LcsRemoveDummyAggInputRule removes the dummy project(TRUE) added by
 * SqlToRelConverter.createAggImpl in the case of COUNT(*).  This
 * is needed in order for LcsIndexAggRule to fire.
 *
 * @author John Sichi
 * @version $Id$
 */
public class LcsRemoveDummyAggInputRule
    extends RelOptRule
{
    public static final LcsRemoveDummyAggInputRule instance =
        new LcsRemoveDummyAggInputRule(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(
                    ProjectRel.class,
                    new RelOptRuleOperand(
                        LcsRowScanRel.class, ANY))));

    public LcsRemoveDummyAggInputRule(
        RelOptRuleOperand operand)
    {
        super(operand);
    }

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel agg = (AggregateRel) call.rels[0];
        ProjectRel project = (ProjectRel) call.rels[1];
        LcsRowScanRel rowScan = (LcsRowScanRel) call.rels[2];
        // We're looking for a single projection which is the constant TRUE.
        if (project.getProjectExps().length != 1) {
            return;
        }
        RexNode rexNode = project.getProjectExps()[0];
        if (!rexNode.isAlwaysTrue()) {
            return;
        }
        // And, we don't want any of the aggregates to reference it.
        for (AggregateCall aggCall : agg.getAggCallList()) {
            if (aggCall.getArgList().size() > 0) {
                return;
            }
        }
        AggregateRel newAgg = new AggregateRel(
            agg.getCluster(),
            rowScan,
            agg.getGroupCount(),
            agg.getAggCallList());
        call.transformTo(newAgg);
    }
}

// End LcsRemoveDummyAggInputRule.java

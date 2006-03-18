/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

package com.lucidera.lcs;

import java.util.*;

import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;
import org.eigenbase.reltype.*;


/**
 * LcsIndexSemiJoinRule implements the rule for converting a semijoin
 * expression into the actual operations used to execute the semijoin.
 * Specfically,
 *
 * <p>SemiJoinRel(LcsRowScanRel, D) ->
 *      LcsRowScanRel(
 *          LcsIndexMergeRel(
 *              LcsIndexSearchRel(
 *                  LcsFennelSortRel
 *                      (ProjectRel(D)))))
 * 
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsIndexSemiJoinRule extends RelOptRule
{
//  ~ Constructors ----------------------------------------------------------

    public LcsIndexSemiJoinRule()
    {
        super(new RelOptRuleOperand(
                SemiJoinRel.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(LcsRowScanRel.class, null),
                    new RelOptRuleOperand(RelNode.class, null)
                }));
    }

    //~ Methods ---------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SemiJoinRel semiJoin = (SemiJoinRel) call.rels[0];
        LcsRowScanRel origRowScan = (LcsRowScanRel) call.rels[1];
        RelNode rightRel = call.rels[2];
        
        int rightOrdinal = semiJoin.getRightOrdinal();
        Integer projOrdinals[] = new Integer [] { rightOrdinal };
        final RelDataTypeField rightField =
            origRowScan.getRowType().getFields()[rightOrdinal];
        
        // find a matching index on the row scan table
        // TODO - add a considerIndex to try each index
        Iterator iter = 
            origRowScan.getIndexGuide().getUnclusteredIndexes().iterator();
        while (iter.hasNext()) {
            FemLocalIndex index = (FemLocalIndex) iter.next();

            considerIndex(
                semiJoin, index, origRowScan, rightRel, rightField,
                rightOrdinal, projOrdinals, call);
        }
    }

    private void considerIndex(
        SemiJoinRel semiJoin,
        FemLocalIndex index,
        LcsRowScanRel origRowScan,
        RelNode rightRel,
        RelDataTypeField rightField,
        int rightOrdinal,
        Integer [] projOrdinals,
        RelOptRuleCall call)
    {
        if (!origRowScan.getIndexGuide().testIndexCoverage(
            index, projOrdinals))
        {
            return;
        }

        // create a projection on the join column from the right input
        RexBuilder rexBuilder = rightRel.getCluster().getRexBuilder();
        RexNode [] projExps = 
            new RexNode [] { 
                rexBuilder.makeInputRef(rightField.getType(), rightOrdinal)
                };
        String [] fieldNames = new String [] { rightField.getName() };
        ProjectRel projectRel =
            (ProjectRel) CalcRel.createProject(
                rightRel, projExps, fieldNames);
        RelNode sortInput =
            mergeTraitsAndConvert(
                semiJoin.getTraits(), FennelRel.FENNEL_EXEC_CONVENTION,
                projectRel);

        // create a sort on the projection
        boolean discardDuplicates = true;
        FennelSortRel sort =
            new FennelSortRel(
                origRowScan.getCluster(),
                sortInput,
                projOrdinals,
                discardDuplicates);
                
        // create an index search on the left input's join column, i.e.,
        // the rowscan input; but first need to create an index scan
        LcsIndexScanRel indexScan =
            new LcsIndexScanRel(
                origRowScan.getCluster(),
                origRowScan.lcsTable,
                index,
                origRowScan.getConnection(),
                projOrdinals,
                false);
        
        // directives don't need to be passed into the index search
        // because we are doing an equijoin where the sort feeds in
        // the search values
        LcsIndexSearchRel indexSearch =
            new LcsIndexSearchRel(
                sort, indexScan, discardDuplicates, false, null, null, null,
                0, 0);
        
        // create a merge on top of the index search
        LcsIndexMergeRel merge = 
            new LcsIndexMergeRel(
                origRowScan.lcsTable, indexSearch, 0, 0, 0);
        
        // finally create the new row scan
        RelNode [] inputRels = new RelNode [] { merge };
        LcsRowScanRel newRowScan =
            new LcsRowScanRel(
                origRowScan.getCluster(),
                inputRels,
                origRowScan.lcsTable,
                origRowScan.clusteredIndexes,
                origRowScan.getConnection(),
                origRowScan.projectedColumns,
                origRowScan.isFullScan,
                origRowScan.hasExtraFilter);

        call.transformTo(newRowScan);
    }
}

// End LcsIndexSemiJoinRule.java

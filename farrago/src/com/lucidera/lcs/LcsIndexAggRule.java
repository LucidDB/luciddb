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

import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;

/**
 * A rule for directly aggregating off of an unclustered index scan.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexAggRule extends RelOptRule
{
    /**
     * The singletons
     * 
     * <p>TODO: handle CalcRel after sort order has been cleaned up
     */
    public final static LcsIndexAggRule instanceRenameRowScan =
        new LcsIndexAggRule(
            new RelOptRuleOperand(AggregateRel.class, 
                new RelOptRuleOperand[] {
                new RelOptRuleOperand(FennelRenameRel.class,
                    new RelOptRuleOperand[] {
                    new RelOptRuleOperand(LcsRowScanRel.class, 
                        RelOptRuleOperand.noOperands)
                })}), "rename row scan");

    public final static LcsIndexAggRule instanceRenameNormalizer =
        new LcsIndexAggRule(
            new RelOptRuleOperand(AggregateRel.class, 
                new RelOptRuleOperand[] {
                new RelOptRuleOperand(FennelRenameRel.class,
                    new RelOptRuleOperand[] {
                    new RelOptRuleOperand(LcsNormalizerRel.class, 
                        new RelOptRuleOperand[] {
                        new RelOptRuleOperand(LcsIndexOnlyScanRel.class, null)
                })})}), "rename normalizer");
    
    //~ Constructors ----------------------------------------------------------
    
    /**
     * Creates a new LcsIndexOnlyAccessRule object.
     */
    public LcsIndexAggRule(
        RelOptRuleOperand operand, String id)
    {
        super(operand);
        description = "LcsIndexAggRule: " + id;
    }
    
    //~ Methods ---------------------------------------------------------------
    
    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }
    
    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        AggregateRel aggRel = (AggregateRel) call.rels[0];
        FennelRenameRel renameRel = (FennelRenameRel) call.rels[1];
        LcsRowScanRel rowScan = null;
        LcsIndexOnlyScanRel indexOnlyScan = null;
        if (call.rels[2] instanceof LcsRowScanRel) {
            rowScan = (LcsRowScanRel) call.rels[2];
            // NOTE: Here we check for no inputs because RelOptRuleOperand 
            // seems to allow a row scan with inputs
            if (rowScan.getInputs().length > 0) {
                return;
            }
        } else {
            assert (call.rels[2] instanceof LcsNormalizerRel);
            assert (call.rels[3] instanceof LcsIndexOnlyScanRel);
            indexOnlyScan = (LcsIndexOnlyScanRel) call.rels[3];
            Integer[] proj = indexOnlyScan.getOutputProj();
            if (!projectionSatisfiesGroupBy(proj, aggRel.getGroupCount())) {
                return;
            }
        }
        
        if (indexOnlyScan == null) {
            // Try to convert a row scan into an index only scan. Find the 
            // thinnest index that satisfies the row scan projection and 
            // the aggregate's required sort order
            LcsIndexGuide indexGuide = rowScan.lcsTable.getIndexGuide();
            FemLocalIndex bestIndex = null;
            Integer[] bestProj = null;
            for (FemLocalIndex index : indexGuide.getUnclusteredIndexes()) {
                Integer[] proj = 
                    indexGuide.findIndexOnlyProjection(rowScan, index);
                if (projectionSatisfiesGroupBy(proj, aggRel.getGroupCount())){
                    int indexSize = index.getIndexedFeature().size();
                    if (bestIndex == null 
                        || bestIndex.getIndexedFeature().size() > indexSize) 
                    {
                        bestIndex = index;
                        bestProj = proj;
                    }
                }
            }
            if (bestIndex == null) {
                return;
            }
            indexOnlyScan = new LcsIndexOnlyScanRel(
                rowScan,
                bestIndex,
                bestProj);
        }

        RelDataType renameType = renameRel.getRowType();
        RelDataType indexType = indexOnlyScan.getRowType();
        String[] fieldNames = new String[indexType.getFieldCount()];
        for (int i = 0; i < fieldNames.length; i++) {
            RelDataType type = 
                (i < renameType.getFieldCount()) ? renameType : indexType;
            fieldNames[i] = type.getFields()[i].getName();
        }
        RelNode indexRename = new FennelRenameRel(
            renameRel.getCluster(),
            indexOnlyScan,
            fieldNames,
            renameRel.getTraits());

        RelNode indexAgg = new LcsIndexAggRel(
            aggRel.getCluster(), 
            indexRename, 
            aggRel.getGroupCount(), 
            aggRel.getAggCalls());


        call.transformTo(indexAgg);
    }
    
    /**
     * Determines whether a projection from an index scan can meet the 
     * group by requirements of an aggregate. The index scan columns are 
     * sorted in order, and the group by columns are required to be sorted 
     * in order, so the index scan projection should be a prefix of the 
     * index scan: 0, 1, 2, ... etc.
     * 
     * @param proj the projection from an index
     * @param groupCount the number of columns to be grouped. the columns 
     *   are assumed to be the prefix of input to the aggregate
     * @return
     */
    private boolean projectionSatisfiesGroupBy(Integer[] proj, int groupCount) 
    {
        if (proj == null) {
            return false;
        }
        assert (proj.length >= groupCount);
        for (int i = 0; i < groupCount; i++) {
            if (proj[i] != i) {
                return false;
            }
        }
        return true;
    }
}

// End LcsIndexOnlyAccessRule.java

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;

/**
 * A rule for modifying the input into a row scan to include a scan of the
 * deletion index.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsAddDeletionScanRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    public final static LcsAddDeletionScanRule instanceNoInputs =
        new LcsAddDeletionScanRule(
            new RelOptRuleOperand(
                LcsRowScanRelBase.class,
                RelOptRuleOperand.noOperands),
            "no inputs");
    
    public final static LcsAddDeletionScanRule instanceMinusInput =
        new LcsAddDeletionScanRule(
            new RelOptRuleOperand(
                LcsRowScanRelBase.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        LcsIndexMinusRel.class,
                        new RelOptRuleOperand [] {
                            new RelOptRuleOperand(RelNode.class, null),
                            new RelOptRuleOperand(LcsIndexSearchRel.class, null)
                })}),
            "minus input");
    
    public final static LcsAddDeletionScanRule instanceAnyInput =
        new LcsAddDeletionScanRule(
            new RelOptRuleOperand(
                LcsRowScanRelBase.class,
                new RelOptRuleOperand [] {
                    new RelOptRuleOperand(
                        RelNode.class,
                        null)
                }),
            "any input"); 
    
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsAddDeletionScanRule object.
     */
    public LcsAddDeletionScanRule(
        RelOptRuleOperand operand,
        String id)
    {
        super(operand);
        description = "LcsAddDeletionScanRule: " + id;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        // Determine if this rule has already been fired
        if (alreadyCalled(call)) {
            return;
        }
        
        // Modify the input to the scan to either scan the deletion index
        // (in the case of a full table scan) or to minus off the deletion
        // index (in the case of an index scan)
        LcsRowScanRelBase origRowScan = (LcsRowScanRelBase) call.rels[0];
        RelNode [] newInputs;
        RelNode [] origInputs = origRowScan.getInputs();
        if (origRowScan.isFullScan) {
            newInputs = new RelNode[origInputs.length + 1];
            System.arraycopy(origInputs, 0, newInputs, 1, origInputs.length);
            LcsIndexSearchRel delIndexScan =
                origRowScan.getIndexGuide().createDeletionIndexScan(
                    origRowScan,
                    origRowScan.lcsTable,
                    null,
                    null,
                    true);
            newInputs[0] = delIndexScan;
        } else {
            newInputs = new RelNode[origInputs.length];
            if (origInputs.length > 1) {
                System.arraycopy(
                    origInputs,
                    1,
                    newInputs,
                    1,
                    origInputs.length - 1);
            }
            newInputs[0] =
                origRowScan.getIndexGuide().createMinusOfDeletionIndex(
                    origRowScan,
                    origRowScan.lcsTable,
                    origInputs[0]);
        }
        RelNode newRowScan;
        if (origRowScan instanceof LcsRowScanRel) {
            newRowScan =
                new LcsRowScanRel(
                    origRowScan.getCluster(),
                    newInputs,
                    origRowScan.lcsTable,
                    origRowScan.clusteredIndexes,
                    origRowScan.getConnection(),
                    origRowScan.projectedColumns,
                    origRowScan.isFullScan,
                    origRowScan.hasResidualFilter,
                    origRowScan.residualColumns,
                    origRowScan.inputSelectivity);
        } else {
            LcsSamplingRowScanRel sampleRel =
                (LcsSamplingRowScanRel) origRowScan;
            newRowScan =
                new LcsSamplingRowScanRel(
                    sampleRel.getCluster(),
                    newInputs,
                    sampleRel.lcsTable,
                    sampleRel.clusteredIndexes,
                    sampleRel.getConnection(),
                    sampleRel.projectedColumns,
                    sampleRel.samplingParams);
        }

        call.transformTo(newRowScan);
    }
        
    /**
     * Determines if the deletion scan is already included in the row scan
     * input, or if this particular instance of the rule shouldn't be fired
     * in favor of another more specific rule instance.  The first is done by
     * examining the first input into the row scan and seeing if there's a
     * minus of a search of the deletion index or a search of the deletion
     * index.
     * 
     * @param call rule call
     * 
     * @return true if this should be fired
     */
    private boolean alreadyCalled(RelOptRuleCall call)
    {
        if (call.rels.length == 1) {
            RelNode rowScan = call.rels[0];
            if (rowScan.getInputs().length > 0) {
                // if the row scan really does have inputs, then don't fire
                // this instance of the rule
                return true;
            }
            return false;
        }
        RelNode relNode;
        if (call.rels[1] instanceof LcsIndexMinusRel) {
            if (call.rels.length == 2) {
                // if the rule pattern was the generic RelNode as opposed
                // to the explicit LcsIndexMinusRel pattern, skip firing
                // this instance of the rule so we can fire with the more
                // explicit pattern later
                return true;
            }
            relNode = call.rels[3];
        } else {
            relNode = call.rels[1];
        }
        if (relNode instanceof LcsIndexSearchRel) {
            LcsIndexSearchRel indexSearch = (LcsIndexSearchRel) relNode;
            if (indexSearch.index.getIndexedFeature().size() == 0) {
                return true;
            }
        }
        return false;
    }
}

// End LcsAddDeletionScanRule.java

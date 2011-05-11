/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.rex.*;


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
                LcsRowScanRelBase.class),
            "no inputs");

    public final static LcsAddDeletionScanRule instanceMinusInput =
        new LcsAddDeletionScanRule(
            new RelOptRuleOperand(
                LcsRowScanRelBase.class,
                new RelOptRuleOperand(
                    LcsIndexMinusRel.class,
                    new RelOptRuleOperand(RelNode.class, ANY),
                    new RelOptRuleOperand(LcsIndexSearchRel.class, ANY))),
            "minus input");

    public final static LcsAddDeletionScanRule instanceAnyInput =
        new LcsAddDeletionScanRule(
            new RelOptRuleOperand(
                LcsRowScanRelBase.class,
                new RelOptRuleOperand(RelNode.class, ANY)),
            "any input");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an LcsAddDeletionScanRule.
     *
     * @param operand Root operand, must not be null
     *
     * @param id Description of rule
     */
    public LcsAddDeletionScanRule(
        RelOptRuleOperand operand,
        String id)
    {
        super(operand, "LcsAddDeletionScanRule: " + id);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        FarragoPreparingStmt stmt =
            FennelRelUtil.getPreparingStmt(call.rels[0]);

        // For ALTER TABLE ADD COLUMN, we want to include deleted rows in the
        // new column.
        boolean alterTable = stmt.getSession().isReentrantAlterTableAddColumn();

        // Determine if this rule has already been fired
        if (alreadyCalled(call, alterTable)) {
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
            if (alterTable) {
                // Feed the row scan an empty set input instead of
                // the deletion index, so it will scan all rows,
                // both deleted and non-deleted.
                newInputs[0] =
                    new FennelValuesRel(
                        origRowScan.getCluster(),
                        delIndexScan.getRowType(),
                        new ArrayList<List<RexLiteral>>(),
                        true);
            } else {
                newInputs[0] = delIndexScan;
            }
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
                    origRowScan.residualColumns,
                    origRowScan.inputSelectivity);
        } else if (origRowScan instanceof LcsRowAggRel) {
            newRowScan =
                new LcsRowAggRel(
                    origRowScan.getCluster(),
                    newInputs,
                    origRowScan.lcsTable,
                    origRowScan.clusteredIndexes,
                    origRowScan.getConnection(),
                    origRowScan.projectedColumns,
                    origRowScan.isFullScan,
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
     * input, or if this particular instance of the rule shouldn't be fired in
     * favor of another more specific rule instance. The first is done by
     * examining the first input into the row scan and seeing if there's a minus
     * of a search of the deletion index or a search of the deletion index.
     *
     * @param call rule call
     * @param alterTable whether we are preparing ALTER TABLE ADD COLUMN
     *
     * @return true if this should be fired
     */
    private boolean alreadyCalled(RelOptRuleCall call, boolean alterTable)
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
        if (alterTable) {
            assert (relNode instanceof FennelValuesRel);
            return true;
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

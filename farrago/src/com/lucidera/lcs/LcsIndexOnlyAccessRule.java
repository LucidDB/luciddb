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

import java.util.*;

import net.sf.farrago.cwm.keysindexes.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/**
 * A rule for converting an LcsRowScanRel(LcsIndexSearchRel) to an
 * LcsIndexOnlyScanRel. The conversion can be applied if an index scan can
 * satisfy the columns of a row scan.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexOnlyAccessRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singletons
     */
    public static final LcsIndexOnlyAccessRule instanceSearch =
        new LcsIndexOnlyAccessRule(
            new RelOptRuleOperand(
                LcsRowScanRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(LcsIndexSearchRel.class, null)
                }),
            "with index search child");

    public static final LcsIndexOnlyAccessRule instanceMerge =
        new LcsIndexOnlyAccessRule(
            new RelOptRuleOperand(
                LcsRowScanRel.class,
                new RelOptRuleOperand[] {
                    new RelOptRuleOperand(
                        LcsIndexMergeRel.class,
                        new RelOptRuleOperand[] {
                            new RelOptRuleOperand(LcsIndexSearchRel.class,
                                null)
                        })
                }),
            "with merge child");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsIndexOnlyAccessRule object.
     */
    public LcsIndexOnlyAccessRule(
        RelOptRuleOperand operand,
        String id)
    {
        super(operand);
        description = "LcsIndexOnlyRule: " + id;
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
        LcsRowScanRel rowScan = (LcsRowScanRel) call.rels[0];
        LcsIndexSearchRel origIndexSearch = null;
        if (call.rels[1] instanceof LcsIndexSearchRel) {
            origIndexSearch = (LcsIndexSearchRel) call.rels[1];
        } else {
            assert (call.rels[1] instanceof LcsIndexMergeRel);
            assert (call.rels[2] instanceof LcsIndexSearchRel);
            origIndexSearch = (LcsIndexSearchRel) call.rels[2];
        }

        // ignore deletion indexes
        if (origIndexSearch.index.getIndexedFeature().size() == 0) {
            return;
        }

        // The original index was chosen because it could handle a filter
        // and hence should be an index search (not a full scan)
        assert (origIndexSearch.getChild() != null);

        // Search for a thin index only scan. The new scan must be
        // compatible with the original scan's input, and it must
        // cover the projections of the row scan.

        // If performing a single keyset search, then the index keys must
        // match exactly and we can only consider the original index.
        // Otherwise, consider widening the index for better column coverage.
        // The search keys (which must also be the prefix keys) cannot change
        // TODO: we might be able to expand a single keyset search if we
        // allow input key projections with them and remember to update null
        // input key projections
        FemLocalIndex origIndex = origIndexSearch.index;
        List<FemLocalIndex> candidateIndexes;
        if (origIndexSearch.isInputSingleKeyset()) {
            candidateIndexes = Collections.singletonList(origIndex);
        } else {
            assert (origIndexSearch.inputKeyProj != null);
            candidateIndexes = LcsIndexOptimizer.getUnclusteredIndexes(rowScan);
        }
        int nInputKeys = origIndexSearch.getInputKeyCount();
        assert (nInputKeys > 0);

        // first sort the indexes in key length
        TreeSet<FemLocalIndex> indexSet =
            new TreeSet<FemLocalIndex>(
                new LcsIndexOptimizer.IndexLengthComparator());

        indexSet.addAll(candidateIndexes);

        FemLocalIndex bestIndex = null;
        Integer [] bestProj = null;

        for (FemLocalIndex index : indexSet) {
            // Starting from the "thinnest" indexes
            if (samePrefix(origIndex, index, nInputKeys)) {
                Integer [] proj =
                    LcsIndexOptimizer.findIndexOnlyProjection(rowScan, index);

                if (proj != null) {
                    bestIndex = index;
                    bestProj = proj;
                    break;
                }
            }
        }

        if (bestIndex == null) {
            return;
        }

        LcsIndexOnlyScanRel indexOnlyScan =
            new LcsIndexOnlyScanRel(
                rowScan.getCluster(),
                origIndexSearch.getChild(),
                origIndexSearch,
                bestIndex,
                bestProj);

        RelNode normalizer =
            new LcsNormalizerRel(
                rowScan.getCluster(),
                indexOnlyScan);
        call.transformTo(normalizer);
    }

    /**
     * Determines whether two indexes have the same prefix, up to a specified
     * number of keys
     */
    private boolean samePrefix(FemLocalIndex a, FemLocalIndex b, int nKeys)
    {
        if (a == b) {
            return true;
        }
        List<CwmIndexedFeature> aCols = a.getIndexedFeature();
        List<CwmIndexedFeature> bCols = b.getIndexedFeature();
        if ((aCols.size() < nKeys) || (bCols.size() < nKeys)) {
            return false;
        }
        for (int i = 0; i < nKeys; i++) {
            if (aCols.get(i).getFeature() != bCols.get(i).getFeature()) {
                return false;
            }
        }
        return true;
    }
}

// End LcsIndexOnlyAccessRule.java

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


/**
 * A rule for directly aggregating off of an unclustered index scan.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexAggRule
    extends RelOptRule
{
    //~ Static fields/initializers ---------------------------------------------

    /**
     * The singletons
     *
     * <p>TODO: handle CalcRel after sort order has been cleaned up
     */
    public final static LcsIndexAggRule instanceRowScan =
        new LcsIndexAggRule(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(
                    LcsRowScanRel.class)),
            "row scan");

    public final static LcsIndexAggRule instanceNormalizer =
        new LcsIndexAggRule(
            new RelOptRuleOperand(
                AggregateRel.class,
                new RelOptRuleOperand(
                    LcsNormalizerRel.class,
                    new RelOptRuleOperand(
                        LcsIndexOnlyScanRel.class,
                        ANY))),
            "normalizer");

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates an LcsIndexAggRule.
     *
     * @param operand Root operand, must not be null
     *
     * @param id Description of rule
     */
    public LcsIndexAggRule(
        RelOptRuleOperand operand,
        String id)
    {
        super(operand, "LcsIndexAggRule: " + id);
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
        AggregateRel aggRel = (AggregateRel) call.rels[0];
        boolean allowRidOnlyProjection = true;
        // If any agg call references any column of the input,
        // then we have something other than COUNT(*), in which case
        // the LDB-201 special case does not apply.
        for (AggregateCall aggCall : aggRel.getAggCallList()) {
            if (aggCall.getArgList().size() > 0) {
                allowRidOnlyProjection = false;
                break;
            }
        }
        LcsRowScanRel rowScan = null;
        LcsIndexOnlyScanRel indexOnlyScan = null;
        if (call.rels[1] instanceof LcsRowScanRel) {
            rowScan = (LcsRowScanRel) call.rels[1];

            // NOTE: Here we check for no inputs because RelOptRuleOperand
            // seems to allow a row scan with inputs
            if (rowScan.getInputs().length > 0) {
                return;
            }
        } else {
            assert (call.rels[1] instanceof LcsNormalizerRel);
            assert (call.rels[2] instanceof LcsIndexOnlyScanRel);
            indexOnlyScan = (LcsIndexOnlyScanRel) call.rels[2];
            Integer [] proj = indexOnlyScan.getOutputProj();
            if (!projectionSatisfiesGroupBy(
                    proj,
                    aggRel.getGroupCount()))
            {
                return;
            }
        }

        if (indexOnlyScan == null) {
            // Try to convert a row scan into an index only scan. Find the
            // thinnest index that satisfies the row scan projection and the
            // aggregate's required sort order

            // first sort the indexes in key length
            TreeSet<FemLocalIndex> indexSet =
                new TreeSet<FemLocalIndex>(
                    new LcsIndexOptimizer.IndexLengthComparator());

            indexSet.addAll(LcsIndexOptimizer.getUnclusteredIndexes(rowScan));

            FemLocalIndex bestIndex = null;
            Integer [] bestProj = null;

            for (FemLocalIndex index : indexSet) {
                Integer [] proj =
                    LcsIndexOptimizer.findIndexOnlyProjection(
                        rowScan, index, allowRidOnlyProjection);
                if ((proj != null)
                    && projectionSatisfiesGroupBy(
                        proj,
                        aggRel.getGroupCount()))
                {
                    bestIndex = index;
                    bestProj = proj;
                    break;
                }
            }

            if (bestIndex == null) {
                return;
            }

            indexOnlyScan =
                new LcsIndexOnlyScanRel(
                    rowScan,
                    bestIndex,
                    bestProj);
        }

        RelNode indexAgg =
            new LcsIndexAggRel(
                aggRel.getCluster(),
                indexOnlyScan,
                aggRel.getGroupCount(),
                aggRel.getAggCallList());

        call.transformTo(indexAgg);
    }

    /**
     * Determines whether a projection from an index scan can meet the group by
     * requirements of an aggregate. The index scan columns are sorted in order,
     * and the group by columns are required to be sorted in order, so the index
     * scan projection should be a prefix of the index scan: 0, 1, 2, ... etc.
     *
     * @param proj the projection from an index
     * @param groupCount the number of columns to be grouped. the columns are
     * assumed to be the prefix of input to the aggregate
     *
     * @return whether the projection can meet the group by requirements of an
     * aggregate
     */
    private boolean projectionSatisfiesGroupBy(
        Integer [] proj,
        int groupCount)
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

// End LcsIndexAggRule.java

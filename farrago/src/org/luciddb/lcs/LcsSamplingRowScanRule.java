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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;


/**
 * LcsSamplingRowScanRule converts a {@link SamplingRel} applied to an {@link
 * LcsRowScanRel} into a {@link LcsSamplingRowScanRel}.
 *
 * @author Stephan Zuercher
 */
public class LcsSamplingRowScanRule
    extends RelOptRule
{
    public static final LcsSamplingRowScanRule instance =
        new LcsSamplingRowScanRule();

    //~ Constructors -----------------------------------------------------------

    /**
     * @deprecated use {@link LcsSamplingRowScanRule} instead
     */
    public LcsSamplingRowScanRule()
    {
        super(
            new RelOptRuleOperand(
                SamplingRel.class,
                new RelOptRuleOperand(LcsRowScanRel.class, ANY)));
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
        SamplingRel origSamplingRel = (SamplingRel) call.rels[0];
        LcsRowScanRel origScanRel = (LcsRowScanRel) call.rels[1];

        Util.permAssert(origScanRel.isFullScan, "Cannot sample index scans");
        Util.permAssert(
            !origScanRel.hasResidualFilters(),
            "Cannot sample scans with residual filters");

        RelOptCluster cluster = origScanRel.getCluster();
        RelOptConnection connection = origScanRel.getConnection();
        RelNode [] origScanRelInputs = origScanRel.getInputs();
        LcsTable lcsTable = origScanRel.lcsTable;

        RelOptSamplingParameters samplingParams =
            origSamplingRel.getSamplingParameters();

        LcsSamplingRowScanRel samplingScanRel =
            new LcsSamplingRowScanRel(
                cluster,
                origScanRelInputs,
                lcsTable,
                origScanRel.clusteredIndexes,
                connection,
                origScanRel.projectedColumns,
                samplingParams);

        call.transformTo(samplingScanRel);
    }
}

// End LcsSamplingRowScanRule.java

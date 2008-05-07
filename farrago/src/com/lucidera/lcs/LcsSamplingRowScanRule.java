/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2007-2007 LucidEra, Inc.
// Copyright (C) 2007-2007 The Eigenbase Project
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

import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.util.*;

/**
 * LcsSamplingRowScanRule converts a {@link SamplingRel} applied to 
 * an {@link LcsRowScanRel} into a {@link LcsSamplingRowScanRel}.
 *
 * @author Stephan Zuercher
 */
public class LcsSamplingRowScanRule
    extends RelOptRule
{
    public LcsSamplingRowScanRule()
    {
        super(
            new RelOptRuleOperand(
                SamplingRel.class,
                new RelOptRuleOperand(LcsRowScanRel.class, ANY)));
    }

    // implement RelOptRule
    public CallingConvention getOutConvention()
    {
        return FennelRel.FENNEL_EXEC_CONVENTION;
    }
    
    // implement RelOptRule
    public void onMatch(RelOptRuleCall call)
    {
        SamplingRel origSamplingRel = (SamplingRel)call.rels[0];
        LcsRowScanRel origScanRel = (LcsRowScanRel)call.rels[1];
        
        Util.permAssert(origScanRel.isFullScan, "Cannot sample index scans");
        Util.permAssert(
            !origScanRel.hasResidualFilter, 
            "Cannot sample scans with residual filters");
        
        RelOptCluster cluster = origScanRel.getCluster();
        RelOptConnection connection = origScanRel.getConnection();
        RelNode[] origScanRelInputs = origScanRel.getInputs();
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

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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

import java.sql.*;

import java.util.*;

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fem.sql2003.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/*
 * LcsSamplingRowScanRel is the relational expression corresponding to a
 * sampling scan on a column store table.
 *
 * This class exists separately from {@link LcsRowScanRel} to prevent rule
 * firings that would convert this row scan into something other than a
 * full row scan:  System sampling is not compatible with index scans.
 * Note, however, that Bernoulli sampling is, in principal, compatible
 * with index scans since the decision on whether to include each row in
 * the sample output is made independently.  A future improvement would be
 * to use a regular LcsRowScanRel for Bernoulli sampling and use this rel
 * only for system sampling.
 *
 * @author Stephan Zuercher
 * @version $Id$
 */
public class LcsSamplingRowScanRel
    extends LcsRowScanRelBase
{
    //~ Instance fields --------------------------------------------------------

    final RelOptSamplingParameters samplingParams;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsSamplingRowScanRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param children children inputs into the row scan
     * @param lcsTable table being scanned
     * @param clusteredIndexes clusters to use for table access
     * @param connection connection
     * @param projectedColumns array of 0-based table-relative column ordinals,
     * or null to project all columns
     */
    public LcsSamplingRowScanRel(
        RelOptCluster cluster,
        RelNode [] children,
        LcsTable lcsTable,
        List<FemLocalIndex> clusteredIndexes,
        RelOptConnection connection,
        Integer [] projectedColumns,
        RelOptSamplingParameters samplingParameters)
    {
        super(
            cluster,
            children,
            lcsTable,
            clusteredIndexes,
            connection,
            projectedColumns,
            true, // full row scan
            new Integer[0], // no residual filters
            1.0);

        this.samplingParams = samplingParameters;
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public LcsSamplingRowScanRel clone()
    {
        LcsSamplingRowScanRel clone =
            new LcsSamplingRowScanRel(
                getCluster(),
                RelOptUtil.clone(inputs),
                lcsTable,
                clusteredIndexes,
                connection,
                projectedColumns,
                samplingParams);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        assert (isFullScan);

        FemLcsRowScanStreamDef scanStream = createScanStream(implementor);

        CwmColumnSet columnSet = lcsTable.getCwmColumnSet();
        assert (columnSet instanceof FemAbstractColumnSet);

        // Don't allow caching of this plan, since it contains a row
        // count which could change at any moment.
        ((FarragoPreparingStmt) connection).disableStatementCaching();

        Long rowCount;
        Long [] rowCounts = new Long[2];
        Timestamp labelTimestamp =
            ((FarragoRelImplementor) implementor).getPreparingStmt()
            .getSession().getSessionLabelCreationTimestamp();
        FarragoCatalogUtil.getRowCounts(
            (FemAbstractColumnSet) columnSet,
            labelTimestamp,
            rowCounts);
        if (samplingParams.isBernoulli()) {
            scanStream.setSamplingMode(
                TableSamplingModeEnum.SAMPLING_BERNOULLI);
            rowCount = rowCounts[0] + rowCounts[1];
        } else {
            scanStream.setSamplingMode(TableSamplingModeEnum.SAMPLING_SYSTEM);
            rowCount = rowCounts[0];
            assert (rowCount != null);
        }
        scanStream.setSamplingRowCount(rowCount);

        scanStream.setSamplingRate(samplingParams.getSamplingPercentage());
        scanStream.setSamplingRepeatable(samplingParams.isRepeatable());
        scanStream.setSamplingRepeatableSeed(
            samplingParams.getRepeatableSeed());

        return scanStream;
    }

    // override LcsRowScanRelBase
    public void explain(RelOptPlanWriter pw)
    {
        super.explain(
            pw,
            new String[] {
                "mode",
                "rate",
                "repeatableSeed"
            },
            new Object[] {
                samplingParams.isBernoulli() ? "bernoulli" : "system",
                samplingParams.getSamplingPercentage(),
                samplingParams.isRepeatable()
                ? String.valueOf(samplingParams.getRepeatableSeed())
                : "-"
            });
    }
}

// End LcsSamplingRowScanRel.java

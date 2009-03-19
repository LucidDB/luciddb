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

import java.util.*;

import net.sf.farrago.fem.med.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;


/*
 * LcsRowScanRel is the relational expression corresponding to a scan on a
 * column store table.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsRowScanRel
    extends LcsRowScanRelBase
{
    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a new LcsRowScanRel object.
     *
     * @param cluster RelOptCluster for this rel
     * @param children children inputs into the row scan
     * @param lcsTable table being scanned
     * @param clusteredIndexes clusters to use for table access
     * @param connection connection
     * @param projectedColumns array of 0-based table-relative column ordinals,
     * or null to project all columns
     * @param isFullScan true if doing a full scan of the table
     * @param resCols residual filter columns
     */
    public LcsRowScanRel(
        RelOptCluster cluster,
        RelNode [] children,
        LcsTable lcsTable,
        List<FemLocalIndex> clusteredIndexes,
        RelOptConnection connection,
        Integer [] projectedColumns,
        boolean isFullScan,
        Integer [] resCols,
        double inputSelectivity)
    {
        super(
            cluster,
            children,
            lcsTable,
            clusteredIndexes,
            connection,
            projectedColumns,
            isFullScan,
            resCols,
            inputSelectivity);
    }

    //~ Methods ----------------------------------------------------------------

    // implement RelNode
    public LcsRowScanRel clone()
    {
        LcsRowScanRel clone =
            new LcsRowScanRel(
                getCluster(),
                RelOptUtil.clone(inputs),
                lcsTable,
                clusteredIndexes,
                connection,
                projectedColumns,
                isFullScan,
                residualColumns,
                inputSelectivity);
        clone.inheritTraitsFrom(this);
        return clone;
    }
}

// End LcsRowScanRel.java

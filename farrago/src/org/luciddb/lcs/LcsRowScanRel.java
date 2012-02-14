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
     * @param inputSelectivity estimate of input selectivity
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

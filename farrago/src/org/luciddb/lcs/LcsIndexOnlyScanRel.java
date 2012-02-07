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

import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;
import org.eigenbase.relopt.*;
import org.eigenbase.reltype.*;


/**
 * A relation for performing an index only scan of a table (as opposed to
 * performing a regular row scan). It may have the benefit of a sorted result
 * set.
 *
 * @author John Pham
 * @version $Id$
 */
public class LcsIndexOnlyScanRel
    extends FennelOptionalRel
{
    //~ Instance fields --------------------------------------------------------

    private final boolean fullScan;
    private final Integer [] projectedColumns;
    private final boolean isUniqueKey;
    private final boolean isOuter;
    private final Integer [] inputKeyProj;
    private final Integer [] inputJoinProj;
    private final Integer [] inputDirectiveProj;
    private final LcsTable table;
    private final FemLocalIndex index;

    //~ Constructors -----------------------------------------------------------

    /**
     * Constructs an index only scan based upon another index scan. The index
     * scan already has an input.
     *
     * @param cluster the environment for the scan
     * @param child the index scan input, which produces search directives
     * @param searchRel the index search to be converted into an index only scan
     * @param index index to be scanned, may differ from the one in searchRel
     * @param proj keys to be projected from the index
     */
    public LcsIndexOnlyScanRel(
        RelOptCluster cluster,
        RelNode child,
        LcsIndexSearchRel searchRel,
        FemLocalIndex index,
        Integer [] proj)
    {
        this(
            cluster,
            child,
            searchRel.lcsTable,
            index,
            searchRel.fullScan,
            proj,
            searchRel.isUniqueKey,
            searchRel.isOuter,
            searchRel.inputKeyProj,
            searchRel.inputJoinProj,
            searchRel.inputDirectiveProj);
    }

    /**
     * Constructs an index-only scan from a row scan.
     *
     * @param rowScan row scan
     * @param index the index to be scanned
     * @param proj keys to be projected from the index
     */
    public LcsIndexOnlyScanRel(
        LcsRowScanRel rowScan,
        FemLocalIndex index,
        Integer [] proj)
    {
        this(
            rowScan.getCluster(),
            null,
            rowScan.lcsTable,
            index,
            true,
            proj,
            false,
            false,
            null,
            null,
            null);
    }

    /**
     * Common method for creating an index only scan
     *
     * @param cluster the environment for the scan
     * @param child the child of the scan
     * @param table the table of the index to be scanned
     * @param index the index to be scanned
     * @param fullScan true if the entire index is being scanned
     * @param projectedColumns for full scans, the output projection. Should be
     * null for searches
     * @param isUniqueKey TODO
     * @param isOuter TODO
     * @param inputKeyProj for searches, the key columns
     * @param inputJoinProj for index joins applied to searches, the input keys
     * to be output
     * @param inputDirectiveProj for searches, the search directive columns
     */
    private LcsIndexOnlyScanRel(
        RelOptCluster cluster,
        RelNode child,
        LcsTable table,
        FemLocalIndex index,
        boolean fullScan,
        Integer [] projectedColumns,
        boolean isUniqueKey,
        boolean isOuter,
        Integer [] inputKeyProj,
        Integer [] inputJoinProj,
        Integer [] inputDirectiveProj)
    {
        super(cluster, child);
        this.table = table;
        this.index = index;
        this.fullScan = fullScan;
        this.projectedColumns = projectedColumns;
        this.isUniqueKey = isUniqueKey;
        this.isOuter = isOuter;
        this.inputKeyProj = inputKeyProj;
        this.inputJoinProj = inputJoinProj;
        this.inputDirectiveProj = inputDirectiveProj;
    }

    //~ Methods ----------------------------------------------------------------

    // implement AbstractRelNode
    public LcsIndexOnlyScanRel clone()
    {
        LcsIndexOnlyScanRel clone =
            new LcsIndexOnlyScanRel(
                getCluster(),
                getChild(),
                table,
                index,
                fullScan,
                projectedColumns,
                isUniqueKey,
                isOuter,
                inputKeyProj,
                inputJoinProj,
                inputDirectiveProj);
        clone.inheritTraitsFrom(this);
        return clone;
    }

    // implement RelNode
    protected RelDataType deriveRowType()
    {
        RelDataType rowType =
            table.getIndexGuide().createUnclusteredRowType(
                index,
                projectedColumns);
        return rowType;
    }

    public void explain(RelOptPlanWriter pw)
    {
        Object projectionObj =
            FennelRelUtil.explainProjection(projectedColumns);
        Object inputKeyProjObj = FennelRelUtil.explainProjection(inputKeyProj);
        Object inputDirectiveProjObj =
            FennelRelUtil.explainProjection(inputDirectiveProj);

        if (!fullScan) {
            // index search
            pw.explain(
                this,
                new String[] {
                    "input", "table", "index", "projection",
                    "inputKeyProj", "inputDirectiveProj"
                },
                new Object[] {
                    Arrays.asList(table.getQualifiedName()),
                    index.getName(), projectionObj,
                    inputKeyProjObj, inputDirectiveProjObj
                });
        } else {
            // index scan
            pw.explain(
                this,
                new String[] {
                    "table", "index", "projection"
                },
                new Object[] {
                    Arrays.asList(table.getQualifiedName()),
                    index.getName(),
                    projectionObj
                });
        }
    }

    // implement FennelRel
    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        LcsIndexGuide indexGuide = table.getIndexGuide();

        // Recreate the original search rel with the new output projection
        LcsIndexSearchRel searchRel =
            new LcsIndexSearchRel(
                getCluster(),
                getChild(),
                table,
                index,
                fullScan,
                projectedColumns,
                isUniqueKey,
                isOuter,
                true,
                inputKeyProj,
                inputJoinProj,
                inputDirectiveProj,
                null,
                null,
                fullScan ? 1.0 : null);
        searchRel.inheritTraitsFrom(this);

        LcsIndexMinusRel minus =
            indexGuide.createMinusOfDeletionIndex(
                this,
                searchRel.lcsTable,
                searchRel);

        return implementor.visitFennelChild(minus, 0);
    }

    public Integer [] getOutputProj()
    {
        return projectedColumns;
    }
}

// End LcsIndexOnlyScanRel.java

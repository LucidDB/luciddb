/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

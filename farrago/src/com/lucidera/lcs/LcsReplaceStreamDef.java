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

import net.sf.farrago.catalog.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.fennel.rel.*;
import net.sf.farrago.query.*;


/**
 * LcsReplaceStreamDef creates an cluster replace execution stream def
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsReplaceStreamDef
    extends LcsAppendStreamDef
{
    //~ Instance fields --------------------------------------------------------

    List<FemLocalIndex> updateClusters;

    //~ Constructors -----------------------------------------------------------

    public LcsReplaceStreamDef(
        FarragoRepos repos,
        LcsTable lcsTable,
        FemExecutionStreamDef inputStream,
        FennelRel appendRel,
        Double estimatedNumInputRows,
        List<FemLocalIndex> updateClusters)
    {
        super(repos, lcsTable, inputStream, appendRel, estimatedNumInputRows);
        this.updateClusters = updateClusters;
    }

    //~ Methods ----------------------------------------------------------------

    protected void setupIndexes()
    {
        replaceColumns = true;

        // Setup an index guide that only includes the clusters being replaced
        clusteredIndexes = updateClusters;
        indexGuide =
            new LcsIndexGuide(
                lcsTable.getPreparingStmt().getFarragoTypeFactory(),
                lcsTable.getCwmColumnSet(),
                clusteredIndexes);

        // Determine the list of relevant unclustered indexes by finding the
        // set of coverage indexes for each clustered index
        unclusteredIndexes = new ArrayList<FemLocalIndex>();
        List<FemLocalIndex> candidateIndexes =
            FarragoCatalogUtil.getUnclusteredIndexes(
                repos,
                lcsTable.getCwmColumnSet());
        for (FemLocalIndex cluster : updateClusters) {
            List<FemLocalIndex> coverageList =
                LcsIndexGuide.getIndexCoverageSet(
                    repos,
                    cluster,
                    candidateIndexes,
                    false,
                    false);
            for (FemLocalIndex index : coverageList) {
                if (!unclusteredIndexes.contains(index)) {
                    unclusteredIndexes.add(index);
                }
            }
        }
    }

    protected FemSplitterStreamDef createSplitter()
    {
        LcsTableMergeRel mergeRel = (LcsTableMergeRel) appendRel;
        return indexGuide.newSplitter(mergeRel.getExpectedInputRowType(0));
    }

    protected void createClusterAppends(
        FennelRelImplementor implementor,
        List<FemLcsClusterAppendStreamDef> clusterAppendDefs)
    {
        int clusterPos = 1;
        for (FemLocalIndex clusteredIndex : clusteredIndexes) {
            FennelRelParamId rootPageIdParamId =
                implementor.allocateRelParamId();
            clusterAppendDefs.add(
                indexGuide.newClusterAppend(
                    appendRel,
                    clusteredIndex,
                    hasIndexes,
                    implementor.translateParamId(rootPageIdParamId).intValue(),
                    clusterPos++,
                    false));
        }
    }
}

// End LcsReplaceStreamDef.java

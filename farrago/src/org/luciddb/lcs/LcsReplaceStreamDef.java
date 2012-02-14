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

/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.rel.*;


/**
 * LcsAppendStreamDef creates an append execution stream def
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsAppendStreamDef
{

    //~ Instance fields --------------------------------------------------------

    private FarragoRepos repos;
    private LcsTable lcsTable;
    private FemExecutionStreamDef inputStream;
    private FennelRel appendRel;
    private LcsIndexGuide indexGuide;

    //~ Constructors -----------------------------------------------------------

    public LcsAppendStreamDef(
        FarragoRepos repos,
        LcsTable lcsTable,
        FemExecutionStreamDef inputStream,
        FennelRel appendRel)
    {
        this.repos = repos;
        this.lcsTable = lcsTable;
        this.inputStream = inputStream;
        this.appendRel = appendRel;
        indexGuide = lcsTable.getIndexGuide();
    }

    //~ Methods ----------------------------------------------------------------

    public FemExecutionStreamDef toStreamDef(FennelRelImplementor implementor)
    {
        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();

        //
        // 1. Setup the SplitterStreamDef
        //
        FemSplitterStreamDef splitter =
            indexGuide.newSplitter((SingleRel) appendRel);

        //
        // 2. Setup all the LcsClusterAppendStreamDef's
        //    - Get all the clustered indices.
        //    - For each index, set up the corresponding clusterAppend stream
        //      def.
        //

        List<FemLcsClusterAppendStreamDef> clusterAppendDefs =
            new ArrayList<FemLcsClusterAppendStreamDef>();

        // Get the clustered indexes associated with this table.
        List<FemLocalIndex> clusteredIndexes =
            FarragoCatalogUtil.getClusteredIndexes(repos, table);

        for (FemLocalIndex clusteredIndex : clusteredIndexes) {
            clusterAppendDefs.add(
                indexGuide.newClusterAppend(appendRel, clusteredIndex));
        }

        //
        // 3. Setup the BarrierStreamDef.
        //
        FemBarrierStreamDef barrier = indexGuide.newBarrier(appendRel, -1);

        //
        // 4. Link the StreamDefs together.  Note that the input may be
        //    a buffering stream
        //                               -> clusterAppend ->
        // input( -> buffer) -> splitter -> clusterAppend -> barrier
        //                                  ...
        //                               -> clusterAppend ->
        //
        implementor.addDataFlowFromProducerToConsumer(
            inputStream,
            splitter);

        for (FemLcsClusterAppendStreamDef clusterAppend : clusterAppendDefs) {
            implementor.addDataFlowFromProducerToConsumer(
                splitter,
                clusterAppend);
            implementor.addDataFlowFromProducerToConsumer(
                clusterAppend,
                barrier);
        }

        //
        // 5. If there are no unclustered indexes, stop at the barrier
        //
        List<FemLocalIndex> unclusteredIndexes =
            FarragoCatalogUtil.getUnclusteredIndexes(repos, table);
        if (unclusteredIndexes.size() == 0) {
            return barrier;
        }

        // Update clustered index scans
        for (FemLcsClusterAppendStreamDef clusterAppend : clusterAppendDefs) {
            clusterAppend.setOutputDesc(
                indexGuide.getUnclusteredInputDesc());
        }
        barrier.setOutputDesc(indexGuide.getUnclusteredInputDesc());

        //
        // 6. Setup unclustered indices.
        //    - For each index, set up the corresponding bitmap append
        //
        ArrayList<LcsCompositeStreamDef> bitmapAppendDefs =
            new ArrayList<LcsCompositeStreamDef>();

        for (FemLocalIndex unclusteredIndex : unclusteredIndexes) {
            LcsIndexGuide ucxIndexGuide = getIndexGuide(unclusteredIndex);
            FennelRelParamId dynParamId = implementor.allocateRelParamId();
            bitmapAppendDefs.add(
                ucxIndexGuide.newBitmapAppend(
                    appendRel,
                    unclusteredIndex,
                    implementor,
                    false,
                    dynParamId));
        }

        //
        // 7. Setup a bitmap SplitterStreamDef
        //
        FemSplitterStreamDef bitmapSplitter =
            indexGuide.newSplitter((SingleRel) appendRel);

        //
        // 8. Setup a bitmap BarrierStreamDef
        //
        FemBarrierStreamDef bitmapBarrier =
            indexGuide.newBarrier(appendRel, -1);

        //
        // 9. Link the bitmap StreamDefs together.
        //                     -> bitmap append streams ->
        // barrier -> splitter -> bitmap append streams -> barrier
        //                                  ...
        //                     -> bitmap append streams ->
        //

        implementor.addDataFlowFromProducerToConsumer(
            barrier,
            bitmapSplitter);

        for (Object streamDef : bitmapAppendDefs) {
            LcsCompositeStreamDef bitmapAppend =
                (LcsCompositeStreamDef) streamDef;
            implementor.addDataFlowFromProducerToConsumer(
                bitmapSplitter,
                bitmapAppend.getConsumer());
            implementor.addDataFlowFromProducerToConsumer(
                bitmapAppend.getProducer(),
                bitmapBarrier);
        }

        return bitmapBarrier;
    }

    /**
     * Returns an index guide specific to an unclustered index
     */
    private LcsIndexGuide getIndexGuide(FemLocalIndex unclusteredIndex)
    {
        return
            new LcsIndexGuide(
                lcsTable.getPreparingStmt().getFarragoTypeFactory(),
                lcsTable.getCwmColumnSet(),
                unclusteredIndex);
    }
}

//End LcsAppendStreamDef

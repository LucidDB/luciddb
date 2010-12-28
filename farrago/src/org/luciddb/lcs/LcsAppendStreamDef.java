/*
// $Id$
// Farrago is an extensible data management system.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
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

import net.sf.farrago.catalog.*;
import net.sf.farrago.cwm.relational.*;
import net.sf.farrago.fem.fennel.*;
import net.sf.farrago.fem.med.*;
import net.sf.farrago.query.*;

import org.eigenbase.reltype.*;


/**
 * LcsAppendStreamDef creates an append execution stream def
 *
 * @author Zelaine Fong
 * @version $Id$
 */
public class LcsAppendStreamDef
{
    //~ Instance fields --------------------------------------------------------

    protected FarragoRepos repos;
    protected LcsTable lcsTable;
    private FemExecutionStreamDef inputStream;
    protected FennelRel appendRel;
    private Double estimatedNumInputRows;
    protected LcsIndexGuide indexGuide;
    protected List<FemLocalIndex> clusteredIndexes;
    protected List<FemLocalIndex> unclusteredIndexes;
    protected FemLocalIndex deletionIndex;
    protected boolean replaceColumns;
    protected boolean hasIndexes;
    private boolean alterTable;

    //~ Constructors -----------------------------------------------------------

    public LcsAppendStreamDef(
        FarragoRepos repos,
        LcsTable lcsTable,
        FemExecutionStreamDef inputStream,
        FennelRel appendRel,
        Double estimatedNumInputRows)
    {
        this.repos = repos;
        this.lcsTable = lcsTable;
        this.inputStream = inputStream;
        this.appendRel = appendRel;
        this.estimatedNumInputRows = estimatedNumInputRows;
        this.deletionIndex =
            FarragoCatalogUtil.getDeletionIndex(
                repos,
                lcsTable.getCwmColumnSet());
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Initializes the clustered and unclustered indexes relevant to the stream
     * that will be created.
     */
    protected void setupIndexes()
    {
        replaceColumns = false;
        indexGuide = lcsTable.getIndexGuide();

        // Get the clustered and unclustered indexes associated with this table
        CwmTable table = (CwmTable) lcsTable.getCwmColumnSet();
        clusteredIndexes = FarragoCatalogUtil.getClusteredIndexes(repos, table);
        unclusteredIndexes =
            FarragoCatalogUtil.getUnclusteredIndexes(repos, table);

        if (appendRel.getInput(0).getRowType().getFieldCount()
            < indexGuide.getFlattenedRowType().getFieldCount())
        {
            // We're doing ALTER TABLE ADD COLUMN
            alterTable = true;

            // TODO jvs 6-Dec-2008: need to deal with these once we allow ALTER
            // TABLE ADD COLUMN c UNIQUE
            unclusteredIndexes = Collections.emptyList();

            // Filter clusteredIndexes down to just the one
            // containing the new column.
            Integer [] projection = new Integer[1];
            projection[0] =
                indexGuide.getFlattenedRowType().getFieldCount() - 1;

            Iterator<FemLocalIndex> iter = clusteredIndexes.iterator();
            while (iter.hasNext()) {
                FemLocalIndex clusteredIndex = iter.next();
                if (!indexGuide.testIndexCoverage(clusteredIndex, projection)) {
                    iter.remove();
                }
            }
            assert (clusteredIndexes.size() == 1);

            // Create a new index guide which pretends that the
            // table only consists of the new clustered index.
            indexGuide =
                new LcsIndexGuide(
                    lcsTable.getPreparingStmt().getFarragoTypeFactory(),
                    table,
                    clusteredIndexes);
        }
    }

    /**
     * Creates the top half of an insert execution stream, i.e., the part that
     * appends the clustered indexes.
     *
     * @param implementor FennelRel implementor
     *
     * @return the barrier stream that anchors the top half of the insert
     * execution stream
     */
    public FemBarrierStreamDef createClusterAppendStreams(
        FennelRelImplementor implementor)
    {
        setupIndexes();

        // if the table has unclustered indexes, the output from the append
        // stream contains a startRid; so make sure to reflect that in the
        // output descriptors
        hasIndexes = (unclusteredIndexes.size() > 0);

        //
        // Setup all the LcsClusterAppendStreamDef's
        //    - Get all the clustered indices.
        //    - For each index, set up the corresponding clusterAppend stream
        //      def.
        //

        List<FemLcsClusterAppendStreamDef> clusterAppendDefs =
            new ArrayList<FemLcsClusterAppendStreamDef>();
        createClusterAppends(implementor, clusterAppendDefs);

        //
        // Setup the BarrierStreamDef.
        //
        RelDataType barrierRowType;
        if (hasIndexes) {
            barrierRowType = indexGuide.getUnclusteredInputType();
        } else {
            barrierRowType = appendRel.getRowType();
        }
        FemBarrierStreamDef barrier =
            indexGuide.newBarrier(
                barrierRowType,
                BarrierReturnModeEnum.BARRIER_RET_ANY_INPUT,
                0);

        //
        // Link the StreamDefs together.  Note that the input may be
        //    a buffering stream
        //                               -> clusterAppend ->
        // input( -> buffer) -> splitter -> clusterAppend -> barrier
        //                                  ...
        //                               -> clusterAppend ->
        //

        //
        // Setup the SplitterStreamDef, unless there's only one
        // clusterAppend, in which case no splitter is needed.
        //
        FemExecutionStreamDef splitter;
        if (clusterAppendDefs.size() != 1) {
            splitter = createSplitter();

            implementor.addDataFlowFromProducerToConsumer(
                inputStream,
                splitter);
        } else {
            splitter = inputStream;
        }

        for (FemLcsClusterAppendStreamDef clusterAppend : clusterAppendDefs) {
            implementor.addDataFlowFromProducerToConsumer(
                splitter,
                clusterAppend);
            implementor.addDataFlowFromProducerToConsumer(
                clusterAppend,
                barrier);
        }

        return barrier;
    }

    protected FemSplitterStreamDef createSplitter()
    {
        return indexGuide.newSplitter(lcsTable.getRowType());
    }

    protected void createClusterAppends(
        FennelRelImplementor implementor,
        List<FemLcsClusterAppendStreamDef> clusterAppendDefs)
    {
        for (FemLocalIndex clusteredIndex : clusteredIndexes) {
            clusterAppendDefs.add(
                indexGuide.newClusterAppend(
                    appendRel,
                    clusteredIndex,
                    hasIndexes,
                    0,
                    0,
                    alterTable));
        }
    }

    /**
     * @return true if this append stream graph will have an index creation
     * substream
     */
    public boolean streamHasIndexes()
    {
        return hasIndexes;
    }

    /**
     * Creates the bottom half of an insert execution stream, i.e., the part the
     * inserts into the unclustered indexes.
     *
     * @param implementor FennelRel implementor
     * @param clusterAppendBarrier the barrier from the cluster appends that
     * serves as the producer for the unclustered index append streams
     * @param writeRowCountParamId parameter id the final barrier will use to
     * retrieve the upstream deletion rowcount, in the case of a MERGE statement
     *
     * @return the final barrier that anchors the full insert stream or the
     * clusterAppendBarrier if the table does not have any unclustered indexes
     */
    public FemBarrierStreamDef createBitmapAppendStreams(
        FennelRelImplementor implementor,
        FemBarrierStreamDef clusterAppendBarrier,
        int writeRowCountParamId)
    {
        // If there are no unclustered indexes, no need to proceed any further
        if (unclusteredIndexes.size() == 0) {
            return clusterAppendBarrier;
        }

        // Setup unclustered indices.
        //    - For each index, set up the corresponding bitmap append
        // If the index is unique, then pass along the deletion index so the
        // bitmap appenders can read from it
        ArrayList<LcsCompositeStreamDef> bitmapAppendDefs =
            new ArrayList<LcsCompositeStreamDef>();
        int numUniqueIndexes = 0;
        FemLocalIndex delIndex;
        for (FemLocalIndex unclusteredIndex : unclusteredIndexes) {
            if (FarragoCatalogUtil.isIndexUnique(unclusteredIndex)) {
                numUniqueIndexes++;
                delIndex = deletionIndex;
            } else {
                delIndex = null;
            }
            LcsIndexGuide ucxIndexGuide = getIndexGuide(unclusteredIndex);
            FennelRelParamId insertDynParamId =
                implementor.allocateRelParamId();
            LcsCompositeStreamDef bitmapAppend =
                ucxIndexGuide.newBitmapAppend(
                    appendRel,
                    unclusteredIndex,
                    delIndex,
                    implementor,
                    false,
                    insertDynParamId,
                    replaceColumns);
            bitmapAppendDefs.add(bitmapAppend);

            // splicers updating unique indexes can produce violations.
            // for these streams, register the types of their error records.
            if (delIndex != null) {
                FemLbmSplicerStreamDef splicer =
                    (FemLbmSplicerStreamDef) bitmapAppend.getProducer();
                RelDataType errorType =
                    ucxIndexGuide.createSplicerErrorType(unclusteredIndex);
                implementor.setErrorRecordType(appendRel, splicer, errorType);
            }
        }

        // Setup a bitmap SplitterStreamDef and link it to the cluster append
        // barrier
        FemSplitterStreamDef bitmapSplitter =
            indexGuide.newSplitter(indexGuide.getUnclusteredInputType());
        implementor.addDataFlowFromProducerToConsumer(
            clusterAppendBarrier,
            bitmapSplitter);

        // Setup a bitmap BarrierStreamDef; if there are no unique keys,
        // this is the final barrier
        int dynParam = (numUniqueIndexes > 0) ? 0 : writeRowCountParamId;
        FemBarrierStreamDef bitmapBarrier =
            indexGuide.newBarrier(
                appendRel.getRowType(),
                BarrierReturnModeEnum.BARRIER_RET_ANY_INPUT,
                dynParam);

        // For each bitmap append stream, link
        //    splitter -> bitmap append stream -> bitmap barrier
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

        // if there are no unique indexes, no need to deal with unique
        // constraint violations
        if (numUniqueIndexes == 0) {
            return bitmapBarrier;
        }

        // Setup the substream for inserting unique constraint violations
        // into the deletion index.  Note that this method also links
        // the bitmap append streams to the violation streams
        FemLbmSplicerStreamDef violationStream =
            createViolationStream(
                implementor,
                bitmapAppendDefs,
                numUniqueIndexes);

        // Setup the final barrier
        FemBarrierStreamDef finalBarrier =
            indexGuide.newBarrier(
                appendRel.getRowType(),
                BarrierReturnModeEnum.BARRIER_RET_ALL_INPUTS,
                writeRowCountParamId);

        // Link to the final barrier
        //    bitmap barrier------\
        //                      final barrier
        //    violation substream /
        implementor.addDataFlowFromProducerToConsumer(
            bitmapBarrier,
            finalBarrier);
        implementor.addDataFlowFromProducerToConsumer(
            violationStream,
            finalBarrier);

        return finalBarrier;
    }

    /**
     * Returns an index guide specific to an unclustered index
     *
     * @param unclusteredIndex the unclustered index
     */
    private LcsIndexGuide getIndexGuide(FemLocalIndex unclusteredIndex)
    {
        return new LcsIndexGuide(
            lcsTable.getPreparingStmt().getFarragoTypeFactory(),
            lcsTable.getCwmColumnSet(),
            unclusteredIndex);
    }

    /**
     * Creates the substream that inserts unique constraint violations into the
     * table's deletion index. Only bitmap appenders that are writing to a
     * unique index will create violations. Those streams are then merged
     * together into a single stream of violating rids.
     *
     * <p>Bitmap appenders that do not write to unique indexes don't need to be
     * involved. In fact, the violation substream can even proceed before those
     * appenders have finished because they don't access the deletion index and
     * therefore there is no conflict.
     *
     * @param implementor FennelRel implementor
     * @param bitmapAppendDefs bitmap append substreams that created the
     * violations
     * @param numUniqueIndexes number of unique indexes on the table; must be
     * &gt; 0
     *
     * @return the stream def corresponding to the splicer that inserts into the
     * deletion index
     */
    private FemLbmSplicerStreamDef createViolationStream(
        FennelRelImplementor implementor,
        ArrayList<LcsCompositeStreamDef> bitmapAppendDefs,
        int numUniqueIndexes)
    {
        assert (numUniqueIndexes > 0);

        // create a merge stream if there is more than one stream of violation
        // rids
        FemExecutionStreamDef deleteInput = null;
        FemMergeStreamDef mergeStream = null;
        if (numUniqueIndexes > 1) {
            mergeStream = repos.newFemMergeStreamDef();
            mergeStream.setSequential(false);
            mergeStream.setPrePullInputs(false);
            deleteInput = mergeStream;
        }

        // locate the bitmap appenders corresponding to unique indexes and
        // either connect them to the merge stream or keep track of the
        // producer so we can use it to feed into the delete substream
        Iterator<FemLocalIndex> indexIter = unclusteredIndexes.iterator();
        for (LcsCompositeStreamDef bitmapAppendStream : bitmapAppendDefs) {
            FemLocalIndex index = indexIter.next();
            if (FarragoCatalogUtil.isIndexUnique(index)) {
                if (numUniqueIndexes > 1) {
                    implementor.addDataFlowFromProducerToConsumer(
                        bitmapAppendStream.getProducer(),
                        mergeStream);
                } else {
                    deleteInput = bitmapAppendStream.getProducer();
                }
            }
        }

        // assume that the number of violations is 1% of the number of input
        // rows
        Double numViolations = estimatedNumInputRows;
        if (numViolations != null) {
            numViolations *= .01;
            if (numViolations < 1.0) {
                numViolations = 1.0;
            }
        }

        // create the delete substream; no need to remove duplicates if we
        // have only one stream of violation rids
        return createDeleteRidStream(
            implementor,
            deleteInput,
            numViolations,
            0,
            (numUniqueIndexes > 1));
    }

    /**
     * Creates a substream that takes an input stream of rids, sorts them,
     * optionally removes duplicates, and then adds them into the deletion index
     * associated with the table we're appending to
     *
     * @param implementor FennelRel implementor
     * @param inputStream input stream containing rids
     * @param inputRowCount estimated number of rids to be added to the deletion
     * index
     * @param writeRowCountParamId > 0 if the splicer that writes to the
     * deletion index needs to return a count of the number of rids written
     * @param removeDups if true, remove duplicate rids from the stream
     *
     * @return the stream def corresponding to the splicer that inserts into the
     * deletion index
     */
    public FemLbmSplicerStreamDef createDeleteRidStream(
        FennelRelImplementor implementor,
        FemExecutionStreamDef inputStream,
        Double inputRowCount,
        int writeRowCountParamId,
        boolean removeDups)
    {
        // sort the rids so the splicer will have ordered input; note that
        // we create a sorter that does early close; this may not be absolutely
        // necessary in all cases, but it doesn't hurt
        FemSortingStreamDef sortingStream =
            indexGuide.newSorter(deletionIndex, inputRowCount, true, true);
        implementor.addDataFlowFromProducerToConsumer(
            inputStream,
            sortingStream);

        // remove duplicate rids; use a sorted agg stream since we need to
        // sort the rids anyway, and we don't expect many duplicates
        FemExecutionStreamDef deleteInput;
        if (!removeDups) {
            deleteInput = sortingStream;
        } else {
            FemSortedAggStreamDef distinctStream =
                repos.newFemSortedAggStreamDef();
            distinctStream.setGroupingPrefixSize(1);
            implementor.addDataFlowFromProducerToConsumer(
                sortingStream,
                distinctStream);
            deleteInput = distinctStream;
        }

        // setup the splicer that inserts the deleted rids into the deletion
        // index
        FemLbmSplicerStreamDef deleter =
            indexGuide.newSplicer(
                appendRel,
                deletionIndex,
                null,
                0,
                writeRowCountParamId,
                false);
        implementor.addDataFlowFromProducerToConsumer(
            deleteInput,
            deleter);

        return deleter;
    }
}

// End LcsAppendStreamDef.java

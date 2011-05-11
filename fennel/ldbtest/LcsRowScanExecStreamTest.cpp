/*
// $Id$
// Fennel is a library of data storage and processing components.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamUnitTestBase.h"
#include "fennel/lcs/LcsClusterAppendExecStream.h"
#include "fennel/lcs/LcsRowScanExecStream.h"
#include "fennel/lcs/LcsCountAggExecStream.h"
#include "fennel/lbm/LbmEntry.h"
#include "fennel/ldbtest/SamplingExecStreamGenerator.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/cache/Cache.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for scanning multiple clusters.  Note that
 * LcsClusterAppendExecStreamTest also has some tests for scans, but those
 * only test single cluster scans
 */
class LcsRowScanExecStreamTest : public ExecStreamUnitTestBase
{
protected:
    static const uint NDUPS = 20;
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;
    TupleAttributeDescriptor attrDesc_bitmap;
    TupleAttributeDescriptor attrDesc_char1;
    uint bitmapColSize;

    vector<boost::shared_ptr<BTreeDescriptor> > bTreeClusters;

    /**
     * Serially loads nClusters, each cluster containing nCols and nRows
     */
    void loadClusters(
        uint nRows,
        uint nCols,
        uint nClusters,
        bool compressed);

    /**
     * Loads a single cluster with nCols columns and nRows rows.
     * Each column will have a different sequence of values, as follows:
     *      column0 - colStart, colStart+1, ..., colStart+nRows-1
     *      column1 - colStart+1, colStart+2, ..., colStart+nRows
     *      column2 - colStart+2, colStart+3, ..., colStart+nRows+1
     *      ...
     */
    void loadOneCluster(
        uint nRows,
        uint nCols,
        int colStart,
        BTreeDescriptor &bTreeDescriptor,
        bool compressed);

    /**
     * Reads rows from clusters.  Assumes clusters have been loaded by
     * loadClusters/loadOneCluster.
     *
     * @param nRows total number of rows in the clusters
     *
     * @param nCols number of columns in each cluster
     *
     * @param nClusters number of clusters
     *
     * @param proj columns to be projected
     *
     * @param skipRows read every "skipRows" rows
     *
     * @param expectedNumRows expected number of rows in scan result;
     * normally should be the same as nRows unless skipping rows or
     * testing exception cases
     */
    void testScanCols(
        uint nRows,
        uint nCols,
        uint nClusters,
        TupleProjection proj,
        uint skipRows,
        uint expectedNumRows);

    /**
     * Filter rows from clusters.  Assumes clusters have been loaded by
     * loadClusters/loadOneCluster.
     *
     * @param nRows total number of rows in the clusters
     *
     * @param nCols number of columns in each cluster
     *
     * @param nClusters number of clusters
     *
     * @param proj columns to be projected
     *
     * @param skipRows read every "skipRows" rows
     *
     * @param expectedNumRows expected number of rows in scan result;
     * normally should be the same as nRows unless skipping rows or
     * testing exception cases
     *
     * @param compressed testing compressed bitmap optimization
     *
     * @param pCountParams if non-null, perform only a count; otherwise
     * (default) fetch and compare actual rows
     */
    void testFilterCols(
        uint nRows,
        uint nCols,
        uint nClusters,
        TupleProjection proj,
        uint skipRows,
        uint expectedNumRows,
        bool compressed,
        LcsCountAggExecStreamParams *pCountParams = NULL);

    void setSearchKey(
        char lowerDirective,
        char upperDirective,
        uint64_t lowerVal,
        uint64_t upperVal,
        PBuffer inputBuf,
        uint &offset,
        TupleAccessor &inputTupleAccessor,
        TupleData &inputTupleData);

    /**
     * Sample rows from clusters.  Assumes clusters have been loaded by
     * loadClusters/loadOneCluster.
     *
     * @param nRows total number of rows in the clusters; if set to 0, full
     * table scans should be done
     *
     * @param nRowsActual actual total number of rows in the clusters
     *
     * @param nCols number of columns in each cluster
     *
     * @param nClusters number of clusters
     *
     * @param proj columns to be projected
     *
     * @param skipRows read every "skipRows" rows
     *
     * @param mode sampling mode (SAMPLING_BERNOULLI or SAMPLING_SYSTEM)
     *
     * @param rate sampling rate
     *
     * @param seed Bernoulli sampling RNG seed
     *
     * @param clumps number of system sampling clumps
     *
     * @param expectedNumRows expected number of rows in scan result;
     * normally should be the same as nRows unless skipping rows or
     * testing exception cases
     */
    void testSampleScanCols(
        uint nRows,
        uint nRowsActual,
        uint nCols,
        uint nClusters,
        TupleProjection proj,
        uint skipRows,
        TableSamplingMode mode,
        float rate,
        int seed,
        uint clumps,
        uint expectedNumRows);

    /**
     * Generate bitmaps to pass as input into row scan exec stream
     *
     * @param nRows number of rows in table
     *
     * @param skipRows generate rids every "skipRows" rows; i.e., if skipRows
     * == 1, there are no gaps in the rids
     *
     * @param bitmapTupleDesc tuple descriptor for bitmap segment
     *
     * @param pBuf buffer where bitmap segment tuples will be marshalled
     *
     * @return size of the buffer containing the marshalled tuples
     */
    int generateBitmaps(
        uint nRows, uint skipRows, TupleDescriptor const &bitmapTupleDesc,
        PBuffer pBuf);

    void produceEntry(
        LbmEntry &lbmEntry, TupleAccessor &bitmapTupleAccessor, PBuffer pBuf,
        int &bufSize);

public:
    explicit LcsRowScanExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testScans);
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testScanOnEmptyCluster);
        FENNEL_UNIT_TEST_CASE(
            LcsRowScanExecStreamTest, testScanPastEndOfCluster);
        FENNEL_UNIT_TEST_CASE(
            LcsRowScanExecStreamTest, testCompressedFiltering);
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testBernoulliSampling);
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testSystemSampling);
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testCount);
    }

    void testCaseSetUp();
    void testCaseTearDown();

    void testScans();
    void testScanOnEmptyCluster();
    void testScanPastEndOfCluster();
    void testCompressedFiltering();
    void testBernoulliSampling();
    void testSystemSampling();
    void testCount();
};

void LcsRowScanExecStreamTest::loadClusters(
    uint nRows,
    uint nCols,
    uint nClusters,
    bool compressed)
{
    for (uint i = 0; i < nClusters; i++) {
        boost::shared_ptr<BTreeDescriptor> pBTreeDesc =
            boost::shared_ptr<BTreeDescriptor> (new BTreeDescriptor());
        bTreeClusters.push_back(pBTreeDesc);
        loadOneCluster(
            nRows, nCols, i * nCols, *(bTreeClusters[i]), compressed);
        resetExecStreamTest();
    }
}

void LcsRowScanExecStreamTest::loadOneCluster(
    uint nRows,
    uint nCols,
    int colStart,
    BTreeDescriptor &bTreeDescriptor,
    bool compressed)
{
    MockProducerExecStreamParams mockParams;
    for (uint i = 0; i < nCols; i++) {
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    mockParams.nRows = nRows;

    // generators for input stream load

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    for (uint i = 0; i < nCols; i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(
            compressed
            ? (Int64ColumnGenerator *) new MixedDupColumnGenerator(
                NDUPS, i + colStart, 500)
            : new SeqColumnGenerator(i + colStart));
        columnGenerators.push_back(col);
    }
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGenerators));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    LcsClusterAppendExecStreamParams lcsAppendParams;
    lcsAppendParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    lcsAppendParams.pCacheAccessor = pCache;
    lcsAppendParams.pSegment = pRandomSegment;

    // initialize the btree parameter portion of lcsAppendParams
    // BTree tuple desc only has one column
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);

    // BTree key only has one column which is the first column.
    (lcsAppendParams.keyProj).push_back(0);

    // output only one value(rows inserted)
    lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);

    for (uint i = 0; i < nCols; i++) {
        lcsAppendParams.inputProj.push_back(i);
    }
    lcsAppendParams.pRootMap = 0;
    lcsAppendParams.rootPageIdParamId = DynamicParamId(0);

    // setup temporary btree descriptor to get an empty page to start the btree

    bTreeDescriptor.segmentAccessor.pSegment = lcsAppendParams.pSegment;
    bTreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    bTreeDescriptor.tupleDescriptor = lcsAppendParams.tupleDesc;
    bTreeDescriptor.keyProjection = lcsAppendParams.keyProj;
    bTreeDescriptor.rootPageId = NULL_PAGE_ID;
    lcsAppendParams.segmentId = bTreeDescriptor.segmentId;
    lcsAppendParams.pageOwnerId = bTreeDescriptor.pageOwnerId;

    BTreeBuilder builder(bTreeDescriptor, pRandomSegment);
    builder.createEmptyRoot();
    lcsAppendParams.rootPageId = bTreeDescriptor.rootPageId =
        builder.getRootPageId();

    // Now use the above initialized parameter

    LcsClusterAppendExecStream *lcsStream = new LcsClusterAppendExecStream();

    ExecStreamEmbryo lcsAppendStreamEmbryo;
    lcsAppendStreamEmbryo.init(lcsStream, lcsAppendParams);
    lcsAppendStreamEmbryo.getStream()->setName("LcsClusterAppendExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, lcsAppendStreamEmbryo);

    // set up a generator which can produce the expected output
    RampExecStreamGenerator expectedResultGenerator(mockParams.nRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void LcsRowScanExecStreamTest::testScanCols(
    uint nRows,
    uint nCols,
    uint nClusters,
    TupleProjection proj,
    uint skipRows,
    uint expectedNumRows)
{
    // setup input rid stream

    ValuesExecStreamParams valuesParams;
    boost::shared_array<FixedBuffer> pBuffer;
    ExecStreamEmbryo valuesStreamEmbryo;
    LcsRowScanExecStreamParams scanParams;

    scanParams.hasExtraFilter = false;
    scanParams.samplingMode = SAMPLING_OFF;

    // setup a values stream either to provide an empty input to simulate
    // the scan of the deletion index (in the case of a full scan) or a stream
    // of rid values when we're doing reads based on specific rids
    valuesParams.outputTupleDesc.push_back(attrDesc_int64);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);

    // set buffer size to max number of bytes required to represent each
    // bit (nRows/8) plus max number of segments (nRows/bitmapColSize)
    // times 8 bytes for each starting rid in the segment
    uint bufferSize = std::max(
        16, (int) (nRows / 8 + nRows / bitmapColSize * 8));
    pBuffer.reset(new FixedBuffer[bufferSize]);
    valuesParams.pTupleBuffer = pBuffer;

    if (nRows > 0) {
        valuesParams.bufSize = generateBitmaps(
            nRows, skipRows, valuesParams.outputTupleDesc, pBuffer.get());
        assert(valuesParams.bufSize <= bufferSize);
        scanParams.isFullScan = false;
    } else {
        scanParams.isFullScan = true;
        valuesParams.bufSize = 0;
    }
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    // setup parameters into scan
    //  nClusters cluster with nCols columns each

    for (uint i = 0; i < nClusters; i++) {
        struct LcsClusterScanDef clusterScanDef;

        for (uint j = 0; j < nCols; j++) {
            clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);
        }

        clusterScanDef.pSegment = bTreeClusters[i]->segmentAccessor.pSegment;
        clusterScanDef.pCacheAccessor =
            bTreeClusters[i]->segmentAccessor.pCacheAccessor;
        clusterScanDef.tupleDesc = bTreeClusters[i]->tupleDescriptor;
        clusterScanDef.keyProj = bTreeClusters[i]->keyProjection;
        clusterScanDef.rootPageId = bTreeClusters[i]->rootPageId;
        clusterScanDef.segmentId = bTreeClusters[i]->segmentId;
        clusterScanDef.pageOwnerId = bTreeClusters[i]->pageOwnerId;

        scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    }

    // setup projection
    scanParams.outputProj = proj;
    for (uint i = 0; i < proj.size(); i++) {
        scanParams.outputTupleDesc.push_back(attrDesc_int64);
    }

    ExecStreamEmbryo scanStreamEmbryo;
    scanStreamEmbryo.init(new LcsRowScanExecStream(), scanParams);
    scanStreamEmbryo.getStream()->setName("RowScanExecStream");
    SharedExecStream pOutputStream;

    pOutputStream =
        prepareTransformGraph(valuesStreamEmbryo, scanStreamEmbryo);

    // setup generators for result stream

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    for (uint i = 0; i < proj.size(); i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(
                new SeqColumnGenerator(
                    proj[i],
                    skipRows));
        columnGenerators.push_back(col);
    }

    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(*pOutputStream, expectedNumRows, resultGenerator);
}

int LcsRowScanExecStreamTest::generateBitmaps(
    uint nRows, uint skipRows, TupleDescriptor const &bitmapTupleDesc,
    PBuffer pBuf)
{
    int bufSize = 0;
    LbmEntry lbmEntry;
    boost::scoped_array<FixedBuffer> entryBuf;
    TupleAccessor bitmapTupleAccessor;
    LcsRid rid = LcsRid(0);

    TupleData bitmapTupleData(bitmapTupleDesc);
    bitmapTupleData[0].pData = (PConstBuffer) &rid;
    bitmapTupleData[1].pData = NULL;
    bitmapTupleData[1].cbData = 0;
    bitmapTupleData[2].pData = NULL;
    bitmapTupleData[2].cbData = 0;

    bitmapTupleAccessor.compute(bitmapTupleDesc);

    // setup an LbmEntry with the initial rid value
    uint scratchBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
    entryBuf.reset(new FixedBuffer[scratchBufSize]);
    lbmEntry.init(entryBuf.get(), NULL, scratchBufSize, bitmapTupleDesc);
    lbmEntry.setEntryTuple(bitmapTupleData);

    // add on the remaining rids
    for (rid = LcsRid(skipRows); rid < LcsRid(nRows); rid += skipRows) {
        if (!lbmEntry.setRID(LcsRid(rid))) {
            // if exhausted buffer space, write the tuple to the output
            // buffer and reset LbmEntry
            produceEntry(lbmEntry, bitmapTupleAccessor, pBuf, bufSize);
            lbmEntry.setEntryTuple(bitmapTupleData);
        }
    }
    // write out the last LbmEntry
    produceEntry(lbmEntry, bitmapTupleAccessor, pBuf, bufSize);

    return bufSize;
}

void LcsRowScanExecStreamTest::produceEntry(
    LbmEntry &lbmEntry, TupleAccessor &bitmapTupleAccessor, PBuffer pBuf,
    int &bufSize)
{
    TupleData bitmapTuple = lbmEntry.produceEntryTuple();
    bitmapTupleAccessor.marshal(bitmapTuple, pBuf + bufSize);
    bufSize += bitmapTupleAccessor.getCurrentByteCount();
}

void LcsRowScanExecStreamTest::testScans()
{
    // 1. load clusters, so they can be used by steps 2-5 below
    // 2. scan all data in clusters
    // 3. test projection
    // 4. test skipping of rows
    // 5. test full table scan

    uint nRows = 50000;
    uint nCols = 12;
    uint nClusters = 3;
    TupleProjection proj;

    loadClusters(nRows, nCols, nClusters, false);
    // note: no need to reset after loadClusters() because already done
    // there

    // scan all rows and columns
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }
    testScanCols(nRows, nCols, nClusters, proj, 1, nRows);
    resetExecStreamTest();

    // project columns 22, 10, 12, 26, 1, 35, 15, 5, 17, 30, 4, 20, 7, and 13
    proj.clear();
    proj.push_back(22);
    proj.push_back(10);
    proj.push_back(12);
    proj.push_back(26);
    proj.push_back(1);
    proj.push_back(35);
    proj.push_back(15);
    proj.push_back(5);
    proj.push_back(17);
    proj.push_back(30);
    proj.push_back(4);
    proj.push_back(20);
    proj.push_back(7);
    proj.push_back(13);

    testScanCols(nRows, nCols, nClusters, proj, 1, nRows);
    resetExecStreamTest();

    // read every 7 rows, same projection as above
    testScanCols(
        nRows, nCols, nClusters, proj, 7, (int) ceil((double) nRows / 7));
    resetExecStreamTest();


    // read every 37 rows, same projection as above
    testScanCols(
        nRows, nCols, nClusters, proj, 37, (int) ceil((double) nRows / 37));
    resetExecStreamTest();

    // full table scan -- input stream is empty
    testScanCols(0, nCols, nClusters, proj, 1, nRows);

    resetExecStreamTest();

    // scan 1000 rows and columns
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }
    testFilterCols(nRows, nCols, nClusters, proj, 1, 1000, false);

    resetExecStreamTest();

    // scan all columns execept the 1st & 2nd of the 1st & 2nd cluster
    proj.resize(0);
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            if (!(i < 2 && (j == 0 || j == 1))) {
                proj.push_back(i * nCols + j);
            }
        }
    }
    testFilterCols(nRows, nCols, nClusters, proj, 1, 1000, false);

    resetExecStreamTest();

    // skip one cluster; also setup the input so every 7 rows are skipped
    proj.resize(0);
    for (uint i = 0; i < nClusters - 1; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }
    testFilterCols(
        nRows, nCols, nClusters, proj, 7, 1000 / 7 + 1, false);
}

void LcsRowScanExecStreamTest::testCompressedFiltering()
{
    // 1. load clusters, so they can be used by steps 2-5 below
    // 2. scan all data in clusters
    // 3. test projection
    // 4. test skipping of rows
    // 5. test full table scan

    uint nRows = 50000;
    uint nCols = 12;
    uint nClusters = 3;
    TupleProjection proj;

    // Test compressed bitmap optimization
    //
    loadClusters(nRows, nCols, nClusters, true);

    // scan 500*NDUPS+500 rows and columns
    proj.resize(0);
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }
    testFilterCols(nRows, nCols, nClusters, proj, 1, 500*NDUPS+500, true);

    resetExecStreamTest();

    // scan all columns execept the 1st & 2nd of the 1st & 2nd cluster
    proj.resize(0);
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            if (!(i < 2 && (j == 0 || j == 1))) {
                proj.push_back(i * nCols + j);
            }
        }
    }
    testFilterCols(nRows, nCols, nClusters, proj, 1, 500*NDUPS+500, true);

    resetExecStreamTest();

    // skip one cluster
    proj.resize(0);
    for (uint i = 0; i < nClusters - 1; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }
    testFilterCols(nRows, nCols, nClusters, proj, 1, 500*NDUPS+500, true);
}

void LcsRowScanExecStreamTest::testCount()
{
    uint nRows = 50000;
    uint nCols = 12;
    uint nClusters = 3;

    loadClusters(nRows, nCols, nClusters, true);

    // scan 500*NDUPS+500 rows and columns
    TupleProjection proj;
    proj.push_back(LCS_RID_COLUMN_ID);
    LcsCountAggExecStreamParams countParams;
    testFilterCols(
        nRows, nCols, nClusters, proj, 1, 500*NDUPS+500, true,
        &countParams);
}

/**
 * Create an empty cluster with 1 column.  Try reading a rid from it
 */
void LcsRowScanExecStreamTest::testScanOnEmptyCluster()
{
    // create empty btree

    BTreeDescriptor &bTreeDescriptor = *(bTreeClusters[0]);

    bTreeDescriptor.segmentAccessor.pSegment = pRandomSegment;
    bTreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    bTreeDescriptor.tupleDescriptor.push_back(attrDesc_int64);
    bTreeDescriptor.tupleDescriptor.push_back(attrDesc_int64);
    bTreeDescriptor.keyProjection.push_back(0);
    bTreeDescriptor.rootPageId = NULL_PAGE_ID;

    BTreeBuilder builder(bTreeDescriptor, pRandomSegment);
    builder.createEmptyRoot();
    bTreeDescriptor.rootPageId = builder.getRootPageId();

    // have testScanCols attempt to scan a single row, although it should
    // return no rows

    TupleProjection proj;

    proj.push_back(0);
    testScanCols(1, 1, 1, proj, 1, 0);
}

/**
 * Create a cluster with only a single row.  Do a rid search on a rid value
 * larger than what's in the table
 */
void LcsRowScanExecStreamTest::testScanPastEndOfCluster()
{
    loadOneCluster(1, 1, 0, *(bTreeClusters[0]), false);
    resetExecStreamTest();

    // have testScanCols attempt to read 2 rows, although it should only
    // be able to read 1

    TupleProjection proj;

    proj.push_back(0);
    testScanCols(2, 1, 1, proj, 1, 1);
}

/**
 * Configure Bernoulli sampling, with a specific seed and verify that the
 * expected number of rows are returned.
 */
void LcsRowScanExecStreamTest::testBernoulliSampling()
{
    uint nRows = 50000;
    uint nCols = 12;
    uint nClusters = 3;
    TupleProjection proj;

    int seed = 19721212;
    float rate = 0.1;
    TableSamplingMode mode = SAMPLING_BERNOULLI;

    loadClusters(nRows, nCols, nClusters, false);
    // note: no need to reset after loadClusters() because already done
    // there

    // scan all rows and columns
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }

    // Full Row Scan (4938 is based on the seed, but determine empirically)
    testSampleScanCols(
        0, nRows, nCols, nClusters, proj, 1, mode, rate, seed, 0, 4938);
    resetExecStreamTest();

    // Skip every other row
    testSampleScanCols(
        nRows, nRows, nCols, nClusters, proj, 2, mode, rate, seed, 0, 2489);
    resetExecStreamTest();
}


/**
 * Configure system sampling, with a specific clump size and verify that the
 * expected number of rows are returned.
 */
void LcsRowScanExecStreamTest::testSystemSampling()
{
    uint nRows = 50000;
    uint nCols = 12;
    uint nClusters = 3;
    TupleProjection proj;

    TableSamplingMode mode = SAMPLING_SYSTEM;

    loadClusters(nRows, nCols, nClusters, false);
    // note: no need to reset after loadClusters() because already done
    // there

    // scan all rows and columns
    for (uint i = 0; i < nClusters; i++) {
        for (uint j = 0; j < nCols; j++) {
            proj.push_back(i * nCols + j);
        }
    }

    testSampleScanCols(
        nRows, nRows, nCols, nClusters, proj, 1, mode, 0.1, -1, 10, 5000);
    resetExecStreamTest();

    testSampleScanCols(
        nRows, nRows, nCols, nClusters, proj, 1, mode, 1.0, -1, 10, 50000);
    resetExecStreamTest();

    testSampleScanCols(
        nRows, nRows, nCols, nClusters, proj, 1, mode, 0.33333, -1, 10, 16670);
    resetExecStreamTest();
}

void LcsRowScanExecStreamTest::setSearchKey(
    char lowerDirective, char upperDirective, uint64_t lowerVal,
    uint64_t upperVal, PBuffer inputBuf, uint &offset,
    TupleAccessor &inputTupleAccessor, TupleData &inputTupleData)
{
    inputTupleData[0].pData = (PConstBuffer) &lowerDirective;
    inputTupleData[2].pData = (PConstBuffer) &upperDirective;
    inputTupleData[1].pData = (PConstBuffer) &lowerVal;
    inputTupleData[3].pData = (PConstBuffer) &upperVal;
    inputTupleAccessor.marshal(inputTupleData, inputBuf + offset);
    offset += inputTupleAccessor.getCurrentByteCount();
}

void LcsRowScanExecStreamTest::testFilterCols(
    uint nRows,
    uint nCols,
    uint nClusters,
    TupleProjection proj,
    uint skipRows,
    uint expectedNumRows,
    bool compressed,
    LcsCountAggExecStreamParams *pCountParams)
{
    // setup input rid stream

    ValuesExecStreamParams valuesParams;
    boost::shared_array<FixedBuffer> pBuffer;
    ExecStreamEmbryo valuesStreamEmbryo;

    LcsRowScanExecStreamParams rowScanParams;
    LcsRowScanExecStreamParams &scanParams =
        pCountParams ? *pCountParams : rowScanParams;

    scanParams.hasExtraFilter = true;
    scanParams.samplingMode = SAMPLING_OFF;

    // setup a values stream either to provide an empty input to simulate
    // the scan of the deletion index (in the case of a full scan) or a stream
    // of rid values when we're doing reads based on specific rids
    valuesParams.outputTupleDesc.push_back(attrDesc_int64);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);


    // set buffer size to max number of bytes required to represent each
    // bit (nRows/8) plus max number of segments (nRows/bitmapColSize)
    // times 8 bytes for each starting rid in the segment
    uint bufferSize = std::max(
        16, (int) (nRows / 8 + nRows / bitmapColSize * 8));
    pBuffer.reset(new FixedBuffer[bufferSize]);
    valuesParams.pTupleBuffer = pBuffer;

    if (nRows > 0) {
        valuesParams.bufSize = generateBitmaps(
            nRows, skipRows, valuesParams.outputTupleDesc, pBuffer.get());
        assert(valuesParams.bufSize <= bufferSize);
        scanParams.isFullScan = false;
    } else {
        scanParams.isFullScan = true;
        valuesParams.bufSize = 0;
    }
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    // setup the following search keys:
    // 1. key0 >= 2000 or key0 < 1000
    // 2. 500 <= key1 - nCols < 2999 or  (key1 - nCols) == 2999
    // 3  key2 - 2*nCols > 1500
    //
    // where key0 corresponds to column #0,
    // key1 corresponds to the column #nCols, and
    // key2 corresponds to column #(2*nCols)

    TupleAttributeDescriptor attrDesc_nullableInt64 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_64),
            true, sizeof(uint64_t));

    valuesParams.outputTupleDesc.resize(0);
    TupleDescriptor inputTupleDesc;
    for (uint i = 0; i < 2; i++) {
        inputTupleDesc.push_back(attrDesc_char1);
        inputTupleDesc.push_back(attrDesc_nullableInt64);
        valuesParams.outputTupleDesc.push_back(attrDesc_char1);
        valuesParams.outputTupleDesc.push_back(attrDesc_nullableInt64);
    }
    TupleData inputTupleData(inputTupleDesc);
    TupleAccessor inputTupleAccessor;
    inputTupleAccessor.compute(inputTupleDesc);

    uint nInputTuples = 3;
    boost::shared_array<FixedBuffer> inputBuffer;
    inputBuffer.reset(
        new FixedBuffer[nInputTuples * inputTupleAccessor.getMaxByteCount()]);

    PBuffer inputBuf = inputBuffer.get();
    uint offset = 0;

    setSearchKey(
        '-', ')', 0, 1000, inputBuf, offset, inputTupleAccessor,
        inputTupleData);
    setSearchKey(
        '[', '+', 2000, 0, inputBuf, offset, inputTupleAccessor,
        inputTupleData);

    TupleData inputTupleData1(inputTupleDesc);
    boost::shared_array<FixedBuffer> inputBuffer1;
    inputBuffer1.reset(
        new FixedBuffer[nInputTuples * inputTupleAccessor.getMaxByteCount()]);
    PBuffer inputBuf1 = inputBuffer1.get();
    uint offset1 = 0;

    setSearchKey(
        '[', ')', 500 + nCols, 2999 + nCols, inputBuf1, offset1,
        inputTupleAccessor,
        inputTupleData1);
    setSearchKey(
        '[', ']', 2999 + nCols, 2999 + nCols, inputBuf1, offset1,
        inputTupleAccessor, inputTupleData1);

    TupleData inputTupleData2(inputTupleDesc);
    boost::shared_array<FixedBuffer> inputBuffer2;
    inputBuffer2.reset(
        new FixedBuffer[nInputTuples * inputTupleAccessor.getMaxByteCount()]);
    PBuffer inputBuf2 = inputBuffer2.get();
    uint offset2 = 0;

    setSearchKey(
        '(', '+', 1500 + 2 * nCols, 0, inputBuf2, offset2, inputTupleAccessor,
        inputTupleData1);

    valuesParams.pTupleBuffer = inputBuffer;
    valuesParams.bufSize = offset;

    ExecStreamEmbryo valuesStreamEmbryo1,  valuesStreamEmbryo2,
        valuesStreamEmbryo3;
    valuesStreamEmbryo1.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo1.getStream()->setName("ValuesExecStream1");

    valuesParams.pTupleBuffer = inputBuffer1;
    valuesParams.bufSize = offset1;
    valuesStreamEmbryo2.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo2.getStream()->setName("ValuesExecStream2");

    valuesParams.pTupleBuffer = inputBuffer2;
    valuesParams.bufSize = offset2;
    valuesStreamEmbryo3.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo3.getStream()->setName("ValuesExecStream3");

    // setup parameters into scan
    //  nClusters cluster with nCols columns each

    for (uint i = 0; i < nClusters; i++) {
        struct LcsClusterScanDef clusterScanDef;

        for (uint j = 0; j < nCols; j++) {
            clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);
        }

        clusterScanDef.pSegment = bTreeClusters[i]->segmentAccessor.pSegment;
        clusterScanDef.pCacheAccessor =
            bTreeClusters[i]->segmentAccessor.pCacheAccessor;
        clusterScanDef.tupleDesc = bTreeClusters[i]->tupleDescriptor;
        clusterScanDef.keyProj = bTreeClusters[i]->keyProjection;
        clusterScanDef.rootPageId = bTreeClusters[i]->rootPageId;
        clusterScanDef.segmentId = bTreeClusters[i]->segmentId;
        clusterScanDef.pageOwnerId = bTreeClusters[i]->pageOwnerId;

        scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    }

    // setup projection
    scanParams.outputProj = proj;
    for (uint i = 0; i < proj.size(); i++) {
        scanParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    scanParams.residualFilterCols.push_back(0);
    scanParams.residualFilterCols.push_back(nCols);
    scanParams.residualFilterCols.push_back(2*nCols);

    ExecStreamEmbryo scanStreamEmbryo;
    if (pCountParams) {
        scanStreamEmbryo.init(new LcsCountAggExecStream(), *pCountParams);
        scanStreamEmbryo.getStream()->setName("CountAggExecStream");
    } else {
        scanStreamEmbryo.init(new LcsRowScanExecStream(), rowScanParams);
        scanStreamEmbryo.getStream()->setName("RowScanExecStream");
    }
    SharedExecStream pOutputStream;

    std::vector<ExecStreamEmbryo> sources;
    sources.push_back(valuesStreamEmbryo);
    sources.push_back(valuesStreamEmbryo1);
    sources.push_back(valuesStreamEmbryo2);
    sources.push_back(valuesStreamEmbryo3);

    pOutputStream =
        prepareConfluenceGraph(sources, scanStreamEmbryo);

    if (pCountParams) {
        RampExecStreamGenerator countResultGenerator(expectedNumRows);
        verifyOutput(*pOutputStream, 1, countResultGenerator);
        return;
    }

    // setup generators for result stream
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    offset = (int) ceil(2000.0 / skipRows) * skipRows;
    for (uint i = 0; i < proj.size(); i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(
                compressed
                ? (Int64ColumnGenerator*) new MixedDupColumnGenerator(
                        NDUPS, proj[i] + 2000, 500)
                : new SeqColumnGenerator(proj[i] + offset, skipRows));
        columnGenerators.push_back(col);
    }


    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(*pOutputStream, expectedNumRows, resultGenerator);
}


void LcsRowScanExecStreamTest::testSampleScanCols(
    uint nRows,
    uint nRowsActual,
    uint nCols,
    uint nClusters,
    TupleProjection proj,
    uint skipRows,
    TableSamplingMode mode,
    float rate,
    int seed,
    uint clumps,
    uint expectedNumRows)
{
    // setup input rid stream

    ValuesExecStreamParams valuesParams;
    boost::shared_array<FixedBuffer> pBuffer;
    ExecStreamEmbryo valuesStreamEmbryo;
    LcsRowScanExecStreamParams scanParams;

    scanParams.hasExtraFilter = false;

    // setup a values stream either to provide an empty input to simulate
    // the scan of the deletion index (in the case of a full scan) or a stream
    // of rid values when we're doing reads based on specific rids
    valuesParams.outputTupleDesc.push_back(attrDesc_int64);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);

    uint nRowsInternal = (mode == SAMPLING_SYSTEM) ? 0 : nRows;

    // set buffer size to max number of bytes required to represent each bit
    // (nRowsInternal/8) plus max number of segments
    // (nRowsInternal/bitmapColSize) times 8 bytes for each starting rid in the
    // segment
    uint bufferSize = std::max(
        16, (int) (nRowsInternal / 8 + nRowsInternal / bitmapColSize * 8));
    pBuffer.reset(new FixedBuffer[bufferSize]);
    valuesParams.pTupleBuffer = pBuffer;

    if (nRowsInternal > 0) {
        valuesParams.bufSize = generateBitmaps(
            nRowsInternal, skipRows, valuesParams.outputTupleDesc,
            pBuffer.get());
        assert(valuesParams.bufSize <= bufferSize);
        scanParams.isFullScan = false;
    } else {
        scanParams.isFullScan = true;
        valuesParams.bufSize = 0;
    }
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    // setup parameters into scan
    //  nClusters cluster with nCols columns each

    for (uint i = 0; i < nClusters; i++) {
        struct LcsClusterScanDef clusterScanDef;

        for (uint j = 0; j < nCols; j++) {
            clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);
        }

        clusterScanDef.pSegment = bTreeClusters[i]->segmentAccessor.pSegment;
        clusterScanDef.pCacheAccessor =
            bTreeClusters[i]->segmentAccessor.pCacheAccessor;
        clusterScanDef.tupleDesc = bTreeClusters[i]->tupleDescriptor;
        clusterScanDef.keyProj = bTreeClusters[i]->keyProjection;
        clusterScanDef.rootPageId = bTreeClusters[i]->rootPageId;
        clusterScanDef.segmentId = bTreeClusters[i]->segmentId;
        clusterScanDef.pageOwnerId = bTreeClusters[i]->pageOwnerId;

        scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    }

    // setup projection
    scanParams.outputProj = proj;
    for (uint i = 0; i < proj.size(); i++) {
        scanParams.outputTupleDesc.push_back(attrDesc_int64);
    }


    // setup sampling
    scanParams.samplingMode = mode;
    scanParams.samplingRate = rate;
    scanParams.samplingIsRepeatable = true;
    scanParams.samplingRepeatableSeed = seed;
    scanParams.samplingClumps = clumps;
    scanParams.samplingRowCount = nRowsActual;

    ExecStreamEmbryo scanStreamEmbryo;
    scanStreamEmbryo.init(new LcsRowScanExecStream(), scanParams);
    scanStreamEmbryo.getStream()->setName("RowScanExecStream");
    SharedExecStream pOutputStream;

    pOutputStream =
        prepareTransformGraph(valuesStreamEmbryo, scanStreamEmbryo);

    // setup generators for result stream

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    for (uint i = 0; i < proj.size(); i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(
                new SeqColumnGenerator(
                    proj[i],
                    skipRows));
        columnGenerators.push_back(col);
    }

    boost::shared_ptr<CompositeExecStreamGenerator> baseResultGenerator(
        new CompositeExecStreamGenerator(columnGenerators));

    if (mode == SAMPLING_BERNOULLI) {
        BernoulliSamplingExecStreamGenerator resultGenerator(
            baseResultGenerator,
            rate,
            seed,
            proj.size());

        verifyOutput(*pOutputStream, expectedNumRows, resultGenerator);
    } else {
        SystemSamplingExecStreamGenerator resultGenerator(
            baseResultGenerator,
            rate,
            nRows,
            proj.size(),
            clumps);

        verifyOutput(*pOutputStream, expectedNumRows, resultGenerator);
    }
}

void LcsRowScanExecStreamTest::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();

    attrDesc_char1 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_CHAR), false, 1);
    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    bitmapColSize = pRandomSegment->getUsablePageSize() / 8;
    attrDesc_bitmap = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true, bitmapColSize);
}

void LcsRowScanExecStreamTest::testCaseTearDown()
{
    for (uint i = 0; i < bTreeClusters.size(); i++) {
        bTreeClusters[i]->segmentAccessor.reset();
    }
    ExecStreamUnitTestBase::testCaseTearDown();
}

FENNEL_UNIT_TEST_SUITE(LcsRowScanExecStreamTest);


// End LcsRowScanExecStreamTest.cpp

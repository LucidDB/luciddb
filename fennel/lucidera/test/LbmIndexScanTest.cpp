/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/lucidera/colstore/LcsClusterAppendExecStream.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/lucidera/sorter/ExternalSortExecStream.h"
#include "fennel/lucidera/bitmap/LbmGeneratorExecStream.h"
#include "fennel/lucidera/bitmap/LbmSplicerExecStream.h"
#include "fennel/lucidera/bitmap/LbmIndexScanExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/cache/Cache.h"
#include "fennel/common/SearchEndpoint.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for scanning a single bitmap index using equality search on
 * all index keys
 */
class LbmIndexScanTest : public ExecStreamUnitTestBase
{
protected:
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_char1;
    TupleAttributeDescriptor attrDesc_int64;

    /**
     * BTrees corresponding to the clusters
     */
    vector<boost::shared_ptr<BTreeDescriptor> > bTreeClusters;
    
    /**
     * Saved root pageids of btrees corresponding to clusters; used to
     * append to existing table and to read from them
     */
    vector<PageId> savedBTreeClusterRootIds;

    /**
     * BTrees corresponding to the bitmaps
     */
    vector<boost::shared_ptr<BTreeDescriptor> > bTreeBitmaps;

    /**
     * Saved root pageids of btrees corresponding to bitmaps; used to
     * append to existing table and to read from them
     */
    vector<PageId> savedBTreeBitmapRootIds;

    /**
     * Initializes a BTreeExecStreamParam structure
     */
    void initBTreeExecStreamParam(
        BTreeExecStreamParams &param, shared_ptr<BTreeDescriptor> pBTreeDesc);

    /**
     * Initializes a cluster scan def structure for a LcsRowScanBase exec
     * stream
     */
    void initClusterScanDef(
        LcsRowScanBaseExecStreamParams &generatorParams,
        struct LcsClusterScanDef &clusterScanDef, uint bTreeIndex);

    /**
     * Initializes BTreeExecStreamParam corresponding to a bitmap index
     *
     * @param nKeys number of keys in the bitmap index; excludes start
     * rid from key count 
     */
    void  initBTreeBitmapDesc(
        TupleDescriptor &param, TupleProjection &keyProj, uint nKeys);

    /**
     * Initializes a tuple descriptor corresponding to a bitmap index
     *
     * @param nKeys number of keys in the bitmap index; excludes start
     * rid from key count
     */
    void initBTreeTupleDesc(TupleDescriptor &tupleDesc, uint nKeys);

    /**
     * Loads a table with nClusters clusters, 1 column per cluster, and nRows
     * rows.
     * 
     *<p>

     * Each column has a repeating sequence of values based on the value in the
     * repeatSeqValues vector.  E.g., a repeating sequence of n will have
     * values:
     *
     * (0, 1, 2, ..., n-1, 0, 1, 2, ..., n-1, 0, 1, 2, ...).
     *  
     * Bitmap indexes are then created on each column as well as a multi-key
     * index that is created on all columns.
     *
     * @param nRows number of rows to load
     * @param nClusters number of clusters to create
     * @param repeatSeqValues repeating sequence values for each column
     * @param newRoot if true, append to existing table
     */
    void loadTableAndIndexes(
        uint nRows, uint nClusters, std::vector<int> const &repeatSeqValues, 
        bool newRoot);

    /**
     * Tests scans on the table created in loadTableAndIndexes, scanning
     * through each of the indexes
     *
     * @param nRows total number of rows in the table
     * @param nClusters number of clusters on the table
     * @param repeatSeqValues1 initial repeating sequence values for each column
     * @param repeatSeqValues2 second set of repeating sequence values for
     * each column appended to the table; if no append was done, this is
     * an empty list
     */
    void testScan(
        uint nRows, uint nClusters, std::vector<int> const &repeatSeqValues1,
        std::vector<int> const &repeatSeqValues2);

    /**
     * Performs equality searches on a single index
     *
     * @param idxToScan ordinal representing index to scan; if equal to
     * nClusters, this represents the multi-key index; otherwise, should be
     * a single column index
     *
     * @param nClusters number of clusters in the table on which the index
     * is created
     *
     * @param nInputTuples number of search/key directive tuples
     *
     * @param inputBuffer buffer containing search key/directive tuples to
     * be passed into the index scan
     *
     * @param proj columns to be projected in the scan
     *
     * @param expectedNRows number of rows the scan should return
     *
     * @param expectedVals expected column values in scan result
     */
    void testScanIdx(
        uint idxToScan, uint nClusters, uint bufSize, PBuffer inputBuffer,
        TupleProjection const &proj, uint expectedNRows,
        boost::scoped_array<uint64_t> const &expectedVals);

    /**
     * Initializes input search key and directives for an equality search
     *
     * @param nKeys number of keys to search on
     * @param nInputTuples number of search ranges to create
     * @param vals values to search on
     * @param lowerDirective lower bound search directive
     * @param upperDirective upper bound search directive
     * @param inputTupleAccessor accessor to marshal/unmarshal search key
     * @param inputTupleData tupledata storing search key
     * @param inputBuffer buffer storing search key
     */
    void initEqualSearch(
        uint nKeys, uint nInputTuples, boost::scoped_array<uint64_t> &vals, 
        char &lowerDirective, char &upperDirective,
        TupleAccessor &inputTupleAccessor, TupleData &inputTupleData,
        boost::scoped_array<FixedBuffer> &inputBuffer);

public:
    explicit LbmIndexScanTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmIndexScanTest, testScan10000);
        FENNEL_UNIT_TEST_CASE(LbmIndexScanTest, testAppendScan);
    }

    void testCaseSetUp();
    void testCaseTearDown();

    void testScan10000();
    void testAppendScan();
};

void LbmIndexScanTest::testScan10000()
{
    uint nRows = 10000;
    uint nClusters = 4; 
    std::vector<int> repeatSeqValues;

    // load the data
    repeatSeqValues.push_back(nRows);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);
    repeatSeqValues.push_back(19);
    loadTableAndIndexes(nRows, nClusters, repeatSeqValues, true);

    // scan each index
    std::vector<int> emptySeqValues;
    testScan(nRows, nClusters, repeatSeqValues, emptySeqValues);
}

void LbmIndexScanTest::testAppendScan()
{
    uint nRows = 100;
    uint nClusters = 4; 
    std::vector<int> repeatSeqValues1;
    std::vector<int> repeatSeqValues2;

    // Set up the column generators for empty load and append.

    // column 1
    repeatSeqValues1.push_back(nRows);
    repeatSeqValues2.push_back(nRows);

    // column 2
    repeatSeqValues1.push_back(23);
    repeatSeqValues2.push_back(31);

    // column 3
    repeatSeqValues1.push_back(1);
    repeatSeqValues2.push_back(2);

    // column 4
    repeatSeqValues1.push_back(7);
    repeatSeqValues2.push_back(29);

    // load into empty btree
    loadTableAndIndexes(nRows, nClusters, repeatSeqValues1, true);

    // append some new values
    resetExecStreamTest();
    loadTableAndIndexes(nRows, nClusters, repeatSeqValues2, false);

    testScan(nRows * 2, nClusters, repeatSeqValues1, repeatSeqValues2);
}

void LbmIndexScanTest::testScan(
    uint nRows, uint nClusters, std::vector<int> const &repeatSeqValues1,
    std::vector<int> const &repeatSeqValues2)
{
    // search for key = <value>
    uint nKeys = 1;
    uint nInputTuples = 1;
    boost::scoped_array<uint64_t> vals;
    char lowerDirective;
    char upperDirective;
    TupleAccessor inputTupleAccessor;
    TupleData inputTupleData;
    boost::scoped_array<FixedBuffer> inputBuffer;

    initEqualSearch(
        nKeys, nInputTuples, vals, lowerDirective, upperDirective,
        inputTupleAccessor, inputTupleData, inputBuffer);

    // scan through each of the single column indexes,
    // searching for each of the possible values in the column
    TupleProjection proj;
    proj.push_back(0);
    for (uint i = 0; i < nClusters; i++) {
        uint maxVal;
        if (repeatSeqValues2.size() == 0) {
            maxVal = repeatSeqValues1[i];
        } else {
            maxVal = std::max(repeatSeqValues1[i], repeatSeqValues2[i]);
        }
        for (uint j = 0; j < maxVal; j++) {
            vals[0] = j;
            inputTupleAccessor.marshal(inputTupleData, inputBuffer.get());
            // number of rows depends on start value, sequence used, 
            // and whether 1 or 2 sets of sequences were used
            uint expectedNRows = 0;
            if (repeatSeqValues2.size() == 0) {
                expectedNRows +=
                    (nRows - 1 - vals[0]) / repeatSeqValues1[i] + 1;
            } else {
                if (j < repeatSeqValues1[i]) {
                    expectedNRows +=
                        (nRows/2 - 1 - vals[0]) / repeatSeqValues1[i] + 1;
                }
                if (j < repeatSeqValues2[i]) {
                    expectedNRows +=
                        (nRows/2 - 1 - vals[0]) / repeatSeqValues2[i] + 1;
                }
            }
            testScanIdx(
                i, nClusters, inputTupleAccessor.getCurrentByteCount(),
                inputBuffer.get(), proj, expectedNRows, vals);
        }
    }

    // search for key0=<val0>, key1=<val1>, etc.
    nKeys = nClusters;
    initEqualSearch(
        nKeys, nInputTuples, vals, lowerDirective, upperDirective,
        inputTupleAccessor, inputTupleData, inputBuffer);

    // scan through each of the columns in the index; determine the possible
    // key value combinations by generating the possible key values; this
    // array will then be used to determine how many occurrences exist for a 
    // particular set of key values
    proj.clear();
    for (uint i = 0; i < nClusters; i++) {
        proj.push_back(i);
    }

    // do lookups on the first set of sequence values; see if the same
    // key values exist in the second sequence; note that we take advantage
    // of the fact that the first column always contains sequential values
    uint n;
    n = (repeatSeqValues2.size() == 0) ? 1 : 2;
    for (uint i = 0; i < nRows/n; i++) {
        for (uint j = 0; j < nClusters; j++) {
            vals[j] = i % repeatSeqValues1[j];
        }
        inputTupleAccessor.marshal(inputTupleData, inputBuffer.get());
        uint expectedNRows = 1;
        if (repeatSeqValues2.size() > 0) {
            uint j;
            for (j = 0; j < nClusters; j++) {
                if (vals[j] != i % repeatSeqValues2[j]) {
                    break;
                }
            }
            if (j == nClusters) {
                expectedNRows++;
            }
        }
        testScanIdx(
            nClusters, nClusters, inputTupleAccessor.getCurrentByteCount(),
            inputBuffer.get(), proj, expectedNRows, vals);
    }
    // now, do a lookup on the second set of sequence values; ignoring the
    // ones we've already looked up
    if (repeatSeqValues2.size() > 0) {
        for (uint i = 0; i < nRows/n; i++) {
            for (uint j = 0; j < nClusters; j++) {
                vals[j] = i % repeatSeqValues2[j];
            }
            uint j;
            for (j = 0; j < nClusters; j++) {
                if (vals[j] != i % repeatSeqValues1[j]) {
                    break;
                }
            }
            if (j == nClusters) {
                continue;
            }
            inputTupleAccessor.marshal(inputTupleData, inputBuffer.get());
            testScanIdx(
                nClusters, nClusters, inputTupleAccessor.getCurrentByteCount(),
                inputBuffer.get(), proj, 1, vals);
        }
    }
}

void LbmIndexScanTest::initEqualSearch(
    uint nKeys, uint nInputTuples, boost::scoped_array<uint64_t> &vals, 
    char &lowerDirective, char &upperDirective,
    TupleAccessor &inputTupleAccessor, TupleData &inputTupleData,
    boost::scoped_array<FixedBuffer> &inputBuffer)
{
    TupleDescriptor inputTupleDesc;
    for (uint i = 0; i < 2; i++) {
        inputTupleDesc.push_back(attrDesc_char1);
        for (uint j = 0; j < nKeys; j++) {
            inputTupleDesc.push_back(attrDesc_int64);
        }
    }

    inputTupleData.compute(inputTupleDesc);

    vals.reset(new uint64_t[nKeys]);
    lowerDirective = '[';
    inputTupleData[0].pData = (PConstBuffer) &lowerDirective;
    upperDirective = ']';
    inputTupleData[nKeys + 1].pData = (PConstBuffer) &upperDirective;
    for (uint i = 0; i < nKeys; i++) {
        inputTupleData[i + 1].pData = (PConstBuffer) &vals[i];
        inputTupleData[nKeys + 1 + i + 1].pData = (PConstBuffer) &vals[i];
    }

    inputTupleAccessor.compute(inputTupleDesc);

    inputBuffer.reset(
        new uint8_t[nInputTuples * inputTupleAccessor.getMaxByteCount()]);
}

void LbmIndexScanTest::loadTableAndIndexes(
    uint nRows, uint nClusters, std::vector<int> const &repeatSeqValues,
    bool newRoot)
{
    // Logic in testScan() in the append case depends on the following
    // condition
    assert(repeatSeqValues[0] == nRows);

    // 0. reset member fields.
    for (uint i = 0; i < bTreeClusters.size(); i++) {
        bTreeClusters[i]->segmentAccessor.reset();
    }
    for (uint i = 0; i < bTreeBitmaps.size(); i++) {
        bTreeBitmaps[i]->segmentAccessor.reset();
    }
    bTreeClusters.clear();
    bTreeBitmaps.clear();

    // 1. setup mock input stream
    
    MockProducerExecStreamParams mockParams;
    for (uint i = 0; i < nClusters; i++) {
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    mockParams.nRows = nRows;

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    SharedInt64ColumnGenerator col;
    assert(repeatSeqValues.size() == nClusters);
    for (uint i = 0; i < repeatSeqValues.size(); i++) {
        col =
            SharedInt64ColumnGenerator(
                new RepeatingSeqColumnGenerator(repeatSeqValues[i]));
        columnGenerators.push_back(col);
    }
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGenerators));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // 2. setup splitter stream for cluster loads

    SplitterExecStreamParams splitterParams;
    ExecStreamEmbryo splitterStreamEmbryo;
    splitterStreamEmbryo.init(new SplitterExecStream(), splitterParams);
    splitterStreamEmbryo.getStream()->setName("ClusterSplitterExecStream");

    // 3. setup loader streams
    
    vector<ExecStreamEmbryo> lcsAppendEmbryos;
    for (uint i = 0; i < nClusters; i++) {
    
        LcsClusterAppendExecStreamParams lcsAppendParams;
        boost::shared_ptr<BTreeDescriptor> pBTreeDesc =
            boost::shared_ptr<BTreeDescriptor> (new BTreeDescriptor());
        bTreeClusters.push_back(pBTreeDesc);

        // initialize the btree parameter portion of lcsAppendParams
        // BTree tuple desc has two columns (rid, clusterPageid)
        (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);
        (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);

        // BTree key only has one column which is the first column.
        (lcsAppendParams.keyProj).push_back(0);

        initBTreeExecStreamParam(lcsAppendParams, pBTreeDesc);

        // output two values (rows inserted, starting rid value)
        lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);
        lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);

        lcsAppendParams.inputProj.push_back(i);
        lcsAppendParams.overwrite = false;

        // create an empty page to start the btree

        if (newRoot) {
            BTreeBuilder builder(*pBTreeDesc, pRandomSegment);
            builder.createEmptyRoot();
            savedBTreeClusterRootIds.push_back(builder.getRootPageId());
        }
        lcsAppendParams.rootPageId = pBTreeDesc->rootPageId =
            savedBTreeClusterRootIds[i];

        // Now use the above initialized parameter 
     
        ExecStreamEmbryo lcsAppendStreamEmbryo;
        lcsAppendStreamEmbryo.init(
            new LcsClusterAppendExecStream(), lcsAppendParams);
        std::ostringstream oss;
        oss << "LcsClusterAppendExecStream" << "#" << i;
        lcsAppendStreamEmbryo.getStream()->setName(oss.str());
        lcsAppendEmbryos.push_back(lcsAppendStreamEmbryo);
    }

    // 4. setup barrier stream for cluster loads
    
    BarrierExecStreamParams barrierParams;
    barrierParams.outputTupleDesc.push_back(attrDesc_int64);
    barrierParams.outputTupleDesc.push_back(attrDesc_int64);

    ExecStreamEmbryo clusterBarrierStreamEmbryo;
    clusterBarrierStreamEmbryo.init(new BarrierExecStream(), barrierParams);
    clusterBarrierStreamEmbryo.getStream()->setName("ClusterBarrierExecStream");

    // create a DAG with the above, but without the final output sink
    prepareDAG(
        mockStreamEmbryo, splitterStreamEmbryo, lcsAppendEmbryos,
        clusterBarrierStreamEmbryo, false);

    // 5. setup splitter stream for create bitmaps

    splitterStreamEmbryo.init(
        new SplitterExecStream(), splitterParams);
    splitterStreamEmbryo.getStream()->setName("BitmapSplitterExecStream");

    // create streams for bitmap generator, sort, and bitmap splicer,
    // 1 index on each column and then an index on all columns
   
    std::vector<std::vector<ExecStreamEmbryo> > createBitmapStreamList;
    for (uint i = 0; i < nClusters + 1; i++) {

        if (i == 1 && nClusters == 1) {
            /*
             * There's only one column. 
             * Do not bother to build the composite index.
             */
            break;
        }

        std::vector<ExecStreamEmbryo> createBitmapStream;

        // 6. setup generator
        
        LbmGeneratorExecStreamParams generatorParams;
        struct LcsClusterScanDef clusterScanDef;
        clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);

        // first nCluster generators only scan a single column; the
        // last one scans all columns
        if (i < nClusters) {
            initClusterScanDef(generatorParams, clusterScanDef, i);
        } else {
            for (uint j = 0; j < nClusters; j++) {
                initClusterScanDef(generatorParams, clusterScanDef, j);
            }
        }

        TupleProjection proj;
        if (i < nClusters) {
            proj.push_back(0);
        } else {
            for (uint j = 0; j < nClusters; j++) {
                proj.push_back(j);
            }
        }
        generatorParams.outputProj = proj;
        generatorParams.dynParamId = DynamicParamId(i + 1);

        boost::shared_ptr<BTreeDescriptor> pBTreeDesc =
            boost::shared_ptr<BTreeDescriptor> (new BTreeDescriptor());
        bTreeBitmaps.push_back(pBTreeDesc);

        // BTree tuple desc has the key columns + starting Rid + varbinary
        // field for bit segments/bit descriptors
        uint nKeys;
        if (i < nClusters) {
            nKeys = 1;
        } else {
            nKeys = nClusters;
        }
        initBTreeTupleDesc(generatorParams.outputTupleDesc, nKeys);

        initBTreeBitmapDesc(
            generatorParams.tupleDesc, generatorParams.keyProj, nKeys);
        initBTreeExecStreamParam(generatorParams, pBTreeDesc);

        // create an empty page to start the btree

        if (newRoot) {
            BTreeBuilder builder(*pBTreeDesc, pRandomSegment);
            builder.createEmptyRoot();
            savedBTreeBitmapRootIds.push_back(builder.getRootPageId());
        }
        generatorParams.rootPageId = pBTreeDesc->rootPageId =
            savedBTreeBitmapRootIds[i];

        ExecStreamEmbryo generatorStreamEmbryo;
        generatorStreamEmbryo.init(
            new LbmGeneratorExecStream(), generatorParams);
        std::ostringstream oss;
        oss << "LbmGeneratorExecStream" << "#" << i;
        generatorStreamEmbryo.getStream()->setName(oss.str());
        createBitmapStream.push_back(generatorStreamEmbryo);

        // 7. setup sorter
        
        ExternalSortExecStreamParams sortParams;
        initBTreeBitmapDesc(
            sortParams.outputTupleDesc, sortParams.keyProj, nKeys);
        sortParams.distinctness = DUP_ALLOW;
        sortParams.pTempSegment = pRandomSegment;
        sortParams.pCacheAccessor = pCache;
        sortParams.scratchAccessor =
            pSegmentFactory->newScratchSegment(pCache, 10);
        sortParams.storeFinalRun = false;
        
        ExecStreamEmbryo sortStreamEmbryo;
        sortStreamEmbryo.init(
            ExternalSortExecStream::newExternalSortExecStream(), sortParams);
        sortStreamEmbryo.getStream()->setName("ExternalSortExecStream");
        std::ostringstream oss2;
        oss2 << "ExternalSortExecStream" << "#" << i;
        sortStreamEmbryo.getStream()->setName(oss2.str());
        createBitmapStream.push_back(sortStreamEmbryo);

        // 8. setup splicer

        LbmSplicerExecStreamParams splicerParams;
        initBTreeBitmapDesc(
            splicerParams.tupleDesc, splicerParams.keyProj, nKeys);
        initBTreeExecStreamParam(splicerParams, pBTreeDesc);
        splicerParams.dynParamId = DynamicParamId(i + 1);
        splicerParams.outputTupleDesc.push_back(attrDesc_int64);
        splicerParams.rootPageId = pBTreeDesc->rootPageId;

        ExecStreamEmbryo splicerStreamEmbryo;
        splicerStreamEmbryo.init(new LbmSplicerExecStream(), splicerParams);
        std::ostringstream oss3;
        oss3 << "LbmSplicerExecStream" << "#" << i;
        splicerStreamEmbryo.getStream()->setName(oss3.str());
        createBitmapStream.push_back(splicerStreamEmbryo);

        // connect the sorter and splicer to generator and then add this
        // newly connected stream to the list of create bitmap stream embryos
        createBitmapStreamList.push_back(createBitmapStream);
    }

    // 9. setup barrier stream for create bitmaps

    barrierParams.outputTupleDesc.clear();
    barrierParams.outputTupleDesc.push_back(attrDesc_int64);

    ExecStreamEmbryo barrierStreamEmbryo;
    barrierStreamEmbryo.init(
        new BarrierExecStream(), barrierParams);
    barrierStreamEmbryo.getStream()->setName("BitmapBarrierExecStream");

    // create the bitmap stream graph, with the load stream graph from
    // above as the source
    SharedExecStream pOutputStream = prepareDAG(
        clusterBarrierStreamEmbryo, splitterStreamEmbryo,
        createBitmapStreamList, barrierStreamEmbryo, true, false);

    // set up a generator which can produce the expected output
    RampExecStreamGenerator expectedResultGenerator(mockParams.nRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void::LbmIndexScanTest::initBTreeExecStreamParam(
    BTreeExecStreamParams &param, shared_ptr<BTreeDescriptor> pBTreeDesc)
{
    param.scratchAccessor = pSegmentFactory->newScratchSegment(pCache, 10);
    param.pCacheAccessor = pCache;
    param.pSegment = pRandomSegment;
    param.pRootMap = 0;

    pBTreeDesc->segmentAccessor.pSegment = param.pSegment;
    pBTreeDesc->segmentAccessor.pCacheAccessor = pCache;
    pBTreeDesc->tupleDescriptor = param.tupleDesc;
    pBTreeDesc->keyProjection = param.keyProj;
    param.pageOwnerId = pBTreeDesc->pageOwnerId;
    param.segmentId = pBTreeDesc->segmentId;
}

void LbmIndexScanTest::initClusterScanDef(
    LcsRowScanBaseExecStreamParams &rowScanParams,
    struct LcsClusterScanDef &clusterScanDef,
    uint bTreeIndex)
{
    clusterScanDef.pSegment =
        bTreeClusters[bTreeIndex]->segmentAccessor.pSegment;
    clusterScanDef.pCacheAccessor = 
        bTreeClusters[bTreeIndex]->segmentAccessor.pCacheAccessor;
    clusterScanDef.tupleDesc = bTreeClusters[bTreeIndex]->tupleDescriptor;
    clusterScanDef.keyProj = bTreeClusters[bTreeIndex]->keyProjection;
    clusterScanDef.rootPageId = bTreeClusters[bTreeIndex]->rootPageId;
    clusterScanDef.pageOwnerId = bTreeClusters[bTreeIndex]->pageOwnerId;
    clusterScanDef.segmentId = bTreeClusters[bTreeIndex]->segmentId;
    rowScanParams.lcsClusterScanDefs.push_back(clusterScanDef);
}

void LbmIndexScanTest::initBTreeBitmapDesc(
    TupleDescriptor &tupleDesc, TupleProjection &keyProj, uint nKeys)
{
    initBTreeTupleDesc(tupleDesc, nKeys);

    // btree key consists of the key columns and the start rid
    for (uint j = 0; j < nKeys + 1; j++) {
        keyProj.push_back(j);
    }
}

void LbmIndexScanTest::initBTreeTupleDesc(
    TupleDescriptor &tupleDesc, uint nKeys)
{
    for (uint i = 0; i < nKeys; i++) {
        tupleDesc.push_back(attrDesc_int64);
    }
    // add on the rid
    tupleDesc.push_back(attrDesc_int64);

    uint varColSize;

    // The default page size is 4K.
    varColSize = pRandomSegment->getUsablePageSize()/8;
    // varColSize = 256;

    tupleDesc.push_back(
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY), true,
            varColSize));
    tupleDesc.push_back(
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY), true,
            varColSize));
}

void LbmIndexScanTest::testCaseSetUp()
{    
    ExecStreamUnitTestBase::testCaseSetUp();

    attrDesc_char1 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_CHAR), false, 1);
    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
}

void LbmIndexScanTest::testCaseTearDown()
{
    for (uint i = 0; i < bTreeClusters.size(); i++) {
        bTreeClusters[i]->segmentAccessor.reset();
    }
    for (uint i = 0; i < bTreeBitmaps.size(); i++) {
        bTreeBitmaps[i]->segmentAccessor.reset();
    }
    bTreeClusters.clear();
    bTreeBitmaps.clear();
    savedBTreeClusterRootIds.clear();
    savedBTreeBitmapRootIds.clear();

    ExecStreamUnitTestBase::testCaseTearDown();
}

void LbmIndexScanTest::testScanIdx(
    uint idxToScan, uint nClusters, uint bufSize, PBuffer inputBuffer,
    TupleProjection const &proj, uint expectedNRows,
    boost::scoped_array<uint64_t> const &expectedValues)
{
    resetExecStreamTest();

    uint nKeys;
    if (idxToScan < nClusters) {
        nKeys = 1;
    } else {
        nKeys = nClusters;
    }

    // setup input into index scan; mock stream will read tuples from
    // inputBuffer

    ValuesExecStreamParams valuesParams;
    for (uint i = 0; i < 2; i++) {
        valuesParams.outputTupleDesc.push_back(attrDesc_char1);
        for (uint j = 0; j < nKeys; j++) {
            valuesParams.outputTupleDesc.push_back(attrDesc_int64);
        }
    }
    valuesParams.pTupleBuffer = inputBuffer;
    valuesParams.bufSize = bufSize;

    ExecStreamEmbryo valuesStreamEmbryo;
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    // setup index scan stream

    LbmIndexScanExecStreamParams indexScanParams;

    // initialize parameters specific to indexScan
    indexScanParams.rowLimitParamId = DynamicParamId(0);
    indexScanParams.ignoreRowLimit = true;
    indexScanParams.startRidParamId = DynamicParamId(0);

    // initialize parameters for btree read
    initBTreeBitmapDesc(
        indexScanParams.tupleDesc, indexScanParams.keyProj, nKeys);
    initBTreeExecStreamParam(indexScanParams, bTreeBitmaps[idxToScan]);
    indexScanParams.rootPageId = bTreeBitmaps[idxToScan]->rootPageId =
        savedBTreeBitmapRootIds[idxToScan];
    TupleProjection outputProj;
    for (uint i = nKeys; i < nKeys + 3; i++) {
        outputProj.push_back(i);
    }
    indexScanParams.outputProj = outputProj;

    // initialize parameters for btree search
    indexScanParams.outerJoin = false;
    TupleProjection inputKeyProj;
    for (uint i = 0; i < 2; i++) {
        for (uint j = 0; j < nKeys; j++) {
            inputKeyProj.push_back(i * (nKeys + 1) + j + 1);
        }
    }
    indexScanParams.inputKeyProj = inputKeyProj;
    indexScanParams.inputDirectiveProj.push_back(0);
    indexScanParams.inputDirectiveProj.push_back(nKeys + 1);

    // output is bitmap btree tuple without the key values, but with the rid
    initBTreeTupleDesc(indexScanParams.outputTupleDesc, 0);

    ExecStreamEmbryo indexScanStreamEmbryo;
    indexScanStreamEmbryo.init(new LbmIndexScanExecStream(), indexScanParams);
    indexScanStreamEmbryo.getStream()->setName("IndexScanStream");

    std::vector<ExecStreamEmbryo> scanStreams;
    scanStreams.push_back(indexScanStreamEmbryo);

    // setup parameters into scan
    //  nClusters cluster with 1 column each
    
    LcsRowScanExecStreamParams rowScanParams;
    struct LcsClusterScanDef clusterScanDef;

    clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);
    if (idxToScan < nClusters) {
        initClusterScanDef(rowScanParams, clusterScanDef, idxToScan);
    } else {
        for (uint i = 0; i < nClusters; i++) {
            initClusterScanDef(rowScanParams, clusterScanDef, i);
        }
    }

    // setup scan projection
    rowScanParams.outputProj = proj;
    for (uint i = 0; i < proj.size(); i++) {
        rowScanParams.outputTupleDesc.push_back(attrDesc_int64);
    }

    ExecStreamEmbryo rowScanStreamEmbryo;
    rowScanStreamEmbryo.init(new LcsRowScanExecStream(), rowScanParams);
    rowScanStreamEmbryo.getStream()->setName("RowScanExecStream");
    scanStreams.push_back(rowScanStreamEmbryo);

    SharedExecStream pOutputStream = prepareTransformGraph(
        valuesStreamEmbryo, scanStreams);
    
    // setup generators for result stream

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    if (idxToScan < nClusters) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(
                new ConstColumnGenerator(expectedValues[0]));
        columnGenerators.push_back(col);
    } else {
        for (uint i = 0; i < nClusters; i++) {
            SharedInt64ColumnGenerator col =
                SharedInt64ColumnGenerator(
                    new ConstColumnGenerator(expectedValues[i]));
            columnGenerators.push_back(col);
        }
    }

    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(*pOutputStream, expectedNRows, resultGenerator);
}

FENNEL_UNIT_TEST_SUITE(LbmIndexScanTest);

// End LbmIndexScanTest.cpp

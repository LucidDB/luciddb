/*
// $Id$
// Fennel is a library of data storage and processing components.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/ExecStreamUnitTestBase.h"
#include "fennel/lucidera/colstore/LcsClusterAppendExecStream.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
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
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;

    vector<boost::shared_ptr<BTreeDescriptor> > bTreeClusters;
    
    /**
     * Serially loads nClusters, each cluster containing nCols and nRows
     */
    void loadClusters(uint nRows, uint nCols, uint nClusters);

    /**
     * Loads a single cluster with nCols columns and nRows rows.
     * Each column will have a different sequence of values, as follows:
     *      column0 - colStart, colStart+1, ..., colStart+nRows-1
     *      column1 - colStart+1, colStart+2, ..., colStart+nRows
     *      column2 - colStart+2, colStart+3, ..., colStart+nRows+1
     *      ...
     */
    void loadOneCluster(uint nRows, uint nCols, int colStart,
                        BTreeDescriptor &bTreeDescriptor);

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
    void testScanCols(uint nRows, uint nCols, uint nClusters,
                      TupleProjection proj, uint skipRows,
                      uint expectedNumRows);

public:
    explicit LcsRowScanExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testScans);
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testScanOnEmptyCluster);
        FENNEL_UNIT_TEST_CASE(LcsRowScanExecStreamTest, testScanPastEndOfCluster);
    }

    void testCaseSetUp();
    void testCaseTearDown();
    
    void testScans();
    void testScanOnEmptyCluster();
    void testScanPastEndOfCluster();
};

void LcsRowScanExecStreamTest::loadClusters(uint nRows, uint nCols,
                                            uint nClusters)
{
    for (uint i = 0; i < nClusters; i++) {
        boost::shared_ptr<BTreeDescriptor> pBTreeDesc =
            boost::shared_ptr<BTreeDescriptor> (new BTreeDescriptor());
        bTreeClusters.push_back(pBTreeDesc);
        loadOneCluster(nRows, nCols, i * nCols, *(bTreeClusters[i]));
        resetExecStreamTest();
    }
}

void LcsRowScanExecStreamTest::loadOneCluster(
    uint nRows,
    uint nCols,
    int colStart,
    BTreeDescriptor &bTreeDescriptor)
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
            SharedInt64ColumnGenerator(new SeqColumnGenerator(i + colStart));
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

    lcsAppendParams.overwrite = false;
    for (uint i = 0; i < nCols; i++) {
        lcsAppendParams.inputProj.push_back(i);
    }
    lcsAppendParams.pRootMap = 0;
    
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

void LcsRowScanExecStreamTest::testScanCols(uint nRows, uint nCols,
                                            uint nClusters,
                                            TupleProjection proj,
                                            uint skipRows,
                                            uint expectedNumRows)
{
    // setup input rid stream

    MockProducerExecStreamParams mockParams;
    for (uint i = 0; i < nCols; i++)
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    mockParams.nRows = nRows/skipRows;
    mockParams.pGenerator.reset(new RampExecStreamGenerator(0, skipRows));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerScanExecStream");

    // setup parameters into scan
    //  nClusters cluster with nCols columns each
    
    LcsRowScanExecStreamParams scanParams;
    for (uint i = 0; i < nClusters; i++) {
        struct LcsClusterScanDef clusterScanDef;

        for (uint j = 0; j < nCols; j++)
            clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);

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

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, scanStreamEmbryo);
    
    // setup generators for result stream

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    for (uint i = 0; i < proj.size(); i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(new SeqColumnGenerator(proj[i],
                                                              skipRows));
        columnGenerators.push_back(col);
    }

    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(*pOutputStream, expectedNumRows, resultGenerator);
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

    loadClusters(nRows, nCols, nClusters);
    // note: no need to reset after loadClusters() because already done
    // there

    // scan all rows and columns
    for (uint i = 0; i < nClusters; i++)
        for (uint j = 0; j < nCols; j++)
            proj.push_back(i * nCols + j);
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
    testScanCols(nRows, nCols, nClusters, proj, 7, nRows/7);
    resetExecStreamTest();

    // full table scan -- input stream is empty
    testScanCols(0, nCols, nClusters, proj, 1, nRows);
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
    loadOneCluster(1, 1, 0, *(bTreeClusters[0]));
    resetExecStreamTest();

    // have testScanCols attempt to read 2 rows, although it should only
    // be able to read 1

    TupleProjection proj;

    proj.push_back(0);
    testScanCols(2, 1, 1, proj, 1, 1);
}

void LcsRowScanExecStreamTest::testCaseSetUp()
{    
    ExecStreamUnitTestBase::testCaseSetUp();
    
    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
}

void LcsRowScanExecStreamTest::testCaseTearDown()
{
    for (uint i = 0; i < bTreeClusters.size(); i++)
        bTreeClusters[i]->segmentAccessor.reset();
    ExecStreamUnitTestBase::testCaseTearDown();
}

FENNEL_UNIT_TEST_SUITE(LcsRowScanExecStreamTest);


// End LcsRowScanExecStreamTest.cpp

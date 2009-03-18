/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
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
#include "fennel/common/FemEnums.h"
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
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/SplitterExecStream.h"
#include "fennel/exec/BarrierExecStream.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/cache/Cache.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for loading multiple clusters.
 */
class LcsMultiClusterAppendTest : public ExecStreamUnitTestBase
{
protected:
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;
    TupleAttributeDescriptor attrDesc_bitmap;

    vector<boost::shared_ptr<BTreeDescriptor> > bTreeClusters;

    /**
     * Loads nClusters clusters, each cluster containing nCols columns and
     * nRows rows.
     *
     * Column values are generated using a duplicate column generator,
     * where the number duplicate values in a column is equal to the column
     * number, assuming 1-based column numbers.  I.e.,
     *  column1 - 0, 1, 2, ...
     *  column2 - 0, 0, 1, 1, 2, 2, ...
     *  column3 - 0, 0, 0, 1, 1, 1, 2, 2, 2, ...
     */
    void loadClusters(uint nRows, uint nCols, uint nClusters);

    /**
     * Performs full table scan on a table.  Table has nRows rows and
     * nClusters clusters, each with nCols.  Therefore, the table has
     * in total nCols*nClusters columns.
     */
    void scanCols(uint nRows, uint nCols, uint nClusters,
                      TupleProjection proj);

public:
    explicit LcsMultiClusterAppendTest()
    {
        FENNEL_UNIT_TEST_CASE(LcsMultiClusterAppendTest, testLoad);
    }

    void testCaseSetUp();
    void testCaseTearDown();

    void testLoad();
};

void LcsMultiClusterAppendTest::testLoad()
{
    // load clusters and then do a full table scan to verify the rows
    // were properly loaded

    uint nRows = 100000;
    uint nCols = 5;
    uint nClusters = 7;
    TupleProjection proj;

    loadClusters(nRows, nCols, nClusters);
    resetExecStreamTest();

    // project all columns
    for (uint i = 0; i < nClusters * nCols; i++) {
        proj.push_back(i);
    }
    scanCols(nRows, nCols, nClusters, proj);
}

void LcsMultiClusterAppendTest::loadClusters(uint nRows, uint nCols,
                                             uint nClusters)
{
    // setup input stream

    MockProducerExecStreamParams mockParams;
    for (uint i = 0; i < nCols * nClusters; i++) {
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    mockParams.nRows = nRows;

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    for (uint i = 0; i < nCols * nClusters; i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(new DupColumnGenerator(i + 1));
        columnGenerators.push_back(col);
    }
    mockParams.pGenerator.reset(
        new CompositeExecStreamGenerator(columnGenerators));

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    // setup splitter stream

    SplitterExecStreamParams splitterParams;
    ExecStreamEmbryo splitterStreamEmbryo;

    splitterStreamEmbryo.init(new SplitterExecStream(), splitterParams);
    splitterStreamEmbryo.getStream()->setName("SplitterExecStream");

    // setup loader streams

    vector<ExecStreamEmbryo> lcsAppendEmbryos;
    for (uint i = 0; i < nClusters; i++) {
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

        for (uint j = 0; j < nCols; j++) {
            lcsAppendParams.inputProj.push_back(i * nCols + j);
        }
        lcsAppendParams.pRootMap = 0;
        lcsAppendParams.rootPageIdParamId = DynamicParamId(0);

        // setup temporary btree descriptor to get an empty page to start
        // the btree

        boost::shared_ptr<BTreeDescriptor> pBTreeDesc =
            boost::shared_ptr<BTreeDescriptor> (new BTreeDescriptor());
        bTreeClusters.push_back(pBTreeDesc);
        pBTreeDesc->segmentAccessor.pSegment = lcsAppendParams.pSegment;
        pBTreeDesc->segmentAccessor.pCacheAccessor = pCache;
        pBTreeDesc->tupleDescriptor = lcsAppendParams.tupleDesc;
        pBTreeDesc->keyProjection = lcsAppendParams.keyProj;
        pBTreeDesc->rootPageId = NULL_PAGE_ID;
        lcsAppendParams.pageOwnerId = pBTreeDesc->pageOwnerId;
        lcsAppendParams.segmentId = pBTreeDesc->segmentId;

        BTreeBuilder builder(*pBTreeDesc, pRandomSegment);
        builder.createEmptyRoot();
        lcsAppendParams.rootPageId = pBTreeDesc->rootPageId =
            builder.getRootPageId();

        // Now use the above initialized parameter

        LcsClusterAppendExecStream *lcsStream =
            new LcsClusterAppendExecStream();

        ExecStreamEmbryo lcsAppendStreamEmbryo;
        lcsAppendStreamEmbryo.init(lcsStream, lcsAppendParams);
        std::ostringstream oss;
        oss << "LcsClusterAppendExecStream" << "#" << i;
        lcsAppendStreamEmbryo.getStream()-> setName(oss.str());
        lcsAppendEmbryos.push_back(lcsAppendStreamEmbryo);
    }

    // setup barrier stream

    BarrierExecStreamParams barrierParams;
    barrierParams.outputTupleDesc.push_back(attrDesc_int64);
    barrierParams.returnMode = BARRIER_RET_ANY_INPUT;

    ExecStreamEmbryo barrierStreamEmbryo;
    barrierStreamEmbryo.init(new BarrierExecStream(), barrierParams);
    barrierStreamEmbryo.getStream()->setName("BarrierExecStream");

    // connect all the embryos together
    SharedExecStream pOutputStream = prepareDAG(
        mockStreamEmbryo, splitterStreamEmbryo, lcsAppendEmbryos,
        barrierStreamEmbryo);

    // set up a generator which can produce the expected output
    RampExecStreamGenerator expectedResultGenerator(mockParams.nRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);
}

void LcsMultiClusterAppendTest::scanCols(uint nRows, uint nCols,
                                             uint nClusters,
                                             TupleProjection proj)
{
    // setup parameters into scan
    //  nClusters cluster with nCols columns each

    LcsRowScanExecStreamParams scanParams;
    scanParams.hasExtraFilter = false;
    scanParams.isFullScan = true;
    scanParams.samplingMode = SAMPLING_OFF;
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
        clusterScanDef.pageOwnerId = bTreeClusters[i]->pageOwnerId;
        clusterScanDef.segmentId = bTreeClusters[i]->segmentId;

        scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    }

    // setup projection

    scanParams.outputProj = proj;
    for (uint i = 0; i < proj.size(); i++) {
        scanParams.outputTupleDesc.push_back(attrDesc_int64);
    }

    // setup a values stream to provide an empty input to simulate
    // the scan of the deletion index

    ValuesExecStreamParams valuesParams;
    ExecStreamEmbryo valuesStreamEmbryo;
    boost::shared_array<FixedBuffer> pBuffer;

    valuesParams.outputTupleDesc.push_back(attrDesc_int64);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);
    valuesParams.outputTupleDesc.push_back(attrDesc_bitmap);

    uint bufferSize = 16;
    pBuffer.reset(new FixedBuffer[bufferSize]);
    valuesParams.pTupleBuffer = pBuffer;
    valuesParams.bufSize = 0;
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    ExecStreamEmbryo scanStreamEmbryo;

    scanStreamEmbryo.init(new LcsRowScanExecStream(), scanParams);
    scanStreamEmbryo.getStream()->setName("RowScanExecStream");

    SharedExecStream pOutputStream =
        prepareTransformGraph(valuesStreamEmbryo, scanStreamEmbryo);

    // setup generators for result stream

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > columnGenerators;
    for (uint i = 0; i < proj.size(); i++) {
        SharedInt64ColumnGenerator col =
            SharedInt64ColumnGenerator(new DupColumnGenerator(proj[i] + 1));
        columnGenerators.push_back(col);
    }

    CompositeExecStreamGenerator resultGenerator(columnGenerators);
    verifyOutput(*pOutputStream, nRows, resultGenerator);
}

void LcsMultiClusterAppendTest::testCaseSetUp()
{
    ExecStreamUnitTestBase::testCaseSetUp();

    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));
    attrDesc_bitmap = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_VARBINARY),
        true, pRandomSegment->getUsablePageSize() / 8);
}

void LcsMultiClusterAppendTest::testCaseTearDown()
{
    for (uint i = 0; i < bTreeClusters.size(); i++) {
        bTreeClusters[i]->segmentAccessor.reset();
    }
    ExecStreamUnitTestBase::testCaseTearDown();
}

FENNEL_UNIT_TEST_SUITE(LcsMultiClusterAppendTest);


// End LcsMultiClusterAppendTest.cpp

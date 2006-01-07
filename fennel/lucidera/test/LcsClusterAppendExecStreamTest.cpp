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
#include "fennel/lucidera/colstore/LcsClusterDump.h"
#include "fennel/lucidera/colstore/LcsClusterVerifier.h"
#include "fennel/lucidera/colstore/LcsRowScanExecStream.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/ftrs/BTreeInsertExecStream.h"
#include "fennel/ftrs/BTreeSearchExecStream.h"
#include "fennel/ftrs/BTreeExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/cache/Cache.h"
#include "fennel/common/TraceSource.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

class LcsClusterAppendExecStreamTest : public ExecStreamUnitTestBase
{
protected:
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc_int64;

    PageId  savedRootPageId;
    BTreeDescriptor btreeDescriptor;
    
    void testLoadSingleCol(
        uint nRows,
        bool newRoot,
        SharedMockProducerExecStreamGenerator pGeneratorInit, 
        std::string testName = "LcsClusterAppendExecStreamTest");
    
    void testLoadMultiCol(
        uint nRows,
        uint nCols,
        bool newRoot,
        SharedMockProducerExecStreamGenerator pGeneratorInit,
        std::string testName = "LcsClusterAppendExecStreamTest");
    
    void verifyClusterPages(std::string testName);
    
    void testScanSingleCol(
        uint nrows,
        SharedMockProducerExecStreamGenerator pGeneratorInit,
        SharedMockProducerExecStreamGenerator pResultGenerator);

    void testScanMultiCol(
        uint nrows,
        uint nCols,
        SharedMockProducerExecStreamGenerator pGeneratorInit,
        SharedMockProducerExecStreamGenerator pResultGenerator);

public:
    explicit LcsClusterAppendExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testSingleColNoDupNewRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testSingleColNoDupOldRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testSingleColConstNewRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testSingleColConstOldRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testSingleColStairNewRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testSingleColStairOldRoot);

        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testMultiColNoDupNewRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testMultiColNoDupOldRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testMultiColConstNewRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testMultiColConstOldRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testMultiColStairNewRoot);
        FENNEL_UNIT_TEST_CASE(LcsClusterAppendExecStreamTest,
            testMultiColStairOldRoot);
    }
    void testCaseSetUp();
    void testCaseTearDown();
    
    void testSingleColNoDupNewRoot();
    void testSingleColNoDupOldRoot();

    void testSingleColConstNewRoot();
    void testSingleColConstOldRoot();

    void testSingleColStairNewRoot();
    void testSingleColStairOldRoot();
    
    void testMultiColNoDupNewRoot();
    void testMultiColNoDupOldRoot();

    void testMultiColConstNewRoot();
    void testMultiColConstOldRoot();

    void testMultiColStairNewRoot();
    void testMultiColStairOldRoot();
};

void LcsClusterAppendExecStreamTest::verifyClusterPages(std::string testName)
{
    bool found;
    PConstLcsClusterNode pBlock;
    PageId clusterPageId;
    LcsRid rid;
    ClusterPageData pageData;
    uint blockSize =
        btreeDescriptor.segmentAccessor.pSegment->getUsablePageSize();
    LcsClusterVerifier clusterVerifier(btreeDescriptor);
    LcsClusterDump clusterDump(btreeDescriptor, TRACE_INFO, shared_from_this(),
                               testName);

    // read every cluster page

    found = clusterVerifier.getFirstClusterPageForRead(pBlock);
    if (!found) {
        BOOST_FAIL("getFirstClusterPageForRead found nothing");
    }
    do {
        pageData = clusterVerifier.getPageData();
        // make sure the rid on the btree matches the rid on the cluster
        // page
        BOOST_CHECK_EQUAL(pageData.bTreeRid, pBlock->firstRID);
        clusterDump.dump(opaqueToInt(pageData.clusterPageId), pBlock,
                         blockSize);
    } while (found = clusterVerifier.getNextClusterPageForRead(pBlock));
}

/*
   Tests inserting a single column of non duplicate rows into a cluster.
   If newRoot is false, a prior call has already created a btree.  However,
   in order for it to be preserved, the subsequent call with newRoot=false must
   be done within the same testcase call.
 */
void LcsClusterAppendExecStreamTest::testLoadSingleCol(
    uint nRows,
    bool newRoot,
    SharedMockProducerExecStreamGenerator pGeneratorInit,
    std::string testName)
{    
    SharedMockProducerExecStreamGenerator pGenerator = pGeneratorInit;

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc_int64);
    mockParams.nRows = nRows;
    mockParams.pGenerator = pGenerator;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerExecStream");

    LcsClusterAppendExecStreamParams lcsAppendParams;
    lcsAppendParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    lcsAppendParams.pCacheAccessor = pCache;
    lcsAppendParams.pSegment = pRandomSegment;
    
    lcsAppendParams.overwrite = false;
    lcsAppendParams.inputProj.push_back(0);

    // initialize the btree parameter portion of lcsAppendParams
    // BTree tuple desc only has one column
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);

    // BTree key only has one column which is the first column.
    (lcsAppendParams.keyProj).push_back(0);

    // output only one value(rows inserted)
    lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);

    lcsAppendParams.pRootMap = 0;

    // Set up BTreeExecStreamParams using default values from BTreeDescriptor.
    lcsAppendParams.segmentId = btreeDescriptor.segmentId;
    lcsAppendParams.pageOwnerId = btreeDescriptor.pageOwnerId;
    
    // setup temporary btree descriptor to get an empty page to start the btree
    btreeDescriptor.segmentAccessor.pSegment = lcsAppendParams.pSegment;
    btreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    btreeDescriptor.tupleDescriptor = lcsAppendParams.tupleDesc;
    btreeDescriptor.keyProjection = lcsAppendParams.keyProj;
    btreeDescriptor.rootPageId = newRoot ? NULL_PAGE_ID : savedRootPageId;

    BTreeBuilder builder(btreeDescriptor, pRandomSegment);

    // if BTree root not yet setup
    if (newRoot) {
        builder.createEmptyRoot();
        savedRootPageId = builder.getRootPageId();
    }

    lcsAppendParams.rootPageId = btreeDescriptor.rootPageId = savedRootPageId;

    /*
      Now use the above initialized parameter 
     */
    LcsClusterAppendExecStream *lcsStream = new LcsClusterAppendExecStream();

    ExecStreamEmbryo lcsAppendStreamEmbryo;
    lcsAppendStreamEmbryo.init(lcsStream, lcsAppendParams);
    lcsAppendStreamEmbryo.getStream()->setName("LcsClusterAppendExecStream");
    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, lcsAppendStreamEmbryo);

    // set up a generator which can produce the expected output
    RampExecStreamGenerator expectedResultGenerator(mockParams.nRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);

    // read records from btree to obtain cluster page ids
    // and dump out contents of cluster pages
    verifyClusterPages(testName);
}

void LcsClusterAppendExecStreamTest::testLoadMultiCol(
    uint nRows,
    uint nCols,
    bool newRoot,
    SharedMockProducerExecStreamGenerator pGeneratorInit,
    std::string testName)
{    
    SharedMockProducerExecStreamGenerator pGenerator = pGeneratorInit;

    MockProducerExecStreamParams mockParams;
    for (uint i = 0; i < nCols; i ++) {
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    }
    mockParams.nRows = nRows;
    mockParams.pGenerator = pGenerator;

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

    btreeDescriptor.segmentAccessor.pSegment = lcsAppendParams.pSegment;
    btreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    btreeDescriptor.tupleDescriptor = lcsAppendParams.tupleDesc;
    btreeDescriptor.keyProjection = lcsAppendParams.keyProj;
    btreeDescriptor.rootPageId = newRoot ? NULL_PAGE_ID : savedRootPageId;
    btreeDescriptor.segmentId = lcsAppendParams.segmentId;
    btreeDescriptor.pageOwnerId = lcsAppendParams.pageOwnerId;
    
    BTreeBuilder builder(btreeDescriptor, pRandomSegment);

    // if BTree root not yet setup
    if (newRoot) {
        builder.createEmptyRoot();
        savedRootPageId = builder.getRootPageId();
    }

    lcsAppendParams.rootPageId = btreeDescriptor.rootPageId = savedRootPageId;

    /*
      Now use the above initialized parameter 
     */
    ExecStreamEmbryo lcsAppendStreamEmbryo;
    lcsAppendStreamEmbryo.init(new LcsClusterAppendExecStream(),
                               lcsAppendParams);
    lcsAppendStreamEmbryo.getStream()->setName("LcsClusterAppendExecStream");
    
    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, lcsAppendStreamEmbryo);

    // set up a generator which can produce the expected output
    RampExecStreamGenerator expectedResultGenerator(mockParams.nRows);

    verifyOutput(*pOutputStream, 1, expectedResultGenerator);

    // read records from btree to obtain cluster page ids
    // and dump out contents of cluster pages
    verifyClusterPages(testName);
}

void LcsClusterAppendExecStreamTest::testScanSingleCol(
    uint nrows,
    SharedMockProducerExecStreamGenerator pGeneratorInit,
    SharedMockProducerExecStreamGenerator pResultGenerator)
{
    SharedMockProducerExecStreamGenerator pGenerator = pGeneratorInit;

    // setup input stream

    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc.push_back(attrDesc_int64);
    mockParams.nRows = nrows;
    mockParams.pGenerator = pGenerator;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerScanExecStream");

    // setup parameters into scan
    //  single cluster with only one column, project that single column
    
    LcsRowScanExecStreamParams scanParams;
    struct LcsClusterScanDef clusterScanDef;

    clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);
    clusterScanDef.pSegment = btreeDescriptor.segmentAccessor.pSegment;
    clusterScanDef.pCacheAccessor =
        btreeDescriptor.segmentAccessor.pCacheAccessor;
    clusterScanDef.tupleDesc = btreeDescriptor.tupleDescriptor;
    clusterScanDef.keyProj = btreeDescriptor.keyProjection;
    clusterScanDef.rootPageId = btreeDescriptor.rootPageId;
    clusterScanDef.segmentId = btreeDescriptor.segmentId;
    clusterScanDef.pageOwnerId = btreeDescriptor.pageOwnerId;

    scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    scanParams.outputTupleDesc.push_back(attrDesc_int64);
    scanParams.outputProj.push_back(0);

    ExecStreamEmbryo scanStreamEmbryo;

    scanStreamEmbryo.init(new LcsRowScanExecStream(), scanParams);
    scanStreamEmbryo.getStream()->setName("RowScanExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, scanStreamEmbryo);
    
    // result should be sequence of rows
    verifyOutput(*pOutputStream, nrows, *pResultGenerator);
}

void LcsClusterAppendExecStreamTest::testScanMultiCol(
    uint nrows,
    uint nCols,
    SharedMockProducerExecStreamGenerator pGeneratorInit,
    SharedMockProducerExecStreamGenerator pResultGenerator)
{
    uint i;
    SharedMockProducerExecStreamGenerator pGenerator = pGeneratorInit;

    // setup input stream

    MockProducerExecStreamParams mockParams;
    for (i = 0; i < nCols; i++)
        mockParams.outputTupleDesc.push_back(attrDesc_int64);
    mockParams.nRows = nrows;
    mockParams.pGenerator = pGenerator;

    ExecStreamEmbryo mockStreamEmbryo;
    mockStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    mockStreamEmbryo.getStream()->setName("MockProducerScanExecStream");

    // setup parameters into scan
    //  single cluster with only n columns, project all columns
    
    LcsRowScanExecStreamParams scanParams;
    struct LcsClusterScanDef clusterScanDef;

    for (i = 0; i < nCols; i++)
        clusterScanDef.clusterTupleDesc.push_back(attrDesc_int64);

    clusterScanDef.pSegment = btreeDescriptor.segmentAccessor.pSegment;
    clusterScanDef.pCacheAccessor =
        btreeDescriptor.segmentAccessor.pCacheAccessor;
    clusterScanDef.tupleDesc = btreeDescriptor.tupleDescriptor;
    clusterScanDef.keyProj = btreeDescriptor.keyProjection;
    clusterScanDef.rootPageId = btreeDescriptor.rootPageId;
    clusterScanDef.segmentId = btreeDescriptor.segmentId;
    clusterScanDef.pageOwnerId = btreeDescriptor.pageOwnerId;

    scanParams.lcsClusterScanDefs.push_back(clusterScanDef);
    for (i = 0; i < nCols; i++) {
        scanParams.outputTupleDesc.push_back(attrDesc_int64);
        scanParams.outputProj.push_back(i);
    }

    ExecStreamEmbryo scanStreamEmbryo;

    scanStreamEmbryo.init(new LcsRowScanExecStream(), scanParams);
    scanStreamEmbryo.getStream()->setName("RowScanExecStream");

    SharedExecStream pOutputStream = prepareTransformGraph(
        mockStreamEmbryo, scanStreamEmbryo);
    
    // result should be sequence of rows
    verifyOutput(*pOutputStream, nrows, *pResultGenerator);
}

void LcsClusterAppendExecStreamTest::testCaseSetUp()
{    
    ExecStreamUnitTestBase::testCaseSetUp();
    
    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    savedRootPageId = NULL_PAGE_ID;
}

void LcsClusterAppendExecStreamTest::testCaseTearDown()
{
    btreeDescriptor.segmentAccessor.reset();
    ExecStreamUnitTestBase::testCaseTearDown();
}

void LcsClusterAppendExecStreamTest::testSingleColNoDupNewRoot()
{
    // 1. load 848 rows
    // 2. scan 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    
    testLoadSingleCol(848, true,  pGenerator, "testSingleColNoDupNewRoot");
    resetExecStreamTest();
    testScanSingleCol(848, pGenerator, pResultGenerator);
}

/*
  Tests appending to an existing tree by first inserting into a new btree and
  then reusing the btree.  Note that all of this needs to be done within a
  single testcase with an intervening resetExecStreamTest().  Otherwise, the
  btree state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testSingleColNoDupOldRoot()
{
    // 1. load 848 rows
    // 2. scan first 848 rows
    // 3. load 848 more rows
    // 4. scan second 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());

    testLoadSingleCol(848, true,  pGenerator,  "testSingleColNoDupOldRoot");
    resetExecStreamTest();
    // this will test scans of variable mode batches
    testScanSingleCol(848, pGenerator, pResultGenerator);

    resetExecStreamTest();
    testLoadSingleCol(848, false,  pGenerator,  "testSingleColNoDupOldRoot");

    resetExecStreamTest();
    pGenerator.reset(new RampExecStreamGenerator(848));
    testScanSingleCol(848, pGenerator, pResultGenerator);
}


void LcsClusterAppendExecStreamTest::testSingleColConstNewRoot()
{
    // 1. load 848 rows
    // 2. scan 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    
    testLoadSingleCol(848, true, pGenerator, "testSingleColConstNewRoot");
    resetExecStreamTest();

    pGenerator.reset(new RampExecStreamGenerator());
    testScanSingleCol(848, pGenerator, pResultGenerator);
}

/*
  Tests appending to an existing tree by first inserting into a new btree and
  then reusing the btree.  Note that all of this needs to be done within a
  single testcase with an intervening resetExecStreamTest().  Otherwise, the
  btree state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testSingleColConstOldRoot()
{
    // 1. load 10 rows
    // 2. load 10 more rows
    // 3. scan 20 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));

    testLoadSingleCol(10, true,  pGenerator,  "testSingleColConstOldRoot");
    resetExecStreamTest();
    testLoadSingleCol(10, false,  pGenerator,  "testSingleColConstOldRoot");

    resetExecStreamTest();
    pGenerator.reset(new RampExecStreamGenerator());
    testScanSingleCol(20, pGenerator, pResultGenerator);
}

void LcsClusterAppendExecStreamTest::testSingleColStairNewRoot()
{
    // 1. load 848 rows
    // 2. scan 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));

    testLoadSingleCol(848, true, pGenerator, "testSingleColStairNewRoot");
    resetExecStreamTest();

    pGenerator.reset(new RampExecStreamGenerator());
    testScanSingleCol(848, pGenerator, pResultGenerator);
}

/*
  Tests appending to an existing tree by first inserting into a new btree and
  then reusing the btree.  Note that all of this needs to be done within a
  single testcase with an intervening resetExecStreamTest().  Otherwise, the
  btree state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testSingleColStairOldRoot()
{
    // 1. load 10 rows
    // 2. scan first 10 rows
    // 3. load 10 more rows
    // 4. scan 2nd 10 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1, 7));
    SharedMockProducerExecStreamGenerator pRidGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));

    testLoadSingleCol(10, true,  pGenerator,  "testSingleColStairOldRoot");
    resetExecStreamTest();
    testScanSingleCol(10, pRidGenerator, pResultGenerator);

    resetExecStreamTest();
    testLoadSingleCol(10, false,  pGenerator, "testSingleColStairOldRoot");

    resetExecStreamTest();
    pRidGenerator.reset(new RampExecStreamGenerator(10));
    testScanSingleCol(10, pRidGenerator, pResultGenerator);
}

void LcsClusterAppendExecStreamTest::testMultiColNoDupNewRoot()
{
    // 1. load 848 rows
    // 2. scan 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    
    testLoadMultiCol(848, 3, true,  pGenerator,  "testMultiColNoDupNewRoot");
    resetExecStreamTest();
    testScanMultiCol(848, 3, pGenerator, pResultGenerator);
}

/*
  Tests appending to an existing tree by first inserting into a new btree and
  then reusing the btree.  Note that all of this needs to be done within a
  single testcase with an intervening resetExecStreamTest().  Otherwise, the
  btree state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testMultiColNoDupOldRoot()
{
    // 1. load 10 rows
    // 2. scan first 10 rows
    // 3. load 10 more rows
    // 4. scan 2nd 10 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pRidGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());

    testLoadMultiCol(10, 3, true,  pGenerator,"testMultiColNoDupOldRoot");
    resetExecStreamTest();
    testScanMultiCol(10, 3, pRidGenerator, pResultGenerator);

    resetExecStreamTest();
    testLoadMultiCol(10, 3, false,  pGenerator, "testMultiColNoDupOldRoot");

    resetExecStreamTest();
    pRidGenerator.reset(new RampExecStreamGenerator(10));
    testScanMultiCol(10, 3, pRidGenerator, pResultGenerator);
}


void LcsClusterAppendExecStreamTest::testMultiColConstNewRoot()
{
    // 1. load 848 rows
    // 2. scan 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    
    testLoadMultiCol(848, 3, true,  pGenerator,  "testMultiColConstNewRoot");
    resetExecStreamTest();

    pGenerator.reset(new RampExecStreamGenerator());
    testScanMultiCol(848, 3, pGenerator, pResultGenerator);
}

/*
  Tests appending to an existing tree by first inserting into a new btree and
  then reusing the btree.  Note that all of this needs to be done within a
  single testcase with an intervening resetExecStreamTest().  Otherwise, the
  btree state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testMultiColConstOldRoot()
{
    // 1. load 10 rows
    // 2. load 10 more rows
    // 3. scan 20 rows
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));

    testLoadMultiCol(10, 3, true,  pGenerator,  "testMultiColConstOldRoot");
    resetExecStreamTest();
    testLoadMultiCol(10, 3, false,  pGenerator, "testMultiColConstOldRoot");

    resetExecStreamTest();
    pGenerator.reset(new RampExecStreamGenerator());
    testScanMultiCol(20, 3, pGenerator, pResultGenerator);
}

void LcsClusterAppendExecStreamTest::testMultiColStairNewRoot()
{
    // 1. load 848 rows
    // 2. scan 848 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));
    
    testLoadMultiCol(848, 3, true,  pGenerator,  "testMultiColStairNewRoot");
    resetExecStreamTest();

    pGenerator.reset(new RampExecStreamGenerator());
    testScanMultiCol(848, 3, pGenerator, pResultGenerator);
}

/*
  Tests appending to an existing tree by first inserting into a new btree and
  then reusing the btree.  Note that all of this needs to be done within a
  single testcase with an intervening resetExecStreamTest().  Otherwise, the
  btree state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testMultiColStairOldRoot()
{
    // 1. load 10 rows
    // 2. scan first 10 rows
    // 3. load more 10 rows
    // 4. scan 2nd 10 rows

    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1, 7));
    SharedMockProducerExecStreamGenerator pRidGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    SharedMockProducerExecStreamGenerator pResultGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));

    testLoadMultiCol(10, 3, true, pGenerator, "testMultiColStairOldRoot");
    resetExecStreamTest();
    testScanMultiCol(10, 3, pRidGenerator, pResultGenerator);

    resetExecStreamTest();
    testLoadMultiCol(10, 3, false, pGenerator, "testMultiColStairOldRoot");

    resetExecStreamTest();
    pRidGenerator.reset(new RampExecStreamGenerator(10));
    testScanMultiCol(10, 3, pRidGenerator, pResultGenerator);
}

FENNEL_UNIT_TEST_SUITE(LcsClusterAppendExecStreamTest);

// End LcsClusterAppendExecStreamTest.cpp

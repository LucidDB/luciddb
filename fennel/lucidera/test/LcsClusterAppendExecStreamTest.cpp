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
    
    void testImplSingleCol(
        uint nRows,
        bool newRoot,
        SharedMockProducerExecStreamGenerator pGeneratorInit, 
        std::string testName = "LcsClusterAppendExecStreamTest");
    
    void testImplMultiCol(
        uint nRows,
        uint nCols,
        bool newRoot,
        SharedMockProducerExecStreamGenerator pGeneratorInit,
        std::string testName = "LcsClusterAppendExecStreamTest");
    
    void verifyClusterPages(
        LcsClusterAppendExecStream *lcsStream,
        BTreeDescriptor &btreeDescriptor,
        std::string testName);
    
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


void LcsClusterAppendExecStreamTest::verifyClusterPages(
        LcsClusterAppendExecStream *lcsStream,
        BTreeDescriptor &btreeDescriptor,
        std::string testName)
{
    BTreeReader reader(btreeDescriptor);
    bool found;
    LcsClusterDump clusterDump(TRACE_INFO, shared_from_this(), testName);

    // read every record on the btree

    found = reader.searchFirst();
    if (!found) {
        BOOST_FAIL("searchFirst found nothing");
    }
    do {
        Rid rid;
        PageId clusterPageId;

        reader.getTupleAccessorForRead().unmarshal(lcsStream->btreeTupleData);
        rid = lcsStream->readRid();
        clusterPageId = lcsStream->readClusterPageId();
        lcsStream->clusterLock.lockShared(clusterPageId);
        LcsClusterNode const &pBlock =
            (lcsStream->clusterLock.getNodeForRead());

        // make sure the rid on the btree matches the rid on the cluster
        // page
        BOOST_CHECK_EQUAL(rid, pBlock.firstRID);
        clusterDump.dump(opaqueToInt(clusterPageId), (PBuffer) &pBlock,
                         lcsStream->m_blockSize);
    } while (reader.searchNext());
}

/*
   Tests inserting a single column of non duplicate rows into a cluster.
   If newRoot is false, a prior call has already created a btree.  However,
   in order for it to be preserved, the subsequent call with newRoot=false must
   be done within the same testcase call.
 */
void LcsClusterAppendExecStreamTest::testImplSingleCol(
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
    lcsAppendParams.pSegment = pLinearSegment;
    
    // initialize the btree parameter portion of lcsAppendParams
    // BTree tuple desc only has one column
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);

    // BTree key only has one column which is the first column.
    (lcsAppendParams.keyProj).push_back(0);

    // output only one value(rows inserted)
    lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);
    lcsAppendParams.overwrite = false;
    lcsAppendParams.pRootMap = 0;
    
    // setup temporary btree descriptor to get an empty page to start the btree
    BTreeDescriptor btreeDescriptor;

    btreeDescriptor.segmentAccessor.pSegment = lcsAppendParams.pSegment;
    btreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    btreeDescriptor.tupleDescriptor = lcsAppendParams.tupleDesc;
    btreeDescriptor.keyProjection = lcsAppendParams.keyProj;
    btreeDescriptor.rootPageId = newRoot ? NULL_PAGE_ID : savedRootPageId;
    btreeDescriptor.segmentId = lcsAppendParams.segmentId;
    btreeDescriptor.pageOwnerId = lcsAppendParams.pageOwnerId;
    
    BTreeBuilder builder(btreeDescriptor, pLinearSegment);

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
    verifyClusterPages(lcsStream, btreeDescriptor, testName);
}


void LcsClusterAppendExecStreamTest::testImplMultiCol(
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
    lcsAppendParams.pSegment = pLinearSegment;
    
    // initialize the btree parameter portion of lcsAppendParams
    // BTree tuple desc only has one column
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);
    (lcsAppendParams.tupleDesc).push_back(attrDesc_int64);

    // BTree key only has one column which is the first column.
    (lcsAppendParams.keyProj).push_back(0);

    // output only one value(rows inserted)
    lcsAppendParams.outputTupleDesc.push_back(attrDesc_int64);
    lcsAppendParams.overwrite = false;
    lcsAppendParams.pRootMap = 0;
    
    // setup temporary btree descriptor to get an empty page to start the btree
    BTreeDescriptor btreeDescriptor;

    btreeDescriptor.segmentAccessor.pSegment = lcsAppendParams.pSegment;
    btreeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    btreeDescriptor.tupleDescriptor = lcsAppendParams.tupleDesc;
    btreeDescriptor.keyProjection = lcsAppendParams.keyProj;
    btreeDescriptor.rootPageId = newRoot ? NULL_PAGE_ID : savedRootPageId;
    btreeDescriptor.segmentId = lcsAppendParams.segmentId;
    btreeDescriptor.pageOwnerId = lcsAppendParams.pageOwnerId;
    
    BTreeBuilder builder(btreeDescriptor, pLinearSegment);

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
    verifyClusterPages(lcsStream, btreeDescriptor, testName);
}


void LcsClusterAppendExecStreamTest::testCaseSetUp()
{    
    ExecStreamUnitTestBase::testCaseSetUp();
    
    attrDesc_int64 = TupleAttributeDescriptor(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    savedRootPageId = NULL_PAGE_ID;
}

void LcsClusterAppendExecStreamTest::testSingleColNoDupNewRoot()
{
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    
    testImplSingleCol(848, true,  pGenerator, "testSingleColNoDupNewRoot");
}

/*
  Tests appending to an existing tree by first inserting into a new btree
  and then reusing the btree.  Note that all of this needs to be done within
  a single testcase with an intervening testReset().  Otherwise, the btree
  state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testSingleColNoDupOldRoot()
{
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());

    testImplSingleCol(10, true,  pGenerator,  "testSingleColNoDupOldRoot");

    testReset();

    testImplSingleCol(10, false,  pGenerator,  "testSingleColNoDupOldRoot");
}


void LcsClusterAppendExecStreamTest::testSingleColConstNewRoot()
{
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    
    testImplSingleCol(848, true,  pGenerator,  "testSingleColConstNewRoot");
}

/*
  Tests appending to an existing tree by first inserting into a new btree
  and then reusing the btree.  Note that all of this needs to be done within
  a single testcase with an intervening testReset().  Otherwise, the btree
  state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testSingleColConstOldRoot()
{
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));

    testImplSingleCol(10, true,  pGenerator,  "testSingleColConstOldRoot");

    testReset();

    testImplSingleCol(10, false,  pGenerator,  "testSingleColConstOldRoot");
}

void LcsClusterAppendExecStreamTest::testSingleColStairNewRoot()
{
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));
    
    testImplSingleCol(848, true,  pGenerator,  "testSingleColStairNewRoot");
}

/*
  Tests appending to an existing tree by first inserting into a new btree
  and then reusing the btree.  Note that all of this needs to be done within
  a single testcase with an intervening testReset().  Otherwise, the btree
  state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testSingleColStairOldRoot()
{
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1, 7));

    testImplSingleCol(10, true,  pGenerator,  "testSingleColStairOldRoot");

    testReset();

    testImplSingleCol(10, false,  pGenerator, "testSingleColStairOldRoot");
}



void LcsClusterAppendExecStreamTest::testMultiColNoDupNewRoot()
{
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());
    
    testImplMultiCol(848, 3, true,  pGenerator,  "testMultiColNoDupNewRoot");
}

/*
  Tests appending to an existing tree by first inserting into a new btree
  and then reusing the btree.  Note that all of this needs to be done within
  a single testcase with an intervening testReset().  Otherwise, the btree
  state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testMultiColNoDupOldRoot()
{
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new RampExecStreamGenerator());

    testImplMultiCol(10, 3, true,  pGenerator,"testMultiColNoDupOldRoot");

    testReset();

    testImplMultiCol(10, 3, false,  pGenerator, "testMultiColNoDupOldRoot");
}


void LcsClusterAppendExecStreamTest::testMultiColConstNewRoot()
{
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));
    
    testImplMultiCol(848, 3, true,  pGenerator,  "testMultiColConstNewRoot");
}

/*
  Tests appending to an existing tree by first inserting into a new btree
  and then reusing the btree.  Note that all of this needs to be done within
  a single testcase with an intervening testReset().  Otherwise, the btree
  state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testMultiColConstOldRoot()
{
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new ConstExecStreamGenerator(72));

    testImplMultiCol(10, 3, true,  pGenerator,  "testMultiColConstOldRoot");

    testReset();

    testImplMultiCol(10, 3, false,  pGenerator, "testMultiColConstOldRoot");
}

void LcsClusterAppendExecStreamTest::testMultiColStairNewRoot()
{
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1,  7));
    
    testImplMultiCol(848, 3, true,  pGenerator,  "testMultiColStairNewRoot");
}

/*
  Tests appending to an existing tree by first inserting into a new btree
  and then reusing the btree.  Note that all of this needs to be done within
  a single testcase with an intervening testReset().  Otherwise, the btree
  state from the first set of inserts is not preserved.
*/
void LcsClusterAppendExecStreamTest::testMultiColStairOldRoot()
{
    
    SharedMockProducerExecStreamGenerator pGenerator =
        SharedMockProducerExecStreamGenerator(new StairCaseExecStreamGenerator(1, 7));

    testImplMultiCol(10, 3, true,  pGenerator,  "testMultiColStairOldRoot");

    testReset();

    testImplMultiCol(10, 3, false,  pGenerator,  "testMultiColStairOldRoot");
}


FENNEL_UNIT_TEST_SUITE(LcsClusterAppendExecStreamTest);


// End LcsClusterAppendExecStreamTest.cpp

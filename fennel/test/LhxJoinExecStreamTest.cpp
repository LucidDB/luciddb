/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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
#include "fennel/hashexe/LhxJoinExecStream.h"
#include "fennel/sorter/ExternalSortExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/exec/ExecStreamScheduler.h"
#include "fennel/exec/ExecStreamGraph.h"
#include "fennel/cache/Cache.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class LhxJoinExecStreamTest : public ExecStreamUnitTestBase
{
    void testSequentialImpl(
        uint numRows,
        uint forcePartitionLevel,
        bool enableJoinFilter,
        bool enableSubPartStat);

    void testDupImpl(
        uint numRows,
        uint cndKeyLeft,
        uint cndKeyRight,
        uint forcePartitionLevel,
        bool enableJoinFilter,
        bool enableSubPartStat,
        bool needSort,
        bool fakeInterrupt);

    void testImpl(
        uint numInputRows,
        uint keyCount,
        uint cndKeys,
        uint numResultRows,
        TupleDescriptor &inputDesc,
        TupleDescriptor &outputDesc,
        TupleProjection &outputProj,
        SharedMockProducerExecStreamGenerator pLeftGenerator,
        SharedMockProducerExecStreamGenerator pRightGenerator,
        CompositeExecStreamGenerator &verifier,
        uint forcePartitionLevel,
        bool enableJoinFilter,
        bool enableSubPartStat,
        bool needSort,
        bool fakeInterrupt);

public:
    explicit LhxJoinExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest, testSequential);
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest, testDup1);
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest, testDup2);
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest, testConst);

/*
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testSequentialPartition);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup1Partition);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup2Partition);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testConstPartition);

        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testSequentialPartitionFilter);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup1PartitionFilter);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup2PartitionFilter);

        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testSequentialPartitionStat);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup1PartitionStat);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup2PartitionStat);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testConstPartitionStat);
*/

        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testSequentialPartitionFilterStat);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup1PartitionFilterStat);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testDup2PartitionFilterStat);
        FENNEL_UNIT_TEST_CASE(
            LhxJoinExecStreamTest,
            testConstPartitionFilterStat);

        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest, testConstCleanup);
    }

    /*
     * Match two identical sets.
     */
    void testSequential();

    void testSequentialPartition();
    void testSequentialPartitionFilter();
    void testSequentialPartitionStat();
    void testSequentialPartitionFilterStat();

    /*
     * Match these two sets:
     *  left:  0, 0  0, .. 1, 1, 1, .. 2, 2, 2, ..
     * right:  0, 0, .. 1, 1, .. 2, 2, .. 3, 3, ..
     *
     * result: 0, 0, 0, 0, 0, 0, .. 1, 1, 1, 1, 1, 1, .. 2, 2, 2, 2, 2, 2, ..
     */
    void testDup1();

    void testDup1Partition();
    void testDup1PartitionFilter();
    void testDup1PartitionStat();
    void testDup1PartitionFilterStat();

    /*
     * Match these two sets:
     *  left:  0, 0, .. 1, 1, .. 2, 2, .. 3, 3, ..
     * right:  0, 0  0, .. 1, 1, 1, .. 2, 2, 2, ..
     *
     * result: 0, 0, 0, 0, 0, 0, .. 1, 1, 1, 1, 1, 1, .. 2, 2, 2, 2, 2, 2, ..
     */
    void testDup2();

    void testDup2Partition();
    void testDup2PartitionFilter();
    void testDup2PartitionStat();
    void testDup2PartitionFilterStat();

    /*
     * Match these two sets:
     *  left:  0, 0  0, .. 0, 0, 0, .. 0, 0, 0, ..
     * right:  0, 0, .. 1, 1, .. 2, 2, .. 3, 3, ..
     *
     * result: 0, 0, 0, .. 0, 0, 0, .. 0, 0, 0, ..
     */
    void testConst();
    void testConstPartition();
    void testConstPartitionStat();
    void testConstPartitionFilterStat();
    void testConstCleanup();
};

void LhxJoinExecStreamTest::testSequential()
{
    testSequentialImpl(1000, 0, true, true);
}

void LhxJoinExecStreamTest::testSequentialPartition()
{
    testSequentialImpl(1000, 2, false, false);
}

void LhxJoinExecStreamTest::testSequentialPartitionFilter()
{
    testSequentialImpl(1000, 2, true, false);
}

void LhxJoinExecStreamTest::testSequentialPartitionStat()
{
    testSequentialImpl(1000, 2, false, true);
}

void LhxJoinExecStreamTest::testSequentialPartitionFilterStat()
{
    testSequentialImpl(1000, 2, true, true);
}

void  LhxJoinExecStreamTest::testDup1()
{
    testDupImpl(960, 16, 60, 0, false, false, false, false);
}

void  LhxJoinExecStreamTest::testDup1Partition()
{
    testDupImpl(960, 16, 60, 2, false, false, true, false);
}

void  LhxJoinExecStreamTest::testDup1PartitionFilter()
{
    testDupImpl(960, 16, 60, 2, true, false, true, false);
}

void  LhxJoinExecStreamTest::testDup1PartitionStat()
{
    testDupImpl(960, 16, 60, 2, false, true, true, false);
}

void  LhxJoinExecStreamTest::testDup1PartitionFilterStat()
{
    testDupImpl(960, 16, 60, 2, true, true, true, false);
}

void  LhxJoinExecStreamTest::testDup2()
{
    testDupImpl(960, 60, 16, 0, false, false, false, false);
}

void  LhxJoinExecStreamTest::testDup2Partition()
{
    testDupImpl(960, 60, 16, 2, false, false, true, false);
}

void  LhxJoinExecStreamTest::testDup2PartitionFilter()
{
    testDupImpl(960, 60, 16, 2, true, false, true, false);
}

void  LhxJoinExecStreamTest::testDup2PartitionStat()
{
    testDupImpl(960, 60, 16, 2, false, true, true, false);
}

void  LhxJoinExecStreamTest::testDup2PartitionFilterStat()
{
    testDupImpl(960, 60, 16, 2, true, true, true, false);
}

void  LhxJoinExecStreamTest::testConst()
{
    testDupImpl(960,  1, 60, 0, false, false, false, false);
}

void  LhxJoinExecStreamTest::testConstPartition()
{
    testDupImpl(960,  1, 60, 2, false, false, false, false);
}

void  LhxJoinExecStreamTest::testConstPartitionStat()
{
    testDupImpl(960,  1, 60, 2, false, true, false, false);
}

void  LhxJoinExecStreamTest::testConstPartitionFilterStat()
{
    testDupImpl(960,  1, 60, 2, true, true, false, false);
}

void  LhxJoinExecStreamTest::testConstCleanup()
{
    /*
     * Fake interrupt to exercise temp seg clean up code.
     */
    testDupImpl(960,  1, 60, 2, false, false, false, true);
}

void LhxJoinExecStreamTest::testSequentialImpl(
    uint numRows,
    uint forcePartitionLevel,
    bool enableJoinFilter,
    bool enableSubPartStat)
{
    uint numColsLeft;
    uint numColsRight;
    numColsRight = numColsLeft = 1;
    uint keyCount = 1;
    uint cndKeys = numRows;

    assert(keyCount <= numColsRight && keyCount <= numColsLeft);

    vector<boost::shared_ptr<ColumnGenerator< ::int64_t > > >
        leftColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator< ::int64_t > > >
        rightColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator< ::int64_t > > >
        outColumnGenerators;

    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    TupleDescriptor inputDesc;
    TupleDescriptor outputDesc;
    TupleProjection outputProj;

    uint i;

    for (i = 0; i < numColsLeft; i++) {
        leftColumnGenerators.push_back(
            SharedInt64ColumnGenerator(new SeqColumnGenerator()));
        /*
         * The two inputs have identical tuple descriptors.
         */
        inputDesc.push_back(attrDesc);

        /*
         * The result row has cols from both inputs.
         */
        outColumnGenerators.push_back(
            SharedInt64ColumnGenerator(new SeqColumnGenerator()));
        outputDesc.push_back(attrDesc);
        outputProj.push_back(i);
    }

    for (; i < numColsLeft + numColsRight; i++) {
        rightColumnGenerators.push_back(
            SharedInt64ColumnGenerator(new SeqColumnGenerator()));

        /*
         * The result row has cols from both inputs.
         */
        outColumnGenerators.push_back(
            SharedInt64ColumnGenerator(new SeqColumnGenerator()));
        outputDesc.push_back(attrDesc);
        outputProj.push_back(i);
    }

    SharedMockProducerExecStreamGenerator pLeftGenerator(
        new CompositeExecStreamGenerator(leftColumnGenerators));

    SharedMockProducerExecStreamGenerator pRightGenerator(
        new CompositeExecStreamGenerator(rightColumnGenerators));

    CompositeExecStreamGenerator verifier(outColumnGenerators);

    bool needSort = (forcePartitionLevel > 0) ? true : false;
    bool fakeInterrupt = false;

    testImpl(
        numRows, keyCount, cndKeys, numRows, inputDesc, outputDesc,
        outputProj, pLeftGenerator, pRightGenerator, verifier,
        forcePartitionLevel, enableJoinFilter, enableSubPartStat,
        needSort, fakeInterrupt);
}

void LhxJoinExecStreamTest::testDupImpl(
    uint numRows,
    uint cndKeyLeft,
    uint cndKeyRight,
    uint forcePartitionLevel,
    bool enableJoinFilter,
    bool enableSubPartStat,
    bool needSort,
    bool fakeInterrupt)
{
    assert(!fakeInterrupt || !needSort);

    uint numColsLeft;
    uint numColsRight;
    numColsRight = numColsLeft = 2;
    uint keyCount = 1;
    uint cndKeys;

    assert(keyCount <= numColsRight && keyCount <= numColsLeft);

    vector<boost::shared_ptr<ColumnGenerator< ::int64_t > > >
        leftColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator< ::int64_t > > >
        rightColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator< ::int64_t > > >
        outColumnGenerators;

    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    TupleDescriptor inputDesc;
    TupleDescriptor outputDesc;
    TupleProjection outputProj;

    uint i;

    for (i = 0; i < numColsLeft; i++) {
        leftColumnGenerators.push_back(
            SharedInt64ColumnGenerator(
                new DupColumnGenerator(numRows / cndKeyLeft)));
        outColumnGenerators.push_back(
            SharedInt64ColumnGenerator(
                new DupColumnGenerator(
                    numRows * numRows / cndKeyLeft / cndKeyRight)));

        inputDesc.push_back(attrDesc);
        outputDesc.push_back(attrDesc);
        outputProj.push_back(i);
    }

    for (; i < numColsLeft + numColsRight; i++) {
        rightColumnGenerators.push_back(
            SharedInt64ColumnGenerator(
                new DupColumnGenerator(numRows / cndKeyRight)));
        outColumnGenerators.push_back(
            SharedInt64ColumnGenerator(
                new DupColumnGenerator(
                    numRows * numRows / cndKeyLeft / cndKeyRight)));

        outputDesc.push_back(attrDesc);
        outputProj.push_back(i);
    }

    cndKeys = cndKeyRight;

    SharedMockProducerExecStreamGenerator pLeftGenerator(
        new CompositeExecStreamGenerator(leftColumnGenerators));

    SharedMockProducerExecStreamGenerator pRightGenerator(
        new CompositeExecStreamGenerator(rightColumnGenerators));

    CompositeExecStreamGenerator verifier(outColumnGenerators);

    uint numResRows =
        (cndKeyLeft > cndKeyRight)
        ? (numRows * numRows / cndKeyLeft)
        : (numRows * numRows / cndKeyRight);

    testImpl(
        numRows, keyCount, cndKeys, numResRows, inputDesc, outputDesc,
        outputProj, pLeftGenerator, pRightGenerator, verifier,
        forcePartitionLevel, enableJoinFilter, enableSubPartStat, needSort,
        fakeInterrupt);
}

void LhxJoinExecStreamTest::testImpl(
    uint numInputRows, uint keyCount, uint cndKeys, uint numResultRows,
    TupleDescriptor &inputDesc, TupleDescriptor &outputDesc,
    TupleProjection &outputProj,
    SharedMockProducerExecStreamGenerator pLeftGenerator,
    SharedMockProducerExecStreamGenerator pRightGenerator,
    CompositeExecStreamGenerator &verifier,
    uint forcePartitionLevel, bool enableJoinFilter, bool enableSubPartStat,
    bool needSort, bool fakeInterrupt)
{
    TupleProjection leftKeyProj;
    TupleProjection rightKeyProj;

    /*
     * Construct left and right input.
     */
    MockProducerExecStreamParams mockParams;
    mockParams.outputTupleDesc = inputDesc;
    mockParams.nRows = numInputRows;

    mockParams.pGenerator = pLeftGenerator;
    ExecStreamEmbryo leftInputStreamEmbryo;
    leftInputStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    leftInputStreamEmbryo.getStream()->setName("LeftInputExecStream");

    /*
     * The left and the right inputs are identical.
     */
    mockParams.pGenerator = pRightGenerator;
    ExecStreamEmbryo rightInputStreamEmbryo;
    rightInputStreamEmbryo.init(new MockProducerExecStream(), mockParams);
    rightInputStreamEmbryo.getStream()->setName("RightInputExecStream");

    /*
     * Construct the join node.
     */
    LhxJoinExecStreamParams joinParams;
    /*
     * Fields in LhxJoinExecStreamParams
     */
    joinParams.leftInner     = true;
    joinParams.leftOuter     = false;
    joinParams.rightInner    = true;
    joinParams.rightOuter    = false;

    joinParams.setopAll      = false;
    joinParams.setopDistinct = false;

    joinParams.forcePartitionLevel = forcePartitionLevel;
    joinParams.enableJoinFilter = enableJoinFilter;
    joinParams.enableSubPartStat = enableSubPartStat;
    joinParams.enableSwing = true;

    joinParams.outputProj = outputProj;
    joinParams.cndKeys = cndKeys;
    joinParams.numRows = numInputRows;

    for (int i = 0; i < keyCount; i ++) {
        joinParams.leftKeyProj.push_back(i);
        joinParams.rightKeyProj.push_back(i);
    }

    /*
     * Fields in SingleOutputExecStreamParams
     */
    joinParams.outputTupleDesc = outputDesc;
    /*
     * Fields in ExecStreamParams
     */
    joinParams.pCacheAccessor = pCache;
    int cacheSize = 100;
    joinParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, cacheSize);
    joinParams.pTempSegment = pRandomSegment;

    ExecStreamEmbryo joinStreamEmbryo;
    joinStreamEmbryo.init(new LhxJoinExecStream(), joinParams);
    joinStreamEmbryo.getStream()->setName("LhxJoinExecStream");

    SharedExecStream pOutputStream;

    if (needSort) {
        ExternalSortExecStreamParams sortParams;
        sortParams.outputTupleDesc = outputDesc;
        sortParams.distinctness = DUP_ALLOW;
        sortParams.pTempSegment = pRandomSegment;
        sortParams.pCacheAccessor = pCache;
        sortParams.scratchAccessor =
            pSegmentFactory->newScratchSegment(pCache, 10);
        sortParams.keyProj.push_back(0);
        sortParams.storeFinalRun = false;
        sortParams.partitionKeyCount = 0;
        sortParams.estimatedNumRows = MAXU;
        sortParams.earlyClose = false;
        ExecStreamEmbryo sortStreamEmbryo;
        sortStreamEmbryo.init(
            ExternalSortExecStream::newExternalSortExecStream(), sortParams);
        sortStreamEmbryo.getStream()->setName("ExternalSortExecStream");

        pOutputStream = prepareConfluenceTransformGraph(
            leftInputStreamEmbryo, rightInputStreamEmbryo, joinStreamEmbryo,
            sortStreamEmbryo);
    } else {
        pOutputStream = prepareConfluenceGraph(
            leftInputStreamEmbryo, rightInputStreamEmbryo, joinStreamEmbryo);
    }

    // after partitioning the order might not be the same as the input, so add
    // a sort before verifying the output

    verifyOutput(
        *pOutputStream,
        fakeInterrupt ? 1 : numResultRows,
        verifier,
        fakeInterrupt);

    if (fakeInterrupt) {
        // simulate error cleanup
        pScheduler->stop();
        pGraph->close();
    }

    BOOST_CHECK_EQUAL(0, pRandomSegment->getAllocatedSizeInPages());
}

FENNEL_UNIT_TEST_SUITE(LhxJoinExecStreamTest);

// End LhxJoinExecStreamTest.cpp

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
#include "fennel/lucidera/hashexe/LhxJoinExecStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/exec/MockProducerExecStream.h"
#include "fennel/exec/ExecStreamEmbryo.h"
#include "fennel/cache/Cache.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class LhxJoinExecStreamTest : public ExecStreamUnitTestBase
{
    void testSequentialImpl(bool leftOuter);

    void testImpl(
        uint numInputRows, uint keyCount, uint cndKeys, uint numResultRows,
        TupleDescriptor &inputDesc, TupleDescriptor &outputDesc,
        TupleProjection &outputProj, bool leftOuter,
        SharedMockProducerExecStreamGenerator pLeftGenerator,
        SharedMockProducerExecStreamGenerator pRightGenerator,
        CompositeExecStreamGenerator &verifier);
    
public:
    explicit LhxJoinExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest,testSequential);
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest,testSequentialLeftOuter);
        FENNEL_UNIT_TEST_CASE(LhxJoinExecStreamTest,testRepeatSequential);
    }
    
    /*
     * Match two identical sets.
     */
    void testSequential();

    /*
     * Match two identical sets.
     */
    void testSequentialLeftOuter();

    /*
     * Match these two sets:
     *  left:  0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, ...
     * right:  0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, ...
     *
     * result: 0, 0, 0, 0,    ...,  1, 1, 1, 1,    ...
     */
    void testRepeatSequential();
};

void LhxJoinExecStreamTest::testSequential()
{
    testSequentialImpl(false);
}

void LhxJoinExecStreamTest::testSequentialLeftOuter()
{
    testSequentialImpl(true);
}

void LhxJoinExecStreamTest::testSequentialImpl(bool leftOuter)
{
    uint numRows = 100;
    uint numColsLeft;
    uint numColsRight;
    numColsRight = numColsLeft = 1;
    uint keyCount = 1;
    uint cndKeys = numRows;

    assert (keyCount <= numColsRight && keyCount <= numColsLeft);

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > leftColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > rightColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > outColumnGenerators;

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

    testImpl(numRows, keyCount, cndKeys, numRows,
        inputDesc, outputDesc, outputProj, leftOuter,
        pLeftGenerator, pRightGenerator, verifier);
}

void LhxJoinExecStreamTest::testRepeatSequential()
{
    uint numRows = 128;
    uint numColsLeft;
    uint numColsRight;
    numColsRight = numColsLeft = 2;
    uint keyCount = 1;
    uint cndKeys;

    assert (keyCount <= numColsRight && keyCount <= numColsLeft);

    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > leftColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > rightColumnGenerators;
    vector<boost::shared_ptr<ColumnGenerator<int64_t> > > outColumnGenerators;

    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_64));

    TupleDescriptor inputDesc;
    TupleDescriptor outputDesc;
    TupleProjection outputProj;
    
    uint i;

    uint cndKeyLeft = 8;
    uint cndKeyRight = 16;

    for (i = 0; i < numColsLeft; i++) {
        leftColumnGenerators.push_back(
            SharedInt64ColumnGenerator(new RepeatingSeqColumnGenerator(cndKeyLeft)));
        outColumnGenerators.push_back(
            SharedInt64ColumnGenerator(
                new DupRepeatingSeqColumnGenerator(cndKeyLeft, numRows/cndKeyRight)));

        inputDesc.push_back(attrDesc);
        outputDesc.push_back(attrDesc);        
        outputProj.push_back(i);
    }

    for (; i < numColsLeft + numColsRight; i++) {
        rightColumnGenerators.push_back(
            SharedInt64ColumnGenerator(new RepeatingSeqColumnGenerator(cndKeyRight)));
        outColumnGenerators.push_back(
            SharedInt64ColumnGenerator(
                new DupRepeatingSeqColumnGenerator(cndKeyLeft, numRows/cndKeyRight)));

        outputDesc.push_back(attrDesc);        
        outputProj.push_back(i);
    }

    cndKeys = cndKeyRight;

    SharedMockProducerExecStreamGenerator pLeftGenerator(
        new CompositeExecStreamGenerator(leftColumnGenerators));

    SharedMockProducerExecStreamGenerator pRightGenerator(
        new CompositeExecStreamGenerator(rightColumnGenerators));

    CompositeExecStreamGenerator verifier(outColumnGenerators);

    testImpl(numRows, keyCount, cndKeys, (numRows * numRows/cndKeyRight),
        inputDesc, outputDesc, outputProj, false,
        pLeftGenerator, pRightGenerator, verifier);
}

void LhxJoinExecStreamTest::testImpl(
    uint numInputRows, uint keyCount, uint cndKeys, uint numResultRows,
    TupleDescriptor &inputDesc, TupleDescriptor &outputDesc,
    TupleProjection &outputProj, bool leftOuter,
    SharedMockProducerExecStreamGenerator pLeftGenerator,
    SharedMockProducerExecStreamGenerator pRightGenerator,
    CompositeExecStreamGenerator &verifier)
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
    leftInputStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    leftInputStreamEmbryo.getStream()->setName("LeftInputExecStream");

    /*
     * The left and the right inputs are identical.
     */
    mockParams.pGenerator = pRightGenerator;
    ExecStreamEmbryo rightInputStreamEmbryo;
    rightInputStreamEmbryo.init(new MockProducerExecStream(),mockParams);
    rightInputStreamEmbryo.getStream()->setName("RightInputExecStream");

    /*
     * Construct the join node.
     */
    LhxJoinExecStreamParams joinParams;
    /*
     * Fields in LhxJoinExecStreamParams
     */
    joinParams.leftInner = true;
    joinParams.leftOuter = leftOuter;
    joinParams.rightInner = true;
    joinParams.rightOuter = false;
    joinParams.eliminateDuplicate = false;
    joinParams.outputProj = outputProj;
    joinParams.cndKeys = cndKeys;
    joinParams.numRows = numInputRows;
    joinParams.aggsCount = 0;

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
    joinParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 100);
 
    ExecStreamEmbryo joinStreamEmbryo;
    joinStreamEmbryo.init(new LhxJoinExecStream(),joinParams);
    joinStreamEmbryo.getStream()->setName("LhxJoinExecStream");
    
   
    SharedExecStream pOutputStream = prepareConfluenceGraph(
        leftInputStreamEmbryo, rightInputStreamEmbryo, joinStreamEmbryo);

    verifyOutput(*pOutputStream, numResultRows, verifier);
}

FENNEL_UNIT_TEST_SUITE(LhxJoinExecStreamTest);

// End LhxJoinExecStreamTest.cpp

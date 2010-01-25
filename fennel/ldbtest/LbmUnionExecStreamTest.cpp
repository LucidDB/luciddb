/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/lbm/LbmChopperExecStream.h"
#include "fennel/lbm/LbmUnionExecStream.h"
#include "fennel/ldbtest/LbmExecStreamTestBase.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

// declares an array on the stack and initializes it to a value
#define DECLARE_CONSTANT_ARRAY(name, value, size) \
FixedBuffer name[size]; \
memset(name, value, size);

// check whether code causes an exception
#define CHECK_THROWN(code) \
{ \
    bool thrown = false; \
    try { \
        code; \
    } catch (...) { \
        thrown = true; \
    } \
    BOOST_CHECK(thrown); \
}

class LbmUnionExecStreamTest : public LbmExecStreamTestBase
{
    boost::shared_array<PBuffer> ppBuffers;
    uint nBuffers;

    // allocate a byte buffer
    SharedByteBuffer allocateBuffer(uint nBuffers, uint bufferSize);

    // free the byte buffer
    void deleteBuffer();

    // run the byte buffer through a couple of tests
    void testBuffer(SharedByteBuffer pByteBuffer);

    // check the results stored in a merge area with a fixed buffer
    void verifyMerge(LbmUnionMergeArea area, PConstBuffer reference);

    // test the results of running union on a set of number streams
    void testUnion(uint nRows, std::vector<LbmNumberStreamInput> inputData);

public:
    explicit LbmUnionExecStreamTest()
    {
        // NOTE: this test only works reliably for one page due to the
        // inconsistent tuple building process
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testSinglePageBuffer);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testTwoPageBuffer);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testMultiPageBuffer);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testTwoInputs);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testThreeInputs);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testLargeRids);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testSparse);
        FENNEL_UNIT_TEST_CASE(LbmUnionExecStreamTest, testGaps);
    }

    void testSinglePageBuffer();
    void testTwoPageBuffer();
    void testMultiPageBuffer();

    void testTwoInputs();
    void testThreeInputs();
    void testLargeRids();
    void testSparse();
    void testGaps();
};

void LbmUnionExecStreamTest::testSinglePageBuffer()
{
    SharedByteBuffer pByteBuffer = allocateBuffer(1, 16);
    testBuffer(pByteBuffer);
    deleteBuffer();
}

void LbmUnionExecStreamTest::testTwoPageBuffer()
{
    SharedByteBuffer pByteBuffer = allocateBuffer(2, 8);
    testBuffer(pByteBuffer);
    deleteBuffer();
}

void LbmUnionExecStreamTest::testMultiPageBuffer()
{
    SharedByteBuffer pByteBuffer = allocateBuffer(4, 4);
    testBuffer(pByteBuffer);
    deleteBuffer();
}

void LbmUnionExecStreamTest::testTwoInputs()
{
    uint nRows = 1000;
    std::vector<LbmNumberStreamInput> inputData;

    // evens from 20 .. 500
    LbmNumberStreamInput input1;
    input1.pStream =
        SharedNumberStream(new SkipNumberStream(20, 500, 2));
    input1.bitmapSize = 4;
    inputData.push_back(input1);

    // multiples of 3 from 300 .. 990
    LbmNumberStreamInput input2;
    input2.pStream =
        SharedNumberStream(new SkipNumberStream(300, 990, 3));
    input2.bitmapSize = 8;
    inputData.push_back(input2);

    testUnion(nRows, inputData);
}

void LbmUnionExecStreamTest::testThreeInputs()
{
    uint nRows = 1051;
    std::vector<LbmNumberStreamInput> inputData;

    // multiples of 7 from 21 .. 700
    LbmNumberStreamInput input1;
    input1.pStream =
        SharedNumberStream(new SkipNumberStream(21, 700, 7));
    input1.bitmapSize = 4;
    inputData.push_back(input1);

    // multiples of 3 from 300 .. 990
    LbmNumberStreamInput input2;
    input2.pStream =
        SharedNumberStream(new SkipNumberStream(300, 990, 3));
    input2.bitmapSize = 8;
    inputData.push_back(input2);

    // multiples of 5 from 500 .. 1050
    LbmNumberStreamInput input3;
    input3.pStream =
        SharedNumberStream(new SkipNumberStream(500, 1050, 5));
    input3.bitmapSize = 8;
    inputData.push_back(input3);

    testUnion(nRows, inputData);
}

void LbmUnionExecStreamTest::testLargeRids()
{
    uint nRows = 5001000;
    std::vector<LbmNumberStreamInput> inputData;

    // multiples of 4 from 5000020 .. 5000500
    LbmNumberStreamInput input1;
    input1.pStream =
        SharedNumberStream(new SkipNumberStream(5000020, 5000500, 4));
    input1.bitmapSize = 4;
    inputData.push_back(input1);

    // multiples of 5 from 5000300 .. 5000990
    LbmNumberStreamInput input2;
    input2.pStream =
        SharedNumberStream(new SkipNumberStream(5000300, 5000990, 5));
    input2.bitmapSize = 8;
    inputData.push_back(input2);

    testUnion(nRows, inputData);
}

void LbmUnionExecStreamTest::testSparse()
{
    uint nRows = 2900;
    std::vector<LbmNumberStreamInput> inputData;

    // multiples of 13 from 26 .. 1300
    LbmNumberStreamInput input1;
    input1.pStream =
        SharedNumberStream(new SkipNumberStream(26, 1300, 13));
    input1.bitmapSize = 4;
    inputData.push_back(input1);

    // multiples of 17 from 340 ... 1700
    LbmNumberStreamInput input2;
    input2.pStream =
        SharedNumberStream(new SkipNumberStream(340, 1700, 17));
    input2.bitmapSize = 8;
    inputData.push_back(input2);

    // multiples of 11 from 1100 .. 2200
    LbmNumberStreamInput input3;
    input3.pStream =
        SharedNumberStream(new SkipNumberStream(1100, 2200, 11));
    input3.bitmapSize = 8;
    inputData.push_back(input3);

    testUnion(nRows, inputData);
}

void LbmUnionExecStreamTest::testGaps()
{
    uint nRows = 2000;
    std::vector<LbmNumberStreamInput> inputData;

    // multiples of 13 from 26 .. 520
    LbmNumberStreamInput input1;
    input1.pStream =
        SharedNumberStream(new SkipNumberStream(26, 520, 13));
    input1.bitmapSize = 4;
    inputData.push_back(input1);

    // multiples of 17 from 680 .. 1020
    LbmNumberStreamInput input2;
    input2.pStream =
        SharedNumberStream(new SkipNumberStream(680, 1020, 17));
    input2.bitmapSize = 8;
    inputData.push_back(input2);

    // multiples of 11 from 1199 ..
    LbmNumberStreamInput input3;
    input3.pStream =
        SharedNumberStream(new SkipNumberStream(1320, 1540, 11));
    input3.bitmapSize = 8;
    inputData.push_back(input3);

    // multiples of 19 from 3800 .. 7600
    LbmNumberStreamInput input4;
    input4.pStream =
        SharedNumberStream(new SkipNumberStream(1330, 1900, 11));
    input4.bitmapSize = 8;
    inputData.push_back(input4);

    testUnion(nRows, inputData);
}

SharedByteBuffer LbmUnionExecStreamTest::allocateBuffer(
    uint nBuffers, uint bufferSize)
{
    ppBuffers.reset(new PBuffer[nBuffers]);
    this->nBuffers = nBuffers;
    for (uint i = 0; i < nBuffers; i++) {
        ppBuffers[i] = new FixedBuffer[bufferSize];
    }

    SharedByteBuffer pByteBuffer(new ByteBuffer());
    pByteBuffer->init(ppBuffers, nBuffers, bufferSize);
    return pByteBuffer;
}

void LbmUnionExecStreamTest::deleteBuffer()
{
    for (uint i = 0; i < nBuffers; i++) {
        delete [] ppBuffers[i];
    }
}

void LbmUnionExecStreamTest::testBuffer(SharedByteBuffer pByteBuffer)
{
    DECLARE_CONSTANT_ARRAY(zeroes, 0, 16);
    DECLARE_CONSTANT_ARRAY(ones, 1, 16);
    DECLARE_CONSTANT_ARRAY(twos, 2, 16);
    DECLARE_CONSTANT_ARRAY(fours, 4, 16);
    DECLARE_CONSTANT_ARRAY(maxByte, 255, 16);

    LbmUnionMergeArea mergeArea;
    mergeArea.init(pByteBuffer);

    // merge area should be able to use arbitrary offsets and zero values
    // 0000 0011 1111 11xx
    mergeArea.advance(10000);
    mergeArea.mergeMem(10006, ones, 8);
    FixedBuffer result0[16] = { 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1 };
    verifyMerge(mergeArea, result0);

    // xxxx x011 1111 11xx
    mergeArea.advance(10005);
    FixedBuffer result1[16] = { 0, 1, 1, 1, 1, 1, 1, 1, 1 };
    verifyMerge(mergeArea, result1);

    // merge area should wrap around
    // 2222 2011 3333 3322
    mergeArea.mergeMem(10008, twos, 13);
    FixedBuffer result2[16] = {
        0, 1, 1, 3, 3, 3, 3, 3,
        3, 2, 2, 2, 2, 2, 2, 2 };
    verifyMerge(mergeArea, result2);

    // 2222 2xx1 3333 3322
    mergeArea.advance(10007);
    FixedBuffer result3[16] = {
        1, 3, 3, 3, 3, 3, 3,
        2, 2, 2, 2, 2, 2, 2 };
    verifyMerge(mergeArea, result3);

    // 6662 2xx1 3377 7766
    mergeArea.mergeMem(10010, fours, 9);
    FixedBuffer result4[16] = {
        1, 3, 3, 7, 7, 7, 7,
        6, 6, 6, 6, 6, 2, 2 };
    verifyMerge(mergeArea, result4);

    // 0011 1122 2244 4400
    mergeArea.advance(10030);
    mergeArea.mergeMem(10040, fours, 4);
    mergeArea.mergeMem(10036, twos, 4);
    mergeArea.mergeMem(10032, ones, 4);
    mergeArea.mergeMem(10044, zeroes, 2);
    FixedBuffer result5[16] = {
        0, 0, 1, 1, 1, 1, 2, 2,
        2, 2, 4, 4, 4, 4, 0, 0 };
    verifyMerge(mergeArea, result5);

    // index too low
    mergeArea.advance(10032);
    // CHECK_THROWN(mergeArea.getByte(10031);

    // index too high
    // CHECK_THROWN(mergeArea.getByte(10046));

    // can't write past bound
    // CHECK_THROWN(mergeArea.mergeMem(10048, zeroes, 2));

    // can't go backwards
    // CHECK_THROWN(mergeArea.advance(10031));

    // this should be ok
    mergeArea.advance(10032);
}

void LbmUnionExecStreamTest::verifyMerge(
    LbmUnionMergeArea area, PConstBuffer reference)
{
    LbmByteNumberPrimitive start = area.getStart();
    uint size = opaqueToInt(area.getEnd() - start);

    for (uint i = 0; i < size; i++) {
        BOOST_CHECK_EQUAL(area.getByte(start + i), reference[i]);
    }
}

void LbmUnionExecStreamTest::testUnion(
    uint nRows, std::vector<LbmNumberStreamInput> inputData)
{
    uint nInputs = inputData.size();

    // set up expected result (with fresh copies of input data)
    UnionNumberStream *pUnion = new UnionNumberStream();
    for (uint i = 0; i < nInputs; i++) {
        SharedNumberStream pChild(inputData[i].pStream->clone());
        pUnion->addChild(pChild);
    }
    LbmNumberStreamInput expectedData;
    expectedData.pStream = SharedNumberStream(pUnion);
    expectedData.bitmapSize = resultBitmapSize(nRows);

    // + 1 is for the precalculated results buffer
    boost::scoped_array<BitmapInput> bmInputs;
    bmInputs.reset(new BitmapInput[nInputs + 1]);

    // combine inputs into a single values exec stream
    uint totalSize = 0;
    uint totalBitmaps = 0;
    for (uint i = 0; i < nInputs; i++) {
        initBitmapInput(bmInputs[i], nRows, inputData[i]);
        totalSize += bmInputs[i].currBufSize;
        totalBitmaps += bmInputs[i].nBitmaps;
    }

    BitmapInput bmCombined;
    bmCombined.bufArray.reset(new FixedBuffer[totalSize]);
    bmCombined.currBufSize = bmCombined.fullBufSize = totalSize;
    bmCombined.nBitmaps = totalBitmaps;
    PBuffer pCurrent = bmCombined.bufArray.get();
    for (uint i = 0; i < nInputs; i++) {
        memcpy(pCurrent, bmInputs[i].bufArray.get(), bmInputs[i].currBufSize);
        pCurrent += bmInputs[i].currBufSize;
    }

    ValuesExecStreamParams valuesParams;
    ExecStreamEmbryo valuesStreamEmbryo;
    initValuesExecStream(
        0, valuesParams, valuesStreamEmbryo, bmCombined);

    // set up precalculated result buffer
    initBitmapInput(bmInputs[nInputs], nRows, expectedData);

    // build values -> chopper -> sorter -> union transforms
    std::vector<ExecStreamEmbryo> transformEmbryoList;

    LbmChopperExecStreamParams chopperParams;
    chopperParams.ridLimitParamId = DynamicParamId(1);
    chopperParams.outputTupleDesc = bitmapTupleDesc;
    chopperParams.scratchAccessor.pSegment = pRandomSegment;
    chopperParams.scratchAccessor.pCacheAccessor = pCache;
    ExecStreamEmbryo chopperEmbryo;
    chopperEmbryo.init(new LbmChopperExecStream(), chopperParams);
    chopperEmbryo.getStream()->setName("ChopperExecStream");
    transformEmbryoList.push_back(chopperEmbryo);

    ExternalSortExecStreamParams sortParams;
    ExecStreamEmbryo sortEmbryo;
    initSorterExecStream(sortParams, sortEmbryo, bitmapTupleDesc);
    transformEmbryoList.push_back(sortEmbryo);

    LbmUnionExecStreamParams unionParams;
    unionParams.maxRid = (LcsRid) 0;
    unionParams.ridLimitParamId = DynamicParamId(1);
    unionParams.startRidParamId = DynamicParamId(0);
    unionParams.segmentLimitParamId = DynamicParamId(0);
    unionParams.outputTupleDesc = bitmapTupleDesc;
    unionParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);
    ExecStreamEmbryo unionEmbryo;
    unionEmbryo.init(new LbmUnionExecStream(), unionParams);
    unionEmbryo.getStream()->setName("UnionExecStream");
    transformEmbryoList.push_back(unionEmbryo);

    SharedExecStream pOutputStream = prepareTransformGraph(
        valuesStreamEmbryo, transformEmbryoList);

    if (bmInputs[nInputs].bufArray.get()) {
        bitmapTupleAccessor.setCurrentTupleBuf(
            bmInputs[nInputs].bufArray.get());
    }
    verifyBufferedOutput(
        *pOutputStream, bitmapTupleDesc, bmInputs[nInputs].nBitmaps,
        bmInputs[nInputs].bufArray.get());
}

FENNEL_UNIT_TEST_SUITE(LbmUnionExecStreamTest);

// End LbmUnionExecStreamTest.cpp

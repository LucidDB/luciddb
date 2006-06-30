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
#include "fennel/lucidera/test/LbmExecStreamTestBase.h"
#include "fennel/lucidera/bitmap/LbmMinusExecStream.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for Minus exec stream
 */
class LbmMinusExecStreamTest : public LbmExecStreamTestBase
{
    void testMinus(
        uint nInputs, uint nRows, std::vector<InputData> const &inputData);

    /**
     * Perform [minuend] - [subtrahend] where minuend is based on
     * repeatSeqValues and subtrahend is 0, n, 2n, 3n, ...
     */
    void testRestartingMinus(
        uint nRows,
        std::vector<int> const &repeatSeqValues,
        uint subtrahendInterval);

    void newMinusStream(
        LbmMinusExecStreamParams &params,
        ExecStreamEmbryo &embryo,
        TupleDescriptor const &outputDesc);

public:
    explicit LbmMinusExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, test2Inputs);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, test3Inputs);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testLargeOutput);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testAnchorLarger1);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testAnchorLarger2);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testChildrenLarger);

        // tests for restarting minus
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testEvens);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testNines);
        FENNEL_UNIT_TEST_CASE(LbmMinusExecStreamTest, testClose);
    }

    void test2Inputs();
    void test3Inputs();
    void testLargeOutput();
    void testAnchorLarger1();
    void testAnchorLarger2();
    void testChildrenLarger();

    void testEvens();
    void testNines();
    void testClose();
};

/**
 * Generates repeating tuples resulting from minus such as
 * <pre>
 * 0, 0, 0 [distinct row 0, repeats 12 times]
 * 0, 1, 1 [distinct row 1, repeats 11 times]
 * 0, 2, 2 [distinct row 1, repeats 12 times]
 * ...
 * </pre>
 */
class RestartingMinusExecStreamGenerator
    : public MockProducerExecStreamGenerator
{
protected:
    uint nKeys;
    std::vector<int> repeatSeqValues;
    uint subtrahendInterval;
    uint interval;
    boost::shared_array<uint> changeIndexes;
    uint current;
    uint lastRow;

public:
    RestartingMinusExecStreamGenerator(
        uint nRows, std::vector<int> repeatSeqValues, uint subtrahendInterval)
    {
        this->nKeys = repeatSeqValues.size();
        this->repeatSeqValues = repeatSeqValues;
        interval = LbmExecStreamTestBase::getTupleInterval(repeatSeqValues);

        // find the number of times each key value repeats
        boost::scoped_array<uint> valueCounts;
        valueCounts.reset(new uint[interval]);
        for (uint i = 0; i < interval; i++) {
            valueCounts[i] = getValueCount(nRows, i);
        }
        // account for subtrahend
        for (uint iRow = 0; iRow < nRows; iRow += subtrahendInterval) {
            valueCounts[iRow % interval]--;
        }

        // find indexes where the values change
        changeIndexes.reset(new uint[interval]);
        changeIndexes[0] = valueCounts[0];
        for (uint i = 1; i < interval; i++) {
            changeIndexes[i] = changeIndexes[i-1] + valueCounts[i];
        }
        current = 0;
        lastRow = 0;
    }
    
    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        // must be generated in order
        assert (lastRow <= iRow);
        lastRow = iRow;

        if (iRow >= changeIndexes[current]) {
            current++;
            assert (current < interval);
        }
        assert (iCol < nKeys);
        return current % repeatSeqValues[iCol];
    }

    /**
     * Returns the number of times a value repeats over nRows
     */
    uint getValueCount(uint nRows, uint iValue)
    {
        uint nCopies = nRows / interval;
        if (iValue < nRows % interval) {
            nCopies++;
        }
        return nCopies;
    }

    /**
     * Returns the total number of rows the result set should have
     */
    uint getRowCount()
    {
        return changeIndexes[interval - 1];
    }
};
    
void LbmMinusExecStreamTest::test2Inputs()
{
    uint nInputs = 2;
    uint nRows = 1000;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- all bits set
    input.bitmapSize = 4;
    input.startRid = LcsRid(10);
    input.skipRows = 1;
    inputData.push_back(input);

    // bitmap input 2 -- every other bit set
    input.bitmapSize = 8;
    input.startRid = LcsRid(0);
    input.skipRows = 2;
    inputData.push_back(input);

    // expected result -- every other bit set, starting at 11
    input.bitmapSize = resultBitmapSize(11, nRows);
    input.startRid = LcsRid(11);
    input.skipRows = 2;
    inputData.push_back(input);

    testMinus(nInputs, nRows, inputData);
}

void LbmMinusExecStreamTest::test3Inputs()
{
    uint nInputs = 3;
    uint nRows = 1000;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every rid set, starting at 22
    input.bitmapSize = 9;
    input.startRid = LcsRid(22);
    input.skipRows = 1;
    inputData.push_back(input);

    // bitmap input 2 -- every 3 rids set, starting at 2
    input.bitmapSize = 8;
    input.startRid = LcsRid(2);
    input.skipRows = 3;
    inputData.push_back(input);

    // bitmap input 3 -- every 3 set, starting at 3
    input.bitmapSize = 10;
    input.startRid = LcsRid(3);
    input.skipRows = 3;
    inputData.push_back(input);

    // expected result -- every 3 rids set, starting at 22
    input.bitmapSize = resultBitmapSize(22, nRows);
    input.startRid = LcsRid(22);
    input.skipRows = 3;
    inputData.push_back(input);

    testMinus(nInputs, nRows, inputData);
}

/**
 * This testcase exercises building up a large number segments in the
 * resultant bitmaps
 */
void LbmMinusExecStreamTest::testLargeOutput()
{
    uint nInputs = 16;
    uint nRows = 100000;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- all bits set
    input.bitmapSize = 1;
    input.startRid = LcsRid(0);
    input.skipRows = 1;
    inputData.push_back(input);

    // bitmap input 2 -- every 16 bits set, starting at 1 
    input.bitmapSize = 1;
    input.startRid = LcsRid(1);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 3 -- every 16 bits set, starting at 2
    input.bitmapSize = 1;
    input.startRid = LcsRid(2);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 4 -- every 16 bits set, starting at 3
    input.bitmapSize = 1;
    input.startRid = LcsRid(3);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 5 -- every 16 bits set, starting at 4
    input.bitmapSize = 1;
    input.startRid = LcsRid(4);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 6 -- every 16 bits set, starting at 5
    input.bitmapSize = 1;
    input.startRid = LcsRid(5);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 7 -- every 16 bits set, starting at 6
    input.bitmapSize = 1;
    input.startRid = LcsRid(6);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 8 -- every 16 bits set, starting at 7
    input.bitmapSize = 1;
    input.startRid = LcsRid(7);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 9 -- every 16 bits set, starting at 8
    input.bitmapSize = 1;
    input.startRid = LcsRid(8);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 10 -- every 16 bits set, starting at 9
    input.bitmapSize = 1;
    input.startRid = LcsRid(9);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 11 -- every 16 bits set, starting at 10 
    input.bitmapSize = 1;
    input.startRid = LcsRid(10);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 12 -- every 16 bits set, starting at 11 
    input.bitmapSize = 1;
    input.startRid = LcsRid(11);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 13 -- every 16 bits set, starting at 12 
    input.bitmapSize = 1;
    input.startRid = LcsRid(12);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 14 -- every 16 bits set, starting at 13 
    input.bitmapSize = 1;
    input.startRid = LcsRid(13);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 15 -- every 16 bits set, starting at 14 
    input.bitmapSize = 1;
    input.startRid = LcsRid(14);
    input.skipRows = 16;
    inputData.push_back(input);

    // bitmap input 16 -- every 16 bits set, starting at 15 
    input.bitmapSize = 1;
    input.startRid = LcsRid(15);
    input.skipRows = 16;
    inputData.push_back(input);

    // expected result -- every 16 bits set, starting at 0
    input.bitmapSize = resultBitmapSize(0, nRows);
    input.startRid = LcsRid(0);
    input.skipRows = 16;
    inputData.push_back(input);

    testMinus(nInputs, nRows, inputData);
}

/**
 * This testcase exercises the case where the anchor input has more rids than
 * its children; input 1 exhausts before input 2
 */
void LbmMinusExecStreamTest::testAnchorLarger1()
{
    uint nInputs = 3;
    uint nRows = 100;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every 17 bits set (0, 17, ..., 85)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 17;
    inputData.push_back(input);

    // bitmap input 2 -- every 60 bits (0, 60)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 60;
    inputData.push_back(input);

    // bitmap input 3 -- every 70 bits set (0, 70)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 70;
    inputData.push_back(input);

    // expected result -- every 17 bits set, starting at 17
    input.bitmapSize = resultBitmapSize(17, nRows);
    input.startRid = LcsRid(17);
    input.skipRows = 17;
    inputData.push_back(input);

    testMinus(nInputs, nRows, inputData);
}

/**
 * This testcase also exercises the case where the anchor input has more rids
 * than its children, but both inputs exhaust at same point
 */
void LbmMinusExecStreamTest::testAnchorLarger2()
{
    uint nInputs = 3;
    uint nRows = 100;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every 17 bits set (0, 17, ..., 85)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 17;
    inputData.push_back(input);

    // bitmap input 2 -- every 60 bits (0, 60)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 60;
    inputData.push_back(input);

    // bitmap input 3 -- every 61 bits set (0, 61)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 61;
    inputData.push_back(input);

    // expected result -- every 17 bits set, starting at 17
    input.bitmapSize = resultBitmapSize(17, nRows);
    input.startRid = LcsRid(17);
    input.skipRows = 17;
    inputData.push_back(input);

    testMinus(nInputs, nRows, inputData);
}

/**
 * This testcase exercises the case where the children input are larger than
 * the anchor, and none of the bits overlap.
 */
void LbmMinusExecStreamTest::testChildrenLarger()
{
    uint nInputs = 3;
    uint nRows = 100;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- (0, 60)
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 60;
    inputData.push_back(input);

    // bitmap input 2 -- only bit 70 set
    input.bitmapSize = 10;
    input.startRid = LcsRid(70);
    input.skipRows = 70;
    inputData.push_back(input);

    // bitmap input 3 -- only bit 80 set
    input.bitmapSize = 10;
    input.startRid = LcsRid(80);
    input.skipRows = 80;
    inputData.push_back(input);

    // expected result -- (0, 60)
    input.bitmapSize = resultBitmapSize(0, nRows);
    input.startRid = LcsRid(0);
    input.skipRows = 60;
    inputData.push_back(input);

    testMinus(nInputs, nRows, inputData);
}

void LbmMinusExecStreamTest::testEvens()
{
    uint nRows = 1000;
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(1);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);
    uint subtrahendInterval = 2;

    testRestartingMinus(nRows, repeatSeqValues, subtrahendInterval);
}

void LbmMinusExecStreamTest::testNines()
{
    uint nRows = 1000;
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(1);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);
    uint subtrahendInterval = 9;

    testRestartingMinus(nRows, repeatSeqValues, subtrahendInterval);
}

void LbmMinusExecStreamTest::testClose()
{
    uint nRows = 1000;
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(2);
    repeatSeqValues.push_back(3);
    uint subtrahendInterval = 4;

    testRestartingMinus(nRows, repeatSeqValues, subtrahendInterval);
}

void LbmMinusExecStreamTest::testMinus(
    uint nInputs, uint nRows, std::vector<InputData> const &inputData)
{
    boost::scoped_array<BitmapInput> bmInputs;
    boost::scoped_array<ValuesExecStreamParams> valuesParams;
    std::vector<ExecStreamEmbryo> valuesStreamEmbryoList;
    ExecStreamEmbryo valuesStreamEmbryo;

    // +1 is for expected result
    assert(inputData.size() == nInputs + 1);

    bmInputs.reset(new BitmapInput[nInputs + 1]);
    valuesParams.reset(new ValuesExecStreamParams[nInputs]);

    // setup values exec stream for each input
    for (uint i = 0; i < nInputs; i++) {
        initBitmapInput(bmInputs[i], nRows, inputData[i]);
        initValuesExecStream(
            i, valuesParams[i], valuesStreamEmbryo, bmInputs[i]);
        valuesStreamEmbryoList.push_back(valuesStreamEmbryo);
    }
    // set up expected result
    if (inputData[nInputs].bitmapSize > 0) {
        initBitmapInput(bmInputs[nInputs], nRows, inputData[nInputs]);
    } else {
        bmInputs[nInputs].bufArray.reset();
        bmInputs[nInputs].nBitmaps = 0;
    }

    LbmMinusExecStreamParams minusParams;
    ExecStreamEmbryo minusEmbryo;
    newMinusStream(minusParams, minusEmbryo, bitmapTupleDesc);

    SharedExecStream pOutputStream = prepareConfluenceGraph(
        valuesStreamEmbryoList, minusEmbryo);

    verifyBufferedOutput(
        *pOutputStream, bitmapTupleDesc, bmInputs[nInputs].nBitmaps,
        bmInputs[nInputs].bufArray.get());
}

void LbmMinusExecStreamTest::testRestartingMinus(
    uint nRows,
    std::vector<int> const &repeatSeqValues,
    uint subtrahendInterval)
{
    uint nKeys = repeatSeqValues.size();

    // initialize minuend [ (keys), (bitmaps) ]
    initKeyBitmap(nRows, repeatSeqValues);
    ValuesExecStreamParams valuesParams;
    valuesParams.outputTupleDesc = keyBitmapTupleDesc;
    valuesParams.pTupleBuffer = keyBitmapBuf;
    valuesParams.bufSize = keyBitmapBufSize;

    ExecStreamEmbryo minuendEmbryo;
    minuendEmbryo.init(new ValuesExecStream(), valuesParams);
    minuendEmbryo.getStream()->setName("MinuendValuesExecStream");

    // initialize subtrahend [ (bitmaps) ]
    InputData subtrahendData;
    subtrahendData.startRid = LcsRid(0);
    subtrahendData.skipRows = subtrahendInterval;
    subtrahendData.bitmapSize = 8;

    BitmapInput bmInput;
    initBitmapInput(bmInput, nRows, subtrahendData);

    ValuesExecStreamParams subtrahendParams;
    ExecStreamEmbryo subtrahendEmbryo;
    initValuesExecStream(0, subtrahendParams, subtrahendEmbryo, bmInput);

    // initialize the minus stream
    LbmMinusExecStreamParams minusParams;
    ExecStreamEmbryo minusEmbryo;
    newMinusStream(minusParams, minusEmbryo, keyBitmapTupleDesc);

    // initialize normalizer
    ExecStreamEmbryo normalizerEmbryo;
    LbmNormalizerExecStreamParams normalizerParams;
    initNormalizerExecStream(normalizerParams, normalizerEmbryo, nKeys);

    // build inputs -> minus stream -> normalizer transforms
    SharedExecStream pOutputStream = prepareConfluenceTransformGraph(
        minuendEmbryo, subtrahendEmbryo, minusEmbryo, normalizerEmbryo);
    RestartingMinusExecStreamGenerator
        verifier(nRows, repeatSeqValues, subtrahendInterval);
    verifyOutput(
        *pOutputStream,
        verifier.getRowCount(), 
        verifier);
}

void LbmMinusExecStreamTest::newMinusStream(
    LbmMinusExecStreamParams &params,
    ExecStreamEmbryo &embryo,
    TupleDescriptor const &outputDesc) 
{
    params.rowLimitParamId = DynamicParamId(1);
    params.startRidParamId = DynamicParamId(2);
    params.outputTupleDesc = outputDesc;
    params.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);

    embryo.init(new LbmMinusExecStream(), params);
    embryo.getStream()->setName("MinusExecStream");
}

FENNEL_UNIT_TEST_SUITE(LbmMinusExecStreamTest);


// End LbmMinusExecStreamTest.cpp

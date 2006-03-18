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
#include "fennel/lucidera/bitmap/LbmIntersectExecStream.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for Intersect exec stream
 */
class LbmIntersectExecStreamTest : public LbmExecStreamTestBase
{
    void testIntersect(
        uint nInputs, uint nRows, std::vector<InputData> const &inputData);

public:
    explicit LbmIntersectExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, test2Inputs);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, test3Inputs);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, testGaps);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, testLargeOutput);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, testLargeInputs);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, testSingleBitmaps);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, testEmptyResult);
        FENNEL_UNIT_TEST_CASE(LbmIntersectExecStreamTest, testZeros);
    }

    void test2Inputs();
    void test3Inputs();
    void testGaps();
    void testLargeOutput();
    void testLargeInputs();
    void testSingleBitmaps();
    void testEmptyResult();
    void testZeros();
};

void LbmIntersectExecStreamTest::test2Inputs()
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

    // expected result -- every other bit set, starting at 10
    input.bitmapSize = resultBitmapSize(10, nRows);
    input.startRid = LcsRid(10);
    input.skipRows = 2;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

void LbmIntersectExecStreamTest::test3Inputs()
{
    uint nInputs = 3;
    uint nRows = 1000;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every 2 rids set, starting at 22
    input.bitmapSize = 9;
    input.startRid = LcsRid(22);
    input.skipRows = 2;
    inputData.push_back(input);

    // bitmap input 2 -- every 3 rids set, starting at 30
    input.bitmapSize = 8;
    input.startRid = LcsRid(30);
    input.skipRows = 3;
    inputData.push_back(input);

    // bitmap input 3 -- every rid set, starting at 35
    input.bitmapSize = 10;
    input.startRid = LcsRid(35);
    input.skipRows = 1;
    inputData.push_back(input);

    // expected result -- every 6 rids set, starting at 36
    input.bitmapSize = resultBitmapSize(36, nRows);
    input.startRid = LcsRid(36);
    input.skipRows = 2*3;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

/**
 * This testcase tests the scenario where there are large gaps of zeros
 * in between each set bit.  Each of the skip values chosen for each input
 * is a prime number so the least common product is the product of the values
 * themselves.
 */
void LbmIntersectExecStreamTest::testGaps()
{
    uint nInputs = 3;
    // ensure we have a result with 8 bits set
    uint nRows = 3*11*19*8;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every 3 rids set, starting at 3
    input.bitmapSize = 11;
    input.startRid = LcsRid(3);
    input.skipRows = 3;
    inputData.push_back(input);

    // bitmap input 2 -- every 11 rids set, starting at 11
    input.bitmapSize = 12;
    input.startRid = LcsRid(11);
    input.skipRows = 11;
    inputData.push_back(input);

    // bitmap input 3 -- every 19 rids set, starting at 19
    input.bitmapSize = 13;
    input.startRid = LcsRid(19);
    input.skipRows = 19;
    inputData.push_back(input);

    // expected result -- every 3*11*19 rids set, starting at 3*11*19
    input.bitmapSize = resultBitmapSize(3*11*19, nRows);
    input.startRid = LcsRid(3*11*19);
    input.skipRows = 3*11*19;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

/**
 * This testcase exercises building up a large number segments in the
 * resultant bitmaps
 */
void LbmIntersectExecStreamTest::testLargeOutput()
{
    uint nInputs = 2;
    uint nRows = 100000;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- all bits set
    input.bitmapSize = 1;
    input.startRid = LcsRid(0);
    input.skipRows = 1;
    inputData.push_back(input);

    // bitmap input 2 -- every 16 bits set
    input.bitmapSize = 1;
    input.startRid = LcsRid(0);
    input.skipRows = 16;
    inputData.push_back(input);

    // expected result -- every 16 bits set
    input.bitmapSize = resultBitmapSize(0, nRows);
    input.startRid = LcsRid(0);
    input.skipRows = 16;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

/**
 * This testcase exercises bitmap inputs that have a large number of segments
 */
void LbmIntersectExecStreamTest::testLargeInputs()
{
    uint nInputs = 2;
    uint nRows = 15*8*24*5;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every 8 bits set
    input.bitmapSize = 15*8*3;
    input.startRid = LcsRid(0);
    input.skipRows = 8;
    inputData.push_back(input);

    // bitmap input 2 -- every 15 bits set
    input.bitmapSize = 15*8*3;
    input.startRid = LcsRid(0);
    input.skipRows = 15;
    inputData.push_back(input);

    // expected result -- every 8*15 bits set
    input.bitmapSize = resultBitmapSize(0, nRows);
    input.startRid = LcsRid(0);
    input.skipRows = 8*15;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

/**
 * This testcase exercises the case where the input contains single bitmaps
 */
void LbmIntersectExecStreamTest::testSingleBitmaps()
{
    uint nInputs = 2;
    uint nRows = 20*8*5;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- all even bits set
    input.bitmapSize = 20;
    input.startRid = LcsRid(0);
    input.skipRows = 2;
    inputData.push_back(input);

    // bitmap input 2 -- every 4 bits set
    input.bitmapSize = 20;
    input.startRid = LcsRid(0);
    input.skipRows = 4;
    inputData.push_back(input);

    // expected result -- every 4 bits set
    input.bitmapSize = resultBitmapSize(0, nRows);
    input.startRid = LcsRid(0);
    input.skipRows = 4;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

/**
 * Result should be empty
 */
void LbmIntersectExecStreamTest::testEmptyResult()
{
    uint nInputs = 2;
    uint nRows = 80;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- all even bits set
    input.bitmapSize = 10;
    input.startRid = LcsRid(0);
    input.skipRows = 2;
    inputData.push_back(input);

    // bitmap input 2 -- all odd bits set
    input.bitmapSize = 10;
    input.startRid = LcsRid(1);
    input.skipRows = 2;
    inputData.push_back(input);

    // expected result -- empty result
    input.bitmapSize = 0;
    input.startRid = LcsRid(0);
    input.skipRows = 1;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

/**
 * Testcase where AND'd segments will result in certain bytes containing
 * zeroes
 */
void LbmIntersectExecStreamTest::testZeros()
{
    uint nInputs = 2;
    uint nRows = 4000;
    std::vector<InputData> inputData;
    InputData input;

    // bitmap input 1 -- every 3 bits set, starting at 3
    input.bitmapSize = 9;
    input.startRid = LcsRid(3);
    input.skipRows = 3;
    inputData.push_back(input);

    // bitmap input 2 -- every 4 bits set, starting at 4
    input.bitmapSize = 9;
    input.startRid = LcsRid(4);
    input.skipRows = 4;
    inputData.push_back(input);

    // expected result -- every 12 bits set, starting at 12
    input.bitmapSize = resultBitmapSize(12, nRows);
    input.startRid = LcsRid(12);
    input.skipRows = 12;
    inputData.push_back(input);

    testIntersect(nInputs, nRows, inputData);
}

void LbmIntersectExecStreamTest::testIntersect(
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

    LbmIntersectExecStreamParams intersectParams;
    intersectParams.rowLimitParamId = DynamicParamId(1);
    intersectParams.startRidParamId = DynamicParamId(2);
    intersectParams.outputTupleDesc = bitmapTupleDesc;
    intersectParams.scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);

    ExecStreamEmbryo intersectEmbryo;
    intersectEmbryo.init(new LbmIntersectExecStream(), intersectParams);
    intersectEmbryo.getStream()->setName("IntersectExecStream");

    SharedExecStream pOutputStream = prepareConfluenceGraph(
        valuesStreamEmbryoList, intersectEmbryo);

    verifyBufferedOutput(
        *pOutputStream, bitmapTupleDesc, bmInputs[nInputs].nBitmaps,
        bmInputs[nInputs].bufArray.get());
}

FENNEL_UNIT_TEST_SUITE(LbmIntersectExecStreamTest);


// End LbmIntersectExecStreamTest.cpp

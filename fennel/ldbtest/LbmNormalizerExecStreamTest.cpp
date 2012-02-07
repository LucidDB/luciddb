/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/ldbtest/LbmExecStreamTestBase.h"
#include "fennel/lbm/LbmByteSegment.h"
#include "fennel/lbm/LbmNormalizerExecStream.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Test case for LbmNormalizerExecStream
 */
class LbmNormalizerExecStreamTest : public LbmExecStreamTestBase
{
protected:
    /**
     * Tests the normalizer against generated bitmap data. The simulated
     * stream consists of repeating columns. The columns produce from 0
     * to <i>value</i>-1 with values specified in an array.
     *
     * <pre>
     * column 0 = 0, 1, 2, ..., a-1, 1, 2, ..., a-1, ...
     * column 1 = 0, 1, 2, ..., b-1, 1, 2, ..., b-1, ...
     * column 2 = ...
     *</pre>
     *
     * The stream is converted into a key bitmap, [keys, RID, bitmap]
     * which is converted into a tuple stream [key projection] by the
     * normalizer.
     *
     * @param nRows number of rows in simulated stream
     * @param nKeys number of keys in the bitmap
     * @param repeatSeqValues specifies data pattern. Each value should be
     *   one or a distinct primary number.
     */
    void testNormalizer(
        uint nRows,
        uint nKeys,
        std::vector<int> const &repeatSeqValues);

public:
    explicit LbmNormalizerExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmNormalizerExecStreamTest, testBitsInByte);
        FENNEL_UNIT_TEST_CASE(LbmNormalizerExecStreamTest, testScanFullKey);
        FENNEL_UNIT_TEST_CASE(LbmNormalizerExecStreamTest, testScanPartKey);
        FENNEL_UNIT_TEST_CASE(LbmNormalizerExecStreamTest, testCloseRepeats);
    }

    void testBitsInByte();
    void testScanFullKey();
    void testScanPartKey();
    void testCloseRepeats();
};

/**
 * Returns the number of times a value repeats over nRows
 */
uint getValueCount(uint nRows, uint interval, uint value)
{
    uint nCopies = nRows / interval;
    if (value < nRows % interval) {
        nCopies++;
    }
    return nCopies;
}

class NormalizerExecStreamGenerator : public MockProducerExecStreamGenerator
{
protected:
    uint nKeys;
    std::vector<int> repeatSeqValues;
    uint interval;
    boost::shared_array<uint> changeIndexes;
    uint current;
    uint lastRow;

public:
    NormalizerExecStreamGenerator(
        uint nRows, uint nKeys, std::vector<int> repeatSeqValues)
    {
        this->nKeys = nKeys;
        this->repeatSeqValues = repeatSeqValues;
        interval = LbmExecStreamTestBase::getTupleInterval(repeatSeqValues);
        changeIndexes.reset(new uint[interval]);
        changeIndexes[0] = getValueCount(nRows, interval, 0);
        for (uint i = 1; i < interval; i++) {
            changeIndexes[i] =
                changeIndexes[i - 1] + getValueCount(nRows, interval, i);
        }
        current = 0;
        lastRow = 0;
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        // must be generated in order
        assert(lastRow <= iRow);
        lastRow = iRow;

        if (iRow >= changeIndexes[current]) {
            current++;
            assert(current < interval);
        }
        assert(iCol < nKeys);
        return current % repeatSeqValues[iCol];
    }
};

void LbmNormalizerExecStreamTest::testBitsInByte()
{
    LbmByteSegment::verifyBitsInByte();
}

void LbmNormalizerExecStreamTest::testScanFullKey()
{
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(1);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);

    testNormalizer(1000, 3, repeatSeqValues);
}

void LbmNormalizerExecStreamTest::testScanPartKey()
{
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(1);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);

    testNormalizer(1000, 2, repeatSeqValues);
}

void LbmNormalizerExecStreamTest::testCloseRepeats()
{
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(1);
    repeatSeqValues.push_back(2);
    repeatSeqValues.push_back(3);

    testNormalizer(1000, 3, repeatSeqValues);
}

void LbmNormalizerExecStreamTest::testNormalizer(
    uint nRows,
    uint nKeys,
    std::vector<int> const &repeatSeqValues)
{
    initKeyBitmap(nRows, repeatSeqValues);

    // test normalizer against the input
    ValuesExecStreamParams valuesParams;
    valuesParams.outputTupleDesc = keyBitmapTupleDesc;
    valuesParams.pTupleBuffer = keyBitmapBuf;
    valuesParams.bufSize = keyBitmapBufSize;

    ExecStreamEmbryo valuesStreamEmbryo;
    valuesStreamEmbryo.init(new ValuesExecStream(), valuesParams);
    valuesStreamEmbryo.getStream()->setName("ValuesExecStream");

    ExecStreamEmbryo normalizerEmbryo;
    LbmNormalizerExecStreamParams normalizerParams;
    initNormalizerExecStream(normalizerParams, normalizerEmbryo, nKeys);

    SharedExecStream pOutputStream = prepareTransformGraph(
        valuesStreamEmbryo, normalizerEmbryo);
    NormalizerExecStreamGenerator verifier(nRows, nKeys, repeatSeqValues);
    verifyOutput(*pOutputStream, nRows, verifier);
}

FENNEL_UNIT_TEST_SUITE(LbmNormalizerExecStreamTest);

// End LbmNormalizerExecStreamTest.cpp

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
#include "fennel/lucidera/test/LbmExecStreamTestBase.h"
#include "fennel/lucidera/bitmap/LbmByteSegment.h"
#include "fennel/lucidera/bitmap/LbmSortedAggExecStream.h"
#include <stdarg.h>

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Test case for LbmSortedAggExecStream
 */
class LbmSortedAggExecStreamTest : public LbmExecStreamTestBase
{
protected:
    /**
     * Tests the sorted agg against generated bitmap data. The simulated
     * stream consists of repeating columns. The columns produce from 0
     * to <i>value</i>-1 with values specified in an array.
     *
     * <pre>
     * column 0 = 0, 1, 2, ..., a-1, 1, 2, ..., a-1, ...
     * column 1 = 0, 1, 2, ..., b-1, 1, 2, ..., b-1, ...
     * column 2 = ...
     * </pre>
     *
     * The stream is converted into a key bitmap, [keys, RID, bitmap]
     * which is converted into a tuple stream [group keys, aggs] by the
     * agg stream.
     *
     * @param nRows number of rows in simulated stream
     * @param nKeys number of keys in bitmap
     * @param repeatSeqValues specifies data pattern. Each value should be
     *   one or a distinct primary number.
     */
    void testSortedAgg(
        uint nRows,
        uint nKeys,
        std::vector<int> const &repeatSeqValues);

public:
    explicit LbmSortedAggExecStreamTest()
    {
        FENNEL_UNIT_TEST_CASE(LbmSortedAggExecStreamTest, testScanFullKey);
        FENNEL_UNIT_TEST_CASE(LbmSortedAggExecStreamTest, testScanPartKey);
    }

    void testScanFullKey();
    void testScanPartKey();
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

/**
 * Generates sorted (key, value count) tuples like:
 * <pre>
 * 0, 0, 0, 23 [row 0]
 * 0, 0, 1, 22 [row 1]
 * ...
 * </pre>
 */
class SortedAggExecStreamGenerator : public MockProducerExecStreamGenerator
{
protected:
    uint nRows;
    uint nKeys;
    std::vector<int> keyRepeats;
    uint interval;
    boost::shared_array<uint> sortedToUnsortedMap;
    int current;
    boost::shared_array<uint> currentRow;

public:
    SortedAggExecStreamGenerator(
        uint nRows, uint nKeys, std::vector<int> repeatSeqValues)
    {
        this->nRows = nRows;
        this->nKeys = nKeys;
        for (uint i = 0; i < nKeys; i++) {
            keyRepeats.push_back(repeatSeqValues[i]);
        }
        interval = LbmExecStreamTestBase::getTupleInterval(keyRepeats);

        sortedToUnsortedMap.reset(new uint[interval]);
        for (uint i = 0; i < interval; i++) {
            uint value = 0;
            uint scale = 1;
            // calculate sorted position (backwards)
            // value = key0 * scale_1_to_n + key1 * scale_2_to_n + ...
            for (int j = nKeys - 1; j >= 0; j--) {
                uint key = i % keyRepeats[j];
                value += key * scale;
                scale *= keyRepeats[j];
            }
            sortedToUnsortedMap[value] = i;
        }
        current = -1;
        currentRow.reset(new uint[nKeys + 1]);
    }

    virtual int64_t generateValue(uint iRow, uint iCol)
    {
        assert (iRow < interval);
        assert (iCol < nKeys + 1);

        if (iRow != current) {
            current = iRow;
            uint unsorted = sortedToUnsortedMap[current];
            for (uint i = 0; i < nKeys; i++) {
                currentRow[i] = unsorted % keyRepeats[i];
            }
            currentRow[nKeys] = getValueCount(nRows, interval, unsorted);
        }
        return currentRow[iCol];
    }
};

void LbmSortedAggExecStreamTest::testScanFullKey()
{
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(1);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);

    testSortedAgg(1000, 3, repeatSeqValues);
}

void LbmSortedAggExecStreamTest::testScanPartKey()
{
    std::vector<int> repeatSeqValues;
    repeatSeqValues.push_back(13);
    repeatSeqValues.push_back(5);
    repeatSeqValues.push_back(9);

    testSortedAgg(1000, 2, repeatSeqValues);
}

void LbmSortedAggExecStreamTest::testSortedAgg(
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

    // build -> sorter -> agg transforms
    std::vector<ExecStreamEmbryo> transformEmbryos;

    ExternalSortExecStreamParams sortParams;
    ExecStreamEmbryo sortEmbryo;
    initSorterExecStream(sortParams, sortEmbryo, keyBitmapTupleDesc, nKeys);
    transformEmbryos.push_back(sortEmbryo);

    ExecStreamEmbryo aggEmbryo;
    LbmSortedAggExecStreamParams aggParams;
    aggParams.groupByKeyCount = nKeys;
    AggInvocation countStar;
    countStar.aggFunction = AGG_FUNC_COUNT;
    countStar.iInputAttr = -1;
    aggParams.aggInvocations.push_back(countStar);

    TupleProjection keyProj;
    for (int i = 0; i < nKeys; i++) {
        keyProj.push_back(i);
    }
    TupleDescriptor aggDesc;
    aggDesc.projectFrom(keyBitmapTupleDesc, keyProj);
    aggDesc.push_back(attrDesc_int64);
    aggParams.outputTupleDesc = aggDesc;

    aggEmbryo.init(new LbmSortedAggExecStream(), aggParams);
    aggEmbryo.getStream()->setName("SortedAgg");
    transformEmbryos.push_back(aggEmbryo);

    SharedExecStream pOutputStream = prepareTransformGraph(
        valuesStreamEmbryo, transformEmbryos);
    SortedAggExecStreamGenerator verifier(nRows, nKeys, repeatSeqValues);
    verifyOutput(
        *pOutputStream,
        getTupleInterval(repeatSeqValues, nKeys),
        verifier);
}

FENNEL_UNIT_TEST_SUITE(LbmSortedAggExecStreamTest);

// End LbmSortedAggExecStreamTest.cpp

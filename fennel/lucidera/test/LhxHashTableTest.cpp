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
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/lucidera/hashexe/LhxHashTableDump.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/cache/Cache.h"

#include <boost/scoped_array.hpp>
#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Testcase for inserting into hash table.
 */
class LhxHashTableTest : virtual public SegStorageTestBase
{
    StandardTypeDescriptorFactory stdTypeFactory;
    
    void testInsert(
        uint numRows,
        uint maxBlockCount,
        uint partitionLevel,
        vector<uint> &repeatSeqValues,
        uint numKeyCols,
        uint numAggs,
        uint numDataCols,
        bool dumpHashTable,
        string testName);

public:
    explicit LhxHashTableTest()
    {
        FENNEL_UNIT_TEST_CASE(LhxHashTableTest, testInsert1Ka);
        FENNEL_UNIT_TEST_CASE(LhxHashTableTest, testInsert1Kb);
    }

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();

    void testInsert1Ka();
    void testInsert1Kb();
};

void LhxHashTableTest::testCaseSetUp()
{
    openStorage(DeviceMode::createNew);
}

void LhxHashTableTest::testCaseTearDown()
{
    SegStorageTestBase::testCaseTearDown();
}

void LhxHashTableTest::testInsert(
    uint numRows,
    uint maxBlockCount,
    uint partitionLevel,
    vector<uint> &repeatSeqValues,
    uint numKeyCols,
    uint numAggs,
    uint numDataCols,
    bool dumpHashTable,
    string testName)
{
    LhxHashTable hashTable;

    SegmentAccessor scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 100);

    TupleAttributeDescriptor attrDesc_int32 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    TupleData inputTuple;
    TupleDescriptor inputTupleDesc;
    TupleProjection keyColsProj;
    TupleProjection aggsProj;
    TupleProjection dataProj;

    uint i, j;

    /*
     * Setup hash table.
     */

    uint numCols = numKeyCols + numAggs + numDataCols;

    boost::scoped_array<uint> colValues(new uint[numCols]);

    for (i = 0; i < numCols; i ++) {
        inputTupleDesc.push_back(attrDesc_int32);

        if ( i < numKeyCols) {
            keyColsProj.push_back(i);
        } else if (i < numKeyCols + numAggs) {
            aggsProj.push_back(i);
        } else {
            dataProj.push_back(i);
        }
    }

    hashTable.init(
        scratchAccessor, maxBlockCount, partitionLevel,
        inputTupleDesc, keyColsProj, aggsProj, dataProj);

    /*
     * Calculate key cardinality, assuming there's no correlation between key
     * cols.
     */
    uint cndKeys = 1;    
    for (i = 0; i < numKeyCols; i ++) {
        cndKeys *= repeatSeqValues[i];
    }
    uint numSlots = hashTable.slotsNeeded(cndKeys);

    bool status = hashTable.allocateResources(numSlots);

    assert(status);

    inputTuple.compute(inputTupleDesc);

    /*
     * Insert some tuples.
     */
    for (i = 0; i < numRows; i ++) {
        
        for (j = 0; j < numCols; j++) {
            colValues[j] = i % repeatSeqValues[j];
            inputTuple[j].pData = (PBuffer)&(colValues[j]);
        }
        
        status =
            hashTable.addTuple(inputTuple, keyColsProj, aggsProj, dataProj);

        assert(status);
    }
        
    /*
     * verify that the hash table reader can see all the tuples.
     */
    LhxHashTableReader hashTableReader;
    hashTableReader.init(&hashTable,
        inputTupleDesc, keyColsProj, aggsProj, dataProj);
    TupleData outputTuple;
        
    outputTuple.compute(inputTupleDesc);
        
    TuplePrinter tuplePrinter;
    ostringstream dataTrace;
    dataTrace << "All Tuples:\n";
    uint numTuples = 0;

    while (hashTableReader.getNext(outputTuple)) {
        tuplePrinter.print(dataTrace, inputTupleDesc, outputTuple);
        dataTrace << "\n";
        numTuples ++;
    }
    assert (numTuples == numRows);
    
    if (dumpHashTable) {
        LhxHashTableDump hashTableDump(
            TRACE_INFO,
            shared_from_this(), 
            "LhxHashTableTest");
        hashTableDump.dump(hashTable);        
        hashTableDump.dump(dataTrace.str());
    }

    /*
     * Verify that the keys are inserted.
     */
    for (i = 0; i < numRows; i ++) {
        
        for (j = 0; j < numCols; j++) {
            colValues[j] = i % repeatSeqValues[j];
            inputTuple[j].pData = (PBuffer)&(colValues[j]);
        }
        
        PBuffer matchingKey =
            hashTable.findKey(inputTuple, keyColsProj);

        assert(matchingKey != NULL);
    }

    hashTable.releaseResources();
    colValues.reset();
}

void LhxHashTableTest::testInsert1Ka()
{
    uint numRows = 100;
    uint maxBlockCount = 10;
    uint partitionLevel = 0;
    vector<uint> values;
    uint numKeyCols = 1;
    uint numAggs = 0;
    uint numDataCols = 0;
    bool dump = true;
    string testName = "testInsert1K";
    uint i;

    for (i = 0; i < numKeyCols; i ++) {
        /*
         * At least one value, hence + 1.
         */
        values.push_back(i+10);
    }

    for (i = 0; i < numAggs; i ++) {
        values.push_back(10);
    }

    for (i = 0; i < numDataCols; i ++) {
        values.push_back(i+1);
    }

    testInsert(
        numRows, maxBlockCount, partitionLevel,
        values, numKeyCols, numAggs, numDataCols,
        dump, testName);
}

void LhxHashTableTest::testInsert1Kb()
{
    uint numRows = 1000;
    uint maxBlockCount = 10;
    uint partitionLevel = 0;
    vector<uint> values;
    uint numKeyCols = 2;
    uint numAggs = 1;
    uint numDataCols = 4;
    bool dump = true;
    string testName = "testInsert1K";
    uint i;

    for (i = 0; i < numKeyCols; i ++) {
        /*
         * At least one value, hence + 1.
         */
        values.push_back(i+10);
    }

    for (i = 0; i < numAggs; i ++) {
        values.push_back(10);
    }

    for (i = 0; i < numDataCols; i ++) {
        values.push_back(i+1);
    }

    testInsert(
        numRows, maxBlockCount, partitionLevel,
        values, numKeyCols, numAggs, numDataCols,
        dump, testName);
}

FENNEL_UNIT_TEST_SUITE(LhxHashTableTest);


// End LhxHashTableTest.cpp

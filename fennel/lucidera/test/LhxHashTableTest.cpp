/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
#include "fennel/lucidera/hashexe/LhxPartition.h"
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
    LhxHashInfo hashInfo;
    uint buildInputIndex;

    uint writeHashTable(LhxHashInfo const &hashInfo, LhxHashTable &hashTable, 
        SharedLhxPartition destPartition);

    uint readPartition(
        LhxHashInfo &hashInfo,
        SharedLhxPartition srcPartition,
        ostringstream &dataTrace);

    void testInsert(
        uint numRows,
        uint maxBlockCount,
        uint partitionLevel,
        vector<uint> &repeatSeqValues,
        uint numKeyCols,
        uint numAggs,
        uint numDataCols,
        bool dumpHashTable,
        bool writeToPartition,
        uint recursivePartitioning,
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
    openRandomSegment();
    hashInfo.externalSegmentAccessor.pSegment = pRandomSegment;
    hashInfo.externalSegmentAccessor.pCacheAccessor = pCache;
    hashInfo.memSegmentAccessor = 
        pSegmentFactory->newScratchSegment(pCache, 100);
}

void LhxHashTableTest::testCaseTearDown()
{
    hashInfo.inputDesc.clear();
    hashInfo.keyProj.clear();
    hashInfo.isKeyColVarChar.clear();

    hashInfo.aggsProj.clear();
    hashInfo.dataProj.clear();

    hashInfo.memSegmentAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);
    hashInfo.externalSegmentAccessor.reset();
    hashInfo.memSegmentAccessor.reset(); 
    SegStorageTestBase::testCaseTearDown();
}

uint LhxHashTableTest::writeHashTable(LhxHashInfo const &hashInfo,
    LhxHashTable &hashTable, SharedLhxPartition destPartition)
{
    uint tuplesWritten = 0;
    LhxPartitionWriter writer;

    LhxHashTableReader hashTableReader;
    hashTableReader.init(&hashTable, hashInfo, buildInputIndex);
    hashTableReader.bindKey(NULL);
    TupleData outputTuple;
        
    outputTuple.compute(hashInfo.inputDesc[destPartition->inputIndex]);

    //write to a paritition
    writer.open(destPartition, (LhxHashInfo const &)hashInfo);
    while (hashTableReader.getNext(outputTuple)) {
        writer.marshalTuple(outputTuple);
        tuplesWritten ++;
    }
    writer.close();

    return tuplesWritten;
}

uint LhxHashTableTest::readPartition(LhxHashInfo &hashInfo,
    SharedLhxPartition srcPartition, ostringstream &dataTrace)
{
    LhxPartitionReader reader;
    uint tuplesRead = 0;
    TupleData outputTuple;
    TuplePrinter tuplePrinter;
    TupleDescriptor &inputTupleDesc = hashInfo.inputDesc[1];
        
    outputTuple.compute(hashInfo.inputDesc[srcPartition->inputIndex]);

    reader.open(srcPartition, (LhxHashInfo const &)hashInfo);

    for (;;) {
        if (!reader.isTupleConsumptionPending()) {
            if (reader.getState() == EXECBUF_EOS) {
                break;
            }
            if (!reader.demandData()) {
                break;
            }
            reader.unmarshalTuple(outputTuple);

            tuplePrinter.print(dataTrace, inputTupleDesc, outputTuple);
            dataTrace << "\n";

            tuplesRead ++;
        }
        
        reader.consumeTuple();
    }
    reader.close();
    return tuplesRead;
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
    bool writeToPartition,
    uint recursivePartitioning,
    string testName)
{
    LhxHashTable hashTable;

    hashInfo.numCachePages = maxBlockCount; 

    TupleAttributeDescriptor attrDesc_int32 =
        TupleAttributeDescriptor(
            stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    TupleData inputTuple;

    uint i, j;

    /*
     * Setup hash table.
     */
    uint numCols = numKeyCols + numAggs + numDataCols;

    boost::scoped_array<uint> colValues(new uint[numCols]);

    TupleDescriptor inputDesc;
    TupleProjection keyProj;
    TupleProjection dataProj;
    std::vector<bool> isKeyVarChar;

    for (i = 0; i < numCols; i ++) {
        inputDesc.push_back(attrDesc_int32);

        if ( i < numKeyCols) {
            keyProj.push_back(i);
            isKeyVarChar.push_back(false);
        } else if (i < numKeyCols + numAggs) {
            hashInfo.aggsProj.push_back(i);
        } else {
            dataProj.push_back(i);
        }
    }

    /*
     * Calculate key cardinality, assuming there's no correlation between key
     * cols.
     */
    uint cndKeys = 1;    
    for (i = 0; i < numKeyCols; i ++) {
        cndKeys *= repeatSeqValues[i];
    }

    uint numInputs = 2;
    buildInputIndex = numInputs - 1;
    for (i = 0; i < numInputs; i ++) {
        hashInfo.inputDesc.push_back(inputDesc);
        hashInfo.keyProj.push_back(keyProj);
        hashInfo.isKeyColVarChar.push_back(isKeyVarChar);
        hashInfo.dataProj.push_back(dataProj);
        hashInfo.useJoinFilter.push_back(false);
        hashInfo.filterNull.push_back(false);
        // empty projection : do not filter nulls
        TupleProjection filterNullKeyProj;
        hashInfo.filterNullKeyProj.push_back(filterNullKeyProj);
        hashInfo.removeDuplicate.push_back(false);
        hashInfo.numRows.push_back(numRows);
        hashInfo.cndKeys.push_back(cndKeys);
    }

    TupleDescriptor &inputTupleDesc = hashInfo.inputDesc.back();
    TupleProjection &keyColsProj = hashInfo.keyProj.back();

    hashTable.init(partitionLevel, hashInfo, buildInputIndex);

    uint usablePageSize =
        (hashInfo.memSegmentAccessor.pSegment)->getUsablePageSize();

    hashTable.calculateNumSlots(cndKeys, usablePageSize, hashInfo.numCachePages);

    bool status = hashTable.allocateResources();

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
            hashTable.addTuple(inputTuple);
        
        assert(status);
    }
        
    LhxHashTableReader hashTableReader;
    hashTableReader.init(&hashTable, hashInfo, buildInputIndex);
    TupleData outputTuple;
    
    outputTuple.compute(inputTupleDesc);
    
    TuplePrinter tuplePrinter;
    ostringstream dataTrace;
    dataTrace << "All Inserted Tuples:\n";
    uint numTuples = 0;
    
    /*
     * verify that the hash table reader can see all the tuples.
     */
    while (hashTableReader.getNext(outputTuple)) {
        tuplePrinter.print(dataTrace, inputTupleDesc, outputTuple);
        dataTrace << "\n";
        numTuples ++;
    }
    assert (numTuples == numRows);
    
    /*
     * Verify that the keys are inserted.
     */
    dataTrace << "All Matched Tuples:\n";
    numTuples = 0;
    /*
     * verify that the hash table reader can see all the unmatched tuples.
     * The above search is done in probing mode, so matched keys are
     * marked as such. The whole table should be returned when reading
     * matched rows.
     */
    for (i = 0; i < numRows; i ++) {
        
        for (j = 0; j < numCols; j++) {
            colValues[j] = i % repeatSeqValues[j];
            inputTuple[j].pData = (PBuffer)&(colValues[j]);
        }
        
        PBuffer matchingKey =
            hashTable.findKey(inputTuple, keyColsProj, true);

        if (matchingKey) {
            hashTableReader.bindKey(matchingKey);
            while (hashTableReader.getNext(outputTuple)) {
                tuplePrinter.print(dataTrace, inputTupleDesc, outputTuple);
                dataTrace << "\n";
                numTuples ++;
            }
        }
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

    if (writeToPartition) {
        SharedLhxPartition partition = SharedLhxPartition(new LhxPartition());
        partition->inputIndex = 1;

        //write to a paritition
        uint tuplesWritten =
            writeHashTable((LhxHashInfo const &)hashInfo, 
                hashTable, partition);
        
        //read from the same paritition
        ostringstream dataTrace;

        dataTrace << "[Tuples read from partitions-1]\n";

        uint tuplesRead =
            readPartition(hashInfo, partition, dataTrace);

        if (dumpHashTable) {
            LhxHashTableDump hashTableDump(
                TRACE_INFO,
                shared_from_this(), 
                "LhxHashTableTest");
            hashTableDump.dump(dataTrace.str());
        }
        // verify read/write row count match.
        assert (tuplesWritten == numRows && tuplesRead == tuplesWritten);
    }

    if (recursivePartitioning > 0) {
        // using the same data set(the one from the hash table( for both inputs
        // partition all leaf nodes till the tree reaches "recrusivePartitioning",
        // partition level starts from level 0 which is a single partition.
        // For each level, read all the data from the leaf partitions and make sure the
        // rwo count remain the same as the rows initially written into the
        // single partition.

        // first set up the plan at level 0 which conprises of a single
        // partition, one from each side.
        std::vector<SharedLhxPartition> partitions;
        uint tuplesWritten[2];

        // for both input sides.
        for (int j = 0; j < 2; j ++) {
            partitions.push_back(SharedLhxPartition(new LhxPartition()));
            partitions[j]->inputIndex = 1;
            tuplesWritten[j] =
                writeHashTable((LhxHashInfo const &)hashInfo,
                    hashTable, partitions[j]);
        }

        assert (tuplesWritten[0] == numRows &&
                tuplesWritten[0] == tuplesWritten[1]);
        
        uint tuplesRead[2];
        SharedLhxPlan plan = SharedLhxPlan(new LhxPlan());

        plan->init(WeakLhxPlan(), 0, partitions, false);
        
        LhxPlan *leafPlan;
        uint numLeafPlanCreated = 1;
        uint numLeafPlanRead = 0;

        for (int i = 0; i < recursivePartitioning; i ++) {
            numLeafPlanCreated *= LhxPlan::LhxChildPartCount;
            numLeafPlanRead = 0;
            leafPlan = plan->getFirstLeaf();

            //create leaf plans for the next level
            while (leafPlan) {
                leafPlan->createChildren(hashInfo, false);
                //skip the next numChildPart leaves as they are newly
                //created children
                leafPlan = leafPlan->getFirstLeaf();
                for (int k = 0; k < LhxPlan::LhxChildPartCount; k ++) {
                    leafPlan = leafPlan->getNextLeaf();
                }
            }
        }

        // each partition can be read exactly once(since SegStreamAllocation
        // deletes the disk content)
        // so only verify the leaf level partitions.

        tuplesRead[0] = 0;
        tuplesRead[1] = 0;
            
        // get the first leaf 
        leafPlan = plan->getFirstLeaf();
        
        while (leafPlan) {
            numLeafPlanRead ++;
            for (int j = 0; j < 2; j ++) {
                ostringstream dataTrace;
                dataTrace << "[Tuples read from partitions-2]"
                          << "recursion depth" << recursivePartitioning
                          << "inputindex " << j << "\n";
                tuplesRead[j] +=
                    readPartition(hashInfo,
                            leafPlan->getPartition(j), dataTrace);
                if (dumpHashTable) {
                    LhxHashTableDump hashTableDump(
                        TRACE_INFO,
                        shared_from_this(), 
                        "LhxHashTableTest");
                    hashTableDump.dump(dataTrace.str());
                }
            }
            leafPlan = leafPlan->getNextLeaf();
        }
        
        assert (numLeafPlanRead == numLeafPlanCreated);
        assert ((tuplesRead[0] == tuplesRead[1]) &&
            (tuplesRead[0] == numRows));
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
    bool dumpHashTable = true;
    bool writeToPartition = true;
    uint recursivePartitioning = 3;
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
        dumpHashTable, writeToPartition, recursivePartitioning,
        testName);
}

void LhxHashTableTest::testInsert1Kb()
{
    uint numRows = 1000;
    uint maxBlockCount = 10;
    uint partitionLevel = 0;
    vector<uint> values;
    uint numKeyCols = 2;
    uint numAggs = 0;
    uint numDataCols = 4;
    bool dumpHashTable = true;
    bool writeToPartition = true;
    uint recursivePartitioning = 3;
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
        dumpHashTable, writeToPartition, recursivePartitioning,
        testName);
}

FENNEL_UNIT_TEST_SUITE(LhxHashTableTest);


// End LhxHashTableTest.cpp

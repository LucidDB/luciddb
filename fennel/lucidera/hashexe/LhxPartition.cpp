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
#include "fennel/lucidera/hashexe/LhxPartition.h"
#include "fennel/lucidera/hashexe/LhxHashGenerator.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxPartitionWriter::open(
    SharedLhxPartition destPartitionInit,
    LhxHashInfo const &hashInfo)
{
    destPartition = destPartitionInit;
    tupleAccessor.compute(hashInfo.inputDesc[destPartition->inputIndex]);
    pSegOutputStream = SegOutputStream::newSegOutputStream(
        hashInfo.externalSegmentAccessor);
    isAggregate = false;
}

void LhxPartitionWriter::open(
    SharedLhxPartition destPartitionInit,
    LhxHashInfo &hashInfo,
    AggComputerList *aggList,
    uint numWriterCachePages)
{
    destPartition = destPartitionInit;
    tupleAccessor.compute(hashInfo.inputDesc[destPartition->inputIndex]);
    pSegOutputStream = SegOutputStream::newSegOutputStream(
        hashInfo.externalSegmentAccessor);

    isAggregate = true;
    /*
     * Any partition level is fine since the data is hash partitioned already.
     */
    uint partitionLevel = 0;
    uint savedNumCachePages = hashInfo.numCachePages;
    hashInfo.numCachePages = numWriterCachePages;

    hashTable.init(partitionLevel, hashInfo, aggList);
    hashTableReader.init(&hashTable, hashInfo);

    hashInfo.numCachePages = savedNumCachePages;
    
    uint cndKeys = hashInfo.cndKeys;
    uint usablePageSize =
        (hashInfo.memSegmentAccessor.pSegment)->getUsablePageSize();

    hashTable.calculateNumSlots(cndKeys, usablePageSize, numWriterCachePages);

    partialAggTuple.compute(hashInfo.inputDesc[destPartition->inputIndex]);    
}

void LhxPartitionWriter::close()
{
    if (isAggregate) {
        /*
         * Write out the remaining partial aggregates in the local hash table.
         */
        while (hashTableReader.getNext(partialAggTuple)) {
            uint tupleStorageLength =
                tupleAccessor.getByteCount(partialAggTuple);
            PBuffer pDestBuf =
                pSegOutputStream->getWritePointer(tupleStorageLength);
            tupleAccessor.marshal(partialAggTuple, pDestBuf);
            pSegOutputStream->consumeWritePointer(tupleStorageLength);    
        }
    }
    destPartition->firstPageId = pSegOutputStream->getFirstPageId();
    pSegOutputStream.reset();
}

void LhxPartitionWriter::marshalTuple(TupleData const &inputTuple)
{
    uint tupleStorageLength = tupleAccessor.getByteCount(inputTuple);
    PBuffer pDestBuf = pSegOutputStream->getWritePointer(tupleStorageLength);
    tupleAccessor.marshal(inputTuple, pDestBuf);
    pSegOutputStream->consumeWritePointer(tupleStorageLength);    
}

void LhxPartitionWriter::aggAndMarshalTuple(TupleData const &inputTuple)
{
    
    while (!hashTable.addTuple(inputTuple)) {
        /*
         * Write everything out to partition.
         */
        while (hashTableReader.getNext(partialAggTuple)) {
            uint tupleStorageLength =
                tupleAccessor.getByteCount(partialAggTuple);
            PBuffer pDestBuf =
                pSegOutputStream->getWritePointer(tupleStorageLength);
            tupleAccessor.marshal(partialAggTuple, pDestBuf);
            pSegOutputStream->consumeWritePointer(tupleStorageLength);    
        }
        bool reuse = true;
        /*
         * hash table size remain unchanged
         */
        bool status = hashTable.allocateResources(reuse);
        assert(status);
        /*
         * Reset the reader and bind it to no particular key(will read the
         * whole hash table).
         */
        hashTableReader.bindKey(NULL);
    }
}

void LhxPartitionReader::open(
    SharedLhxPartition srcPartitionInit,
    LhxHashInfo const &hashInfo,
    bool setDeallocate)
{
    bufState = EXECBUF_NONEMPTY;
    srcPartition = srcPartitionInit;
    
    if (srcPartition->firstPageId == NULL_PAGE_ID) {
        srcIsStream = true;
    } else {
        srcIsStream = false;
    }

    if (srcIsStream) {
        streamBufAccessor = hashInfo.streamBufAccessor[srcPartition->inputIndex];
        outputTupleDesc = streamBufAccessor->getTupleDesc();
    } else {
        outputTupleDesc = hashInfo.inputDesc[srcPartition->inputIndex];
        tupleAccessor.compute(outputTupleDesc);
        pSegInputStream = SegInputStream::newSegInputStream(
            hashInfo.externalSegmentAccessor, srcPartition->firstPageId);
        pSegInputStream->startPrefetch();
        /*
         * Once read the disk page can be deallocated.
         */
        pSegInputStream->setDeallocate(setDeallocate);
        tupleAccessor.resetCurrentTupleBuf();
    }
}

void LhxPartitionReader::close() 
{
    if (srcIsStream) {
        /*
         * Do nothing if reading from stream.
         */
    } else {
        pSegInputStream.reset();
    }
}

void LhxPartitionReader::unmarshalTuple(TupleData &outputTuple)
{
    if (srcIsStream) {
        /*
         * Read from stream.
         */
        streamBufAccessor->unmarshalTuple(outputTuple);
    } else {
        tupleAccessor.unmarshal(outputTuple);
    }
}

void LhxPartitionReader::consumeTuple()
{
    if (srcIsStream) {
        streamBufAccessor->consumeTuple();
    } else {
        tupleAccessor.resetCurrentTupleBuf();
        pSegInputStream->consumeReadPointer(tupleStorageLength);        
    }
}

bool LhxPartitionReader::isTupleConsumptionPending()
{
    if (srcIsStream) {
        return streamBufAccessor->isTupleConsumptionPending();
    } else {
        if (tupleAccessor.getCurrentTupleBuf()) {
            return true;
        } else {
            return false;
        }
    }
}

bool LhxPartitionReader::demandData()
{
    if (srcIsStream) {
        return streamBufAccessor->demandData();
    } else {
        /*
         * Read from disk.
         */
        uint bytesReadable = 0;
        PConstBuffer pSrcBuf = pSegInputStream->getReadPointer(1, &bytesReadable);
        
        /*
         * If readable data does not fill a tuple, it means the segment stream
         * has reached EOD.
         */
        if (!pSrcBuf) {
           bufState = EXECBUF_EOS;
           return false;
        } else {
            tupleStorageLength = tupleAccessor.getBufferByteCount(pSrcBuf);
            if (bytesReadable < tupleStorageLength) {
                /*
                 * REVIEW: when will this happen
                 */
                bufState = EXECBUF_EOS;
                return false;
            } else {
               tupleAccessor.setCurrentTupleBuf(pSrcBuf);
               return true;
            }
        }
    }
}

void LhxPartitionInfo::init(
    uint numInputInit,
    uint numChildPartInit,
    LhxHashInfo *hashInfoInit)
{
    numChildPart = numChildPartInit;
    numInput     = numInputInit;

    writerList.clear();
    for (uint i = 0; i < numChildPart; i ++) {
        writerList.push_back(SharedLhxPartitionWriter(new LhxPartitionWriter()));
    }
    
    hashInfo = hashInfoInit;
}

void LhxPartitionInfo::open(
    uint curInputIndexInit,
    SharedLhxPartition partition)
{
    curInputIndex = curInputIndexInit;

    reader = &tmpReader;
    reader->open(partition, *hashInfo, true);

    for (uint i = 0; i < numChildPart; i ++) {
        uint index = i + curInputIndex * numChildPart;
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[index]->inputIndex = curInputIndex;
        writerList[i]->open(destPartitionList[index], *hashInfo);
    }
    /*
     * Tuples will come from a partition(stream or disk).
     */
    partitionMemory = false;
}

void LhxPartitionInfo::open(
    uint curInputIndexInit,
    LhxHashTableReader *hashTableReaderInit,
    LhxPartitionReader *buildReader,
    TupleData &buildTupleInit)
{
    curInputIndex = curInputIndexInit;

    hashTableReader = hashTableReaderInit;
    /*
     * Reset the reader and bind it to no particular key(will read the
     * whole hash table).
     */
    hashTableReader->bindKey(NULL);    

    /*
     * The build reader is from the LhxJoinExecStream and is already open.
     */
    reader = buildReader;

    /*
     * The inflight(between disk partition and hash table) build tuple.
     */
    buildTuple = buildTupleInit;

    for (uint i = 0; i < numChildPart; i ++) {
        uint index = i + curInputIndex * numChildPart;
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[index]->inputIndex = curInputIndex;
        writerList[i]->open(destPartitionList[index], *hashInfo);
    }
    /*
     * Tuples will come from memory(hash table) first.
     */
    partitionMemory = true;
}

void LhxPartitionInfo::open(
    uint curInputIndexInit,
    LhxHashTableReader *hashTableReaderInit,
    LhxPartitionReader *buildReader,
    TupleData &buildTupleInit,
    AggComputerList *aggList)
{
    curInputIndex = curInputIndexInit;

    hashTableReader = hashTableReaderInit;
    /*
     * Reset the reader and bind it to no particular key(will read the
     * whole hash table).
     */
    hashTableReader->bindKey(NULL);    

    /*
     * The build reader is from the LhxJoinExecStream and is already open.
     */
    reader = buildReader;

    /*
     * The inflight(between disk partition and hash table) build tuple.
     */
    buildTuple = buildTupleInit;

    /*
     * The hash table contained in the writer should only use up to the child's
     * share of scratch buffer.
     */
    uint numWriterCachePages = hashInfo->numCachePages / numChildPart;

    for (uint i = 0; i < numChildPart; i ++) {
        uint index = i + curInputIndex * numChildPart;
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[index]->inputIndex = curInputIndex;
        writerList[i]->open(destPartitionList[index], *hashInfo, aggList, numWriterCachePages);
    }
    /*
     * Tuples will come from memory(hash table) first.
     */
    partitionMemory = true;
}

void LhxPartitionInfo::close(
    uint curInputIndexInit)
{
    assert(curInputIndex == curInputIndexInit);

    reader->close();

    for (uint i = 0; i < writerList.size(); i ++) {
        writerList[i]->close();
    }
}

void LhxPartitionInfo::reset()
{
    destPartitionList.clear();
}

void LhxPlan::init(
    uint partitionLevelInit,
    uint numChildPartInit,   
    vector<SharedLhxPartition> &partitionsInit,
    LhxPlan *parentPlanInit)
{
    partitionLevel = partitionLevelInit;

    /*
     * After support is added for repartitioning using stats(gathered during previous round of
     * partitioning), numSubPart can be > numChildPart. A bin-packing
     * algorithm will be used to keep in memory some subpartitions and output
     * the rest to child partitions of similar size.
     * 
     */
    numSubPart   = numChildPart = numChildPartInit;
    for (int i = 0; i < partitionsInit.size(); i ++) {
        partitions.push_back(partitionsInit[i]);
    }
    parentPlan   = parentPlanInit;
}

void LhxPlan::createChildren(LhxHashInfo const &hashInfo)
{
    LhxHashGenerator hashSubPart;
    hashSubPart.init(partitionLevel + 1);

    vector<SharedLhxPartition> destPartitionList(numChildPart * 2);

    LhxPartitionReader reader;
    LhxPartitionWriter writerList[numChildPart];
    uint childNum, i, j;
    TupleData outputTuple;

    uint numInputs = 2;

    /*
     * Generate partitions for each input.
     */
    for (j = 0; j < numInputs; j ++) {
        reader.open(partitions[j], hashInfo, true);
        outputTuple.compute(hashInfo.inputDesc[j]);
    
        for (i = 0; i < numChildPart; i ++) {
            uint index = j * numChildPart + i;
            destPartitionList[index].reset(new LhxPartition());
            destPartitionList[index]->inputIndex = j;
            writerList[i].open(destPartitionList[index], hashInfo);
        }
    
        for (;;) {
            if (!reader.isTupleConsumptionPending()) {
                if (reader.getState() == EXECBUF_EOS) {
                    /*
                     * The current plan is completely partitioned.
                     */
                    break;                    
                }
                if (!reader.demandData()) {
                    break;
                }
                reader.unmarshalTuple(outputTuple);
            }
            
            childNum = 
                hashSubPart.hash(outputTuple, hashInfo.keyProj[j], 
                    hashInfo.isKeyColVarChar[j]) % numChildPart;

            writerList[childNum].marshalTuple(outputTuple);
            reader.consumeTuple();
        }
    
        for (i = 0; i < numChildPart; i ++) {
            writerList[i].close();
        }
        
        reader.close();
    }

    /*
     * Create child plans consisting of one partition from each input.
     */
    for (i = 0; i < numChildPart; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        vector<SharedLhxPartition> partitionList;
        partitionList.push_back(destPartitionList[i]);
        partitionList.push_back(destPartitionList[i + numChildPart]);
        newChildPlan->init(
            partitionLevel + 1,
            numChildPart,
            partitionList,
            this);
        if (!firstChildPlan) {
            firstChildPlan = newChildPlan;
        } else {
            firstChildPlan->addSibling(newChildPlan);
        }
    }
}

LhxPartitionState LhxPlan::generatePartitions(LhxHashInfo const &hashInfo,
    LhxPartitionInfo &partInfo, bool traceOn)
{
    bool isAggregate = (partInfo.numInput == 1);

    LhxHashGenerator hashSubPart;
    hashSubPart.init(partitionLevel + 1);

    LhxPartitionReader *reader = partInfo.reader;
    vector<SharedLhxPartitionWriter> &writerList = partInfo.writerList;

    uint curInputIndex = partInfo.curInputIndex;

    uint childNum;
    TupleData hashTableTuple;
    TupleDescriptor hashTableTupleDesc;
    TupleData inputTuple;
    TupleDescriptor const &inputTupleDesc = reader->getTupleDesc();

    hashTableTupleDesc = hashInfo.inputDesc[curInputIndex];
    hashTableTuple.compute(hashTableTupleDesc);
    inputTuple.compute(inputTupleDesc);

    /*
     * If there's hash table to read from.
     */
    if (traceOn) {
        if (curInputIndex == partInfo.numInput - 1) {
            dataTrace << "Input [1] [Tuples from Hash Table]\n";
        } else {
            dataTrace << "Input [0]\n";
        }
    }

    if ((curInputIndex == partInfo.numInput - 1) && partInfo.partitionMemory) {
        while ((partInfo.hashTableReader)->getNext(hashTableTuple)) {

            if (traceOn) {
                tuplePrinter.print(dataTrace, hashTableTupleDesc, hashTableTuple);
                dataTrace <<"\n";
            }

            childNum = hashSubPart.hash(hashTableTuple,
                hashInfo.keyProj[curInputIndex],
                hashInfo.isKeyColVarChar[curInputIndex]) % numChildPart;
            
            writerList[childNum]->marshalTuple(hashTableTuple);
            
        }
        
        if (isAggregate) {
            /*
             * release the hash table scratch pages
             * and initialize writer scratch pages.
             */
            ((partInfo.hashTableReader)->getHashTable())->releaseResources();
            for (int i = 0; i < writerList.size();i ++) {
                writerList[i]->allocateResources();
            }
        }

        /*
         * Done with tuples in the hash table. Next tuples will come from a
         * partition(stream or disk).
         */
        partInfo.partitionMemory = false;

        /*
         * The tuple for which hash table full is detected is not in the hash
         * table yet.
         */
        inputTuple = partInfo.buildTuple;
    }

    if (traceOn) {
        dataTrace << "[Tuples from exec stream or disk partition]\n";
    }

    for (;;) {
        /*
         * Note that the buildTuple is an unconsumed tuple from the reader. So
         * when first time in this loop, isTupleConsumptionPending returns
         * true.
         */
        if (!reader->isTupleConsumptionPending()) {
            if (reader->getState() == EXECBUF_EOS) {
                /*
                 * The current plan is completely partitioned.
                 */
                return PartitionEndOfData;
            }
            
            if (!reader->demandData()) {
                if (partitionLevel == 0) {
                    /*
                     * Reading from a stream: indicate underflow to producer.
                     */
                    return PartitionUnderflow;
                } else {
                    return PartitionEndOfData;
                }
            }
            
            reader->unmarshalTuple(inputTuple);
        }
        
        if (traceOn) {    
            tuplePrinter.print(dataTrace, inputTupleDesc, inputTuple);
            dataTrace <<"\n";
        }

        childNum = hashSubPart.hash(inputTuple,
            hashInfo.keyProj[curInputIndex],
            hashInfo.isKeyColVarChar[curInputIndex]) % numChildPart;
        
        if (!isAggregate) {
            writerList[childNum]->marshalTuple(inputTuple);
        } else {
            writerList[childNum]->aggAndMarshalTuple(inputTuple);
        }

        reader->consumeTuple();
    }
}

void LhxPlan::createChildren(LhxPartitionInfo &partInfo)
{
    uint i, j;

    for (i = 0; i < numChildPart; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        vector<SharedLhxPartition> partitionList;
        for (j = 0; j < partInfo.numInput; j ++) {
            partitionList.push_back(partInfo.destPartitionList[i + numChildPart * j]);
        }
        newChildPlan->init(
            partitionLevel + 1,
            numChildPart,
            partitionList,
            this);
        if (!firstChildPlan) {
            firstChildPlan = newChildPlan;
        } else {
            firstChildPlan->addSibling(newChildPlan);
        }
    }
    partInfo.destPartitionList.clear();
}

LhxPlan *LhxPlan::getFirstLeaf()
{
    if (!firstChildPlan) {
        return this;
    } else {
        return firstChildPlan->getFirstLeaf();
    }
}

LhxPlan *LhxPlan::getNextLeaf()
{
    if (siblingPlan) {
        return siblingPlan->getFirstLeaf();
    } else {
        LhxPlan *parent = this->parentPlan;

        if (parent) {
            return parent->getNextLeaf();
        } else {
            return NULL;
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LhxPartition.cpp

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
    destPartition->segStream =
        SegStreamAllocation::newSegStreamAllocation();
    destPartition->segStream->beginWrite(pSegOutputStream);

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
    destPartition->segStream =
        SegStreamAllocation::newSegStreamAllocation();
    destPartition->segStream->beginWrite(pSegOutputStream);

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
    destPartition->segStream->endWrite();
    pSegOutputStream->close();
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
    LhxHashInfo const &hashInfo)
{
    bufState = EXECBUF_NONEMPTY;
    srcPartition = srcPartitionInit;
    
    if (!srcPartition->segStream) {
        /*
         * source has never been written to, which means the source
         * is not from the disk but from input stream.
         */
        srcIsInputStream = true;
    } else {
        srcIsInputStream = false;
    }

    if (srcIsInputStream) {
        streamBufAccessor = hashInfo.streamBufAccessor[srcPartition->inputIndex];
        outputTupleDesc = streamBufAccessor->getTupleDesc();
    } else {
        outputTupleDesc = hashInfo.inputDesc[srcPartition->inputIndex];
        tupleAccessor.compute(outputTupleDesc);
        tupleAccessor.resetCurrentTupleBuf();

        /*
         * Since reader now gets input stream from the partition,
         * this inputStream will delete content that is read.
         * This also means each partition can only be read once. 
         */
        pSegInputStream = srcPartition->segStream->getInputStream();
        pSegInputStream->startPrefetch();
    }
}

void LhxPartitionReader::close() 
{
    if (srcIsInputStream) {
        /*
         * Do nothing if reading from stream.
         */
    } else {
        pSegInputStream->close();
    }
}

void LhxPartitionReader::unmarshalTuple(TupleData &outputTuple)
{
    if (srcIsInputStream) {
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
    if (srcIsInputStream) {
        streamBufAccessor->consumeTuple();
    } else {
        tupleAccessor.resetCurrentTupleBuf();
        pSegInputStream->consumeReadPointer(tupleStorageLength);        
    }
}

bool LhxPartitionReader::isTupleConsumptionPending()
{
    if (srcIsInputStream) {
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
    if (srcIsInputStream) {
        return streamBufAccessor->demandData();
    } else {
        /*
         * Read from disk.
         */
        uint bytesReadable = 0;
        PConstBuffer pSrcBuf =
            pSegInputStream->getReadPointer(1, &bytesReadable);
        
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
    /*
     * writerList is shared across iterations of partitioning. At each
     * iteration, writerList is initialized with new destination partitions.
     */
    for (uint i = 0; i < numInput * numChildPart; i ++) {
        writerList.push_back(
            SharedLhxPartitionWriter(new LhxPartitionWriter()));
    }
    
    hashInfo = hashInfoInit;
}

void LhxPartitionInfo::open(
    LhxHashTableReader *hashTableReaderInit,
    LhxPartitionReader *buildReader,
    TupleData &buildTupleInit,
    SharedLhxPartition probePartition)
{
    uint i;

    probeReader.open(probePartition, *hashInfo);

    /*
     * Start partitioning from the build side.
     */
    curInputIndex = numInput - 1;

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

    destPartitionList.clear();
    joinFilterList.clear();

    for (i = 0; i < numInput * numChildPart; i ++) {
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[i]->inputIndex = (i / numChildPart);

        /*
         * One filter for each partition
         * filter bitmap is only allocated when a partition is written to
         */
        joinFilterList.push_back(boost::shared_ptr<boost::dynamic_bitset<> >());

        writerList[i]->open(destPartitionList[i], *hashInfo);
    }

    /*
     * Tuples will come from memory(hash table) first.
     */
    partitionMemory = true;
}


void LhxPartitionInfo::open(
    LhxHashTableReader *hashTableReaderInit,
    LhxPartitionReader *buildReader,
    TupleData &buildTupleInit,
    AggComputerList *aggList)
{
    uint i;
    assert (numInput == 1);
    curInputIndex = 0;

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

    destPartitionList.clear();
    joinFilterList.clear();

    for (i = 0; i < numInput * numChildPart; i ++) {
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[i]->inputIndex = (i / numChildPart);
        writerList[i]->open(destPartitionList[i], *hashInfo, aggList,
                            numWriterCachePages);
        joinFilterList.push_back(boost::shared_ptr<boost::dynamic_bitset<> >());
    }

    /*
     * Tuples will come from memory(hash table) first.
     */
    partitionMemory = true;
}

void LhxPartitionInfo::close()
{
    reader->close();
    
    uint numWriters = writerList.size();

    for (uint i = 0; i < numWriters; i ++) {
        writerList[i]->close();
    }
}

void LhxPlan::init(
    uint partitionLevelInit,
    uint numChildPartInit,   
    vector<SharedLhxPartition> &partitionsInit,
    LhxPlan *parentPlanInit,
    vector<bool> useFilterInit,
    boost::shared_ptr<boost::dynamic_bitset<> > joinFilterInit)
{
    partitionLevel = partitionLevelInit;
    useFilter = useFilterInit;
    joinFilter = joinFilterInit;

    /*
     * After support is added for repartitioning using stats(gathered during
     * previous round of partitioning), numSubPart can be > numChildPart. A
     * bin-packing algorithm will be used to keep in memory some subpartitions
     * and output the rest to child partitions of similar size.
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

    uint numInputs = 2;

    vector<SharedLhxPartition> destPartitionList(numChildPart * numInputs);

    LhxPartitionReader reader;
    LhxPartitionWriter writerList[numChildPart];
    uint childNum, i, j;
    TupleData outputTuple;

    /*
     * Generate partitions for each input.
     */
    for (j = 0; j < numInputs; j ++) {
        reader.open(partitions[j], hashInfo);
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

LhxPartitionState LhxPlan::generatePartitions(
    LhxHashInfo const &hashInfo,
    LhxPartitionInfo  &partInfo)
{
    bool isAggregate = (partInfo.numInput == 1);

    LhxHashGenerator hashPrevPart;
    hashPrevPart.init(partitionLevel);

    LhxHashGenerator hashSubPart;
    hashSubPart.init(partitionLevel + 1);

    LhxPartitionReader *&reader = partInfo.reader;
    vector<SharedLhxPartitionWriter> &writerList = partInfo.writerList;
    vector<boost::shared_ptr<boost::dynamic_bitset<> > > &joinFilterList =
        partInfo.joinFilterList;
    uint &curInputIndex = partInfo.curInputIndex;

    TupleData inputTuple;
    inputTuple.compute(reader->getTupleDesc());
    
    uint hashKey;
    uint prevHashKey;
    uint writerNum;
    bool writeToPartition;
    
    /*
     * If partition source is from memory,
     * i.e. there's hash table to read from.
     */
    if (partInfo.partitionMemory) {
        TupleData hashTableTuple;
        TupleDescriptor hashTableTupleDesc = hashInfo.inputDesc[curInputIndex];

        hashTableTuple.compute(hashTableTupleDesc);

        while ((partInfo.hashTableReader)->getNext(hashTableTuple)) {

            hashKey = hashSubPart.hash(hashTableTuple,
                hashInfo.keyProj[curInputIndex],
                hashInfo.isKeyColVarChar[curInputIndex]);

            writerNum =  hashKey % numChildPart + curInputIndex * numChildPart;

            writeToPartition = false;

            if (useFilter[curInputIndex]) {
                /*
                 * Use input filter if there is one. Note top level build input
                 * does not have input filter.
                 */
                if (partitionLevel == 0) {
                    writeToPartition = true;
                } else {
                    prevHashKey =
                        hashPrevPart.hash(hashTableTuple,
                            hashInfo.keyProj[curInputIndex],
                            hashInfo.isKeyColVarChar[curInputIndex]);
                    if (joinFilter && joinFilter->test(prevHashKey % 4096)) {
                        writeToPartition = true;
                    }
                }
            } else {
                /*
                 * Not using filter.
                 */
                writeToPartition = true;
            }
            
            if (writeToPartition) {
                writerList[writerNum]->marshalTuple(hashTableTuple);
                if (useFilter[partInfo.numInput - curInputIndex - 1]) {
                    /*
                     * Set output filter for the other input.
                     */
                    if (!joinFilterList[writerNum]) {
                        /*
                         * Filter not allocated yet.
                         */
                        joinFilterList[writerNum].reset(
                            new boost::dynamic_bitset<>(4096));
                    }
                    joinFilterList[writerNum]->set(hashKey % 4096);
                }
            }
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

    for (;;) {
        /*
         * Note that partInfo.buildTuple is an unconsumed tuple from the
         * reader. So when first time in this loop, isTupleConsumptionPending
         * returns true.
         */
        if (!reader->isTupleConsumptionPending()) {
            if (reader->getState() == EXECBUF_EOS) {
                if (curInputIndex == 0) {
                    /*
                     * Done with partitioning the 0th input.
                     * The current plan is completely partitioned.
                     */
                    return PartitionEndOfData;
                } else {
                    curInputIndex = 0;
                    reader->close();
                    reader = &partInfo.probeReader;
                    inputTuple.compute(reader->getTupleDesc());
                    continue;
                }
            }
            
            if (!reader->demandData()) {
                if (partitionLevel == 0) {
                    /*
                     * Reading from a stream: indicate underflow to producer.
                     */
                    return PartitionUnderflow;
                } else {
                    if (curInputIndex == 0) {
                        /*
                         * Done with partitioning the 0th input.
                         * The current plan is completely partitioned.
                         */
                        return PartitionEndOfData;
                    } else {
                        curInputIndex = 0;
                        reader->close();
                        reader = &partInfo.probeReader;
                        inputTuple.compute(reader->getTupleDesc());
                        continue;
                    }
                }
            }
            
            reader->unmarshalTuple(inputTuple);
        }
        
        hashKey = hashSubPart.hash(inputTuple,
            hashInfo.keyProj[curInputIndex],
            hashInfo.isKeyColVarChar[curInputIndex]);
        writerNum =  hashKey % numChildPart + curInputIndex * numChildPart;
        
        writeToPartition = false;

        if (!isAggregate) {
            if (useFilter[curInputIndex]) {
                /*
                 * Use input filter if there exists one
                 */                
                if (writerNum >= numChildPart) {
                    /*
                     * Build input.
                     * Note top level build input does not have input filter.
                     */
                    if (partitionLevel == 0) {
                        writeToPartition = true;
                    } else {
                        prevHashKey =
                            hashPrevPart.hash(inputTuple,
                                hashInfo.keyProj[curInputIndex],
                                hashInfo.isKeyColVarChar[curInputIndex]);
                        if (joinFilter && joinFilter->test(prevHashKey % 4096)) {
                            writeToPartition = true;
                        }
                    }
                } else {
                    /*
                     * Probe input
                     */
                    if (joinFilterList[writerNum + numChildPart] &&
                        joinFilterList[writerNum + numChildPart]->
                        test(hashKey % 4096)) {
                        writeToPartition = true;
                    }
                }
            } else {
                /*
                 * Not using filter.
                 */
                writeToPartition = true;
            }

            if (writeToPartition) {
                writerList[writerNum]->marshalTuple(inputTuple);
                if (useFilter[partInfo.numInput - curInputIndex - 1]) {
                    /*
                     * Set output filter for the other input.
                     */
                    if (!joinFilterList[writerNum]) {
                        /*
                         * Filter not allocated yet.
                         */
                        joinFilterList[writerNum].reset(
                            new boost::dynamic_bitset<>(4096));
                    }
                    joinFilterList[writerNum]->set(hashKey % 4096);
                }
            }
        } else {
            writerList[writerNum]->aggAndMarshalTuple(inputTuple);
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
            this,
            useFilter,
            partInfo.joinFilterList[i]);
        if (!firstChildPlan) {
            firstChildPlan = newChildPlan;
        } else {
            firstChildPlan->addSibling(newChildPlan);
        }
    }
    partInfo.destPartitionList.clear();
    partInfo.joinFilterList.clear();
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

void LhxPlan::close()
{
    if (firstChildPlan) {
        firstChildPlan->close();
    }

    if (siblingPlan) {
        siblingPlan->close();
    }

    for (uint i = 0; i < partitions.size(); i ++) {
        if (partitions[i] && partitions[i]->segStream) {
            partitions[i]->segStream->close();
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LhxPartition.cpp

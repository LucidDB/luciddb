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
    LhxHashInfo *hashInfoInit)
{
    numInput     = numInputInit;

    writerList.clear();
    /*
     * writerList is shared across iterations of partitioning. At each
     * iteration, writerList is initialized with new destination partitions.
     */
    for (uint i = 0; i < numInput * LhxPlan::LhxChildPartCount; i ++) {
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
    uint i, j;
    uint buildIndex = numInput - 1;

    probeReader.open(probePartition, *hashInfo);

    /*
     * Start partitioning from the build side.
     */
    curInputIndex = buildIndex;

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
    subPartStatList.clear();
    joinFilterList.clear();

    for (i = 0; i < numInput * LhxPlan::LhxChildPartCount; i ++) {
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[i]->inputIndex = (i / LhxPlan::LhxChildPartCount);
        subPartStatList.push_back(
            shared_array<uint>(new uint[LhxPlan::LhxSubPartCount]));

        for (j = 0; j < LhxPlan::LhxSubPartCount; j ++) {
            (subPartStatList[i])[j] = 0;
        }

        /*
         * One filter for each partition
         * filter bitmap is only allocated when a partition is written to
         */
        joinFilterList.push_back(shared_ptr<dynamic_bitset<> >());

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
    uint i, j;
    assert (numInput == 1);
    uint buildIndex = numInput - 1;

    curInputIndex = buildIndex;

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
    uint numWriterCachePages =
        hashInfo->numCachePages / LhxPlan::LhxChildPartCount;

    destPartitionList.clear();
    subPartStatList.clear();
    joinFilterList.clear();
    
    for (i = 0; i < numInput * LhxPlan::LhxChildPartCount; i ++) {
        destPartitionList.push_back(SharedLhxPartition(new LhxPartition()));
        destPartitionList[i]->inputIndex = (i / LhxPlan::LhxChildPartCount);
        subPartStatList.push_back(
            shared_array<uint>(new uint[LhxPlan::LhxSubPartCount]));

        for (j = 0; j < LhxPlan::LhxSubPartCount; j ++) {
            (subPartStatList[i])[j] = 0;
        }

        /*
         * One filter for each partition
         * filter bitmap is only allocated when a partition is written to
         */
        joinFilterList.push_back(shared_ptr<dynamic_bitset<> >());

        writerList[i]->open(destPartitionList[i], *hashInfo, aggList,
                            numWriterCachePages);
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
    vector<SharedLhxPartition> &partitionsInit,
    LhxPlan *parentPlanInit,
    bool useSubPartStatInit)
{
    /*
     * No filter for this plan.
     */
    shared_ptr<dynamic_bitset<> > joinFilterInit =
        shared_ptr<dynamic_bitset<> >();
    vector<shared_array<uint> > subPartStatsInit;

    vector<bool> useJoinFilter;

    for (uint i = 0; i < partitionsInit.size(); i ++) {
        useJoinFilter.push_back(false);
        subPartStatsInit.push_back(shared_array<uint>());
    }

    useSubPartStat = useSubPartStatInit;

    init(partitionLevelInit, partitionsInit, useSubPartStat, subPartStatsInit,
        parentPlanInit, useJoinFilter, joinFilterInit);
}

void LhxPlan::init(
    uint partitionLevelInit,
    vector<SharedLhxPartition> &partitionsInit,
    bool useSubPartStatInit,
    vector<shared_array<uint> > &subPartStats,
    LhxPlan *parentPlanInit,
    vector<bool> useJoinFilterInit,
    shared_ptr<dynamic_bitset<> > joinFilterInit)
{
    partitionLevel = partitionLevelInit;
    useJoinFilter = useJoinFilterInit;
    joinFilter = joinFilterInit;
    uint numInput = partitionsInit.size();

    useSubPartStat = useSubPartStatInit;

    uint buildIndex =  numInput - 1;
    shared_array<uint> buildSubPartStat = subPartStats[buildIndex];
    
    if (useSubPartStat && buildSubPartStat) {
        mapSubPartToChild(buildSubPartStat);
    } else {
        subPartToChildMap.reset();
    }

    /*
     * After support is added for repartitioning using stats(gathered during
     * previous round of partitioning), a bin-packing algorithm will be used to
     * keep in memory some subpartitions and output the rest to child
     * partitions of similar size.
     */
    for (int i = 0; i < numInput; i ++) {
        partitions.push_back(partitionsInit[i]);
    }
    parentPlan   = parentPlanInit;
}

void LhxPlan::createChildren(LhxHashInfo const &hashInfo)
{
    LhxHashGenerator hashGen;
    hashGen.init(partitionLevel + 1);

    uint numInput = hashInfo.inputDesc.size();

    vector<SharedLhxPartition> destPartitionList(LhxChildPartCount * numInput);

    LhxPartitionReader reader;
    LhxPartitionWriter writerList[LhxChildPartCount];
    uint childNum, i, j;
    TupleData outputTuple;

    /*
     * Generate partitions for each input.
     */
    for (j = 0; j < numInput; j ++) {
        reader.open(partitions[j], hashInfo);
        outputTuple.compute(hashInfo.inputDesc[j]);
    
        for (i = 0; i < LhxChildPartCount; i ++) {
            uint index = j * LhxChildPartCount + i;
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
                hashGen.hash(outputTuple, hashInfo.keyProj[j], 
                    hashInfo.isKeyColVarChar[j]) % LhxChildPartCount;

            writerList[childNum].marshalTuple(outputTuple);
            reader.consumeTuple();
        }
    
        for (i = 0; i < LhxChildPartCount; i ++) {
            writerList[i].close();
        }
        reader.close();
    }

    /*
     * Create child plans consisting of one partition from each input.
     */
    for (i = 0; i < LhxChildPartCount; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        vector<SharedLhxPartition> partitionList;
        partitionList.push_back(destPartitionList[i]);
        partitionList.push_back(destPartitionList[i + LhxChildPartCount]);

        newChildPlan->init(
            partitionLevel + 1,
            partitionList,
            this, useSubPartStat);

        newChildPlan->addSibling(firstChildPlan);     
        firstChildPlan = newChildPlan;
    }
}

void LhxPlan::mapSubPartToChild(
    shared_array<uint> subPartStat)
{
    subPartToChildMap.reset(new uint[LhxSubPartCount]);
    childPartSize.reset(new uint[LhxChildPartCount]);

    uint i, j, k;

    for (i = 0; i < LhxChildPartCount; i ++) {
        childPartSize[i] = 0;
    }

    j = 0;
    for (i = 0; i < LhxSubPartCount; i ++) {
        childPartSize[j] += subPartStat[i];
        subPartToChildMap[i] = j;

        k = 1;
        while ((childPartSize[j] > childPartSize[(j + k) % LhxChildPartCount])
            && k < LhxChildPartCount) {
            k ++;
        }

        if (k == LhxChildPartCount) {
            // If current child partition is bigger than all other child
            // partitions, move to the next child
            j = (j + 1) % LhxChildPartCount;
        }
    }
}

uint LhxPlan::calculateChildIndex(uint hashKey, uint curInputIndex)
{
    if (subPartToChildMap) {
        return (subPartToChildMap[hashKey % LhxSubPartCount] +
            curInputIndex * LhxChildPartCount);
    } else {
        return (hashKey % LhxChildPartCount +
            curInputIndex * LhxChildPartCount);
    }
}

LhxPartitionState LhxPlan::generatePartitions(
    LhxHashInfo const &hashInfo,
    LhxPartitionInfo  &partInfo)
{
    uint filterSize = 4096;
    bool isAggregate = (partInfo.numInput == 1);

    LhxHashGenerator hashGenPrev;
    LhxHashGenerator hashGen;
    LhxHashGenerator hashGenNext;

    hashGenPrev.init(partitionLevel);
    hashGen.init(partitionLevel + 1);
    hashGenNext.init(partitionLevel + 2);

    LhxPartitionReader *&reader = partInfo.reader;
    vector<SharedLhxPartitionWriter> &writerList = partInfo.writerList;
    vector<shared_ptr<dynamic_bitset<> > > &joinFilterList =
        partInfo.joinFilterList;
    vector<shared_array<uint> > &subPartStatList = partInfo.subPartStatList;

    uint &curInputIndex = partInfo.curInputIndex;

    TupleData inputTuple;
    inputTuple.compute(reader->getTupleDesc());
    
    uint prevHashKey;
    uint hashKey;
    uint nextHashKey;

    uint writerIndex;
    bool writeToPartition;

    uint statIndex;
    
    /*
     * If partition source is from memory,
     * i.e. there's hash table to read from.
     */
    if (partInfo.partitionMemory) {
        TupleData hashTableTuple;
        TupleDescriptor hashTableTupleDesc = hashInfo.inputDesc[curInputIndex];

        hashTableTuple.compute(hashTableTupleDesc);

        while ((partInfo.hashTableReader)->getNext(hashTableTuple)) {

            writeToPartition = false;

            if (useJoinFilter[curInputIndex]) {
                /*
                 * Use input filter if there is one. Note top level build input
                 * does not have input filter.
                 */
                if (partitionLevel == 0) {
                    writeToPartition = true;
                } else {
                    prevHashKey =
                        hashGenPrev.hash(hashTableTuple,
                            hashInfo.keyProj[curInputIndex],
                            hashInfo.isKeyColVarChar[curInputIndex]);
                    if (joinFilter && joinFilter->test(prevHashKey % filterSize)) {
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
                hashKey = hashGen.hash(hashTableTuple,
                    hashInfo.keyProj[curInputIndex],
                    hashInfo.isKeyColVarChar[curInputIndex]);
                
                writerIndex = calculateChildIndex(hashKey, curInputIndex);

                writerList[writerIndex]->marshalTuple(hashTableTuple);

                nextHashKey = hashGenNext.hash(hashTableTuple,
                    hashInfo.keyProj[curInputIndex],
                    hashInfo.isKeyColVarChar[curInputIndex]);

                statIndex = nextHashKey % LhxSubPartCount;

                (subPartStatList[writerIndex])[statIndex]++;

                if (useJoinFilter[partInfo.numInput - curInputIndex - 1]) {
                    /*
                     * Set output filter for the other input.
                     */
                    if (!joinFilterList[writerIndex]) {
                        /*
                         * Filter not allocated yet.
                         */
                        joinFilterList[writerIndex].reset(
                            new dynamic_bitset<>(filterSize));
                    }
                    joinFilterList[writerIndex]->set(hashKey % filterSize);
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
        
        writeToPartition = false;

        hashKey = hashGen.hash(inputTuple,
            hashInfo.keyProj[curInputIndex],
            hashInfo.isKeyColVarChar[curInputIndex]);
        
        writerIndex = calculateChildIndex(hashKey, curInputIndex);

        nextHashKey = hashGenNext.hash(inputTuple,
            hashInfo.keyProj[curInputIndex],
            hashInfo.isKeyColVarChar[curInputIndex]);
        
        statIndex = nextHashKey % LhxSubPartCount;

        if (!isAggregate) {
            if (useJoinFilter[curInputIndex]) {
                /*
                 * Use input filter if there exists one
                 */                
                if (writerIndex >= LhxChildPartCount) {
                    /*
                     * Build input.
                     * Note top level build input does not have input filter.
                     */
                    if (partitionLevel == 0) {
                        writeToPartition = true;
                    } else {
                        prevHashKey =
                            hashGenPrev.hash(inputTuple,
                                hashInfo.keyProj[curInputIndex],
                                hashInfo.isKeyColVarChar[curInputIndex]);
                        if (joinFilter &&
                            joinFilter->test(prevHashKey % filterSize)) {
                            writeToPartition = true;
                        }
                    }
                } else {
                    /*
                     * Probe input
                     */
                    if (joinFilterList[writerIndex + LhxChildPartCount] &&
                        joinFilterList[writerIndex + LhxChildPartCount]->
                        test(hashKey % filterSize)) {
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
                writerList[writerIndex]->marshalTuple(inputTuple);

                (subPartStatList[writerIndex])[statIndex]++;

                if (useJoinFilter[partInfo.numInput - curInputIndex - 1]) {
                    /*
                     * Set output filter for the other input.
                     */
                    if (!joinFilterList[writerIndex]) {
                        /*
                         * Filter not allocated yet.
                         */
                        joinFilterList[writerIndex].reset(
                            new dynamic_bitset<>(filterSize));
                    }
                    joinFilterList[writerIndex]->set(hashKey % filterSize);
                }
            }
        } else {
            writerList[writerIndex]->aggAndMarshalTuple(inputTuple);
            (subPartStatList[writerIndex])[statIndex]++;
        }

        reader->consumeTuple();
    }
}

void LhxPlan::createChildren(LhxPartitionInfo &partInfo)
{
    uint i, j;

    for (i = 0; i < LhxChildPartCount; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        vector<SharedLhxPartition> partitionList;
        vector<shared_array<uint> > subPartStats;
        for (j = 0; j < partInfo.numInput; j ++) {
            partitionList.push_back(
                partInfo.destPartitionList[i + LhxChildPartCount * j]);
            subPartStats.push_back(
                partInfo.subPartStatList[i + LhxChildPartCount * j]);
        }
        newChildPlan->init(
            partitionLevel + 1,
            partitionList,
            useSubPartStat,
            subPartStats,
            this,
            useJoinFilter,
            partInfo.joinFilterList[i]);

        newChildPlan->addSibling(firstChildPlan);     
        firstChildPlan = newChildPlan;
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

string LhxPlan::toString()
{
    ostringstream planTrace;

    planTrace << "\n"
              << "[Plan : addr       = " << this                 << "]\n"
              << "[       level      = " << partitionLevel       << "]\n"
              << "[       parent     = " << parentPlan           << "]\n"
              << "[       firstChild = " << firstChildPlan.get() << "]\n"
              << "[       sibling    = " << siblingPlan.get()    << "]\n";

    /*
     * Print out the partitions.
     */
    for (uint i = 0; i < partitions.size(); i ++) {
        planTrace << "[Partition(" << i << ")]\n"
                  << "[       inputIndex     = " << partitions[i]->inputIndex << "]\n"
                  << "[       useJoinFilter  = " << useJoinFilter[i]             << "]\n";
    }

    planTrace << "[joinFilter = ";
    if (joinFilter) {
        planTrace << joinFilter.get();
    }
    planTrace <<"]\n";

    planTrace << "[childPartSize = ";
    if (childPartSize) {
        for (uint i = 0; i < LhxChildPartCount; i ++) {
            planTrace << childPartSize[i] << " ";
        }
    }
    planTrace <<"]\n";

    planTrace << "[subPartToChildMap = ";
    if (subPartToChildMap) {
        for (uint i = 0; i < LhxSubPartCount; i ++) {
            planTrace << subPartToChildMap[i] << " ";
        }
    }
    planTrace <<"]\n";

    SharedLhxPlan childPlan = firstChildPlan;

    while (childPlan) {
        planTrace << childPlan->toString();
        childPlan = childPlan->siblingPlan;
    }
    
    return planTrace.str();
}

FENNEL_END_CPPFILE("$Id$");

// End LhxPartition.cpp

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

    hashTable.init(partitionLevel, hashInfo, aggList,
        destPartition->inputIndex);
    hashTableReader.init(&hashTable, hashInfo, destPartition->inputIndex);

    hashInfo.numCachePages = savedNumCachePages;

    uint cndKeys = hashInfo.cndKeys.back();
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
         * Reset the reader and bind it to no particular key (will read the
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
        streamBufAccessor =
            hashInfo.streamBufAccessor[srcPartition->inputIndex];
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
            assert(bytesReadable >= tupleStorageLength);
            if (bytesReadable == tupleStorageLength) {
                // We're processing the last tuple in a buffer,
                // so now is a good time to check for abort.
                if (srcPartition->pExecStream) {
                    srcPartition->pExecStream->checkAbort();
                }
            }
            tupleAccessor.setCurrentTupleBuf(pSrcBuf);
            return true;
        }
    }
}

void LhxPartitionInfo::init(LhxHashInfo *hashInfoInit)
{
    hashInfo = hashInfoInit;
    numInputs = (hashInfo->inputDesc).size();

    writerList.clear();
    /*
     * writerList is shared across iterations of partitioning. At each
     * iteration, writerList is initialized with new destination partitions.
     */
    for (uint i = 0; i < numInputs * LhxPlan::LhxChildPartCount; i ++) {
        writerList.push_back(
            SharedLhxPartitionWriter(new LhxPartitionWriter()));
    }

    filteredRowCountList.reset(
        new uint[numInputs * LhxPlan::LhxChildPartCount]);
}

void LhxPartitionInfo::open(
    LhxHashTableReader *hashTableReaderInit,
    LhxPartitionReader *buildReader,
    TupleData &buildTupleInit,
    SharedLhxPartition probePartition,
    uint buildInputIndex)
{
    uint i, j;

    probeReader.open(probePartition, *hashInfo);

    /*
     * Start partitioning from the build side.
     */
    curInputIndex = buildInputIndex;

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
     * The inflight (between disk partition and hash table) build tuple.
     */
    buildTuple = buildTupleInit;

    destPartitionList.clear();
    subPartStatList.clear();
    joinFilterList.clear();
    shared_array<uint> curSubPartStat;

    for (i = 0; i < numInputs * LhxPlan::LhxChildPartCount; i ++) {
        destPartitionList.push_back(
            SharedLhxPartition(new LhxPartition(probePartition->pExecStream)));
        destPartitionList[i]->inputIndex = (i / LhxPlan::LhxChildPartCount);
        subPartStatList.push_back(
            shared_array<uint>(new uint[LhxPlan::LhxSubPartCount]));

        curSubPartStat = subPartStatList[i];

        for (j = 0; j < LhxPlan::LhxSubPartCount; j ++) {
            curSubPartStat[j] = 0;
        }

        /*
         * One filter for each partition
         * filter bitmap is only allocated when a partition is written to
         */
        joinFilterList.push_back(shared_ptr<dynamic_bitset<> >());

        writerList[i]->open(destPartitionList[i], *hashInfo);
        filteredRowCountList[i] = 0;
    }

    /*
     * Tuples will come from memory (hash table) first.
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
    assert (numInputs == 1);
    uint buildIndex = numInputs - 1;

    curInputIndex = buildIndex;

    hashTableReader = hashTableReaderInit;
    /*
     * Reset the reader and bind it to no particular key (will read the
     * whole hash table).
     */
    hashTableReader->bindKey(NULL);

    // REVIEW jvs 26-Aug-2006:  no join if doing agg...
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
    shared_array<uint> curSubPartStat;

    for (i = 0; i < numInputs * LhxPlan::LhxChildPartCount; i ++) {
        destPartitionList.push_back(
            SharedLhxPartition(
                new LhxPartition(reader->getSourcePartition()->pExecStream)));
        destPartitionList[i]->inputIndex = (i / LhxPlan::LhxChildPartCount);
        subPartStatList.push_back(
            shared_array<uint>(new uint[LhxPlan::LhxSubPartCount]));

        curSubPartStat = subPartStatList[i];

        for (j = 0; j < LhxPlan::LhxSubPartCount; j ++) {
            curSubPartStat[j] = 0;
        }

        // REVIEW jvs 26-Aug-2006:  no join if doing agg...
        /*
         * One filter for each partition
         * filter bitmap is only allocated when a partition is written to
         */
        joinFilterList.push_back(shared_ptr<dynamic_bitset<> >());

        writerList[i]->open(destPartitionList[i], *hashInfo, aggList,
                            numWriterCachePages);
        filteredRowCountList[i] = 0;
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

    /*
     * Partial aggregate hash tables used inside the writers(one HT
     * for each writer) share the same scratch buffer space.
     * Release the buffer pages used by these hash tables at the end,
     * after all writers have been closed(so that there will be no more
     * scratch page alloc calls).
     */
    for (uint i = 0; i < numWriters; i ++) {
        writerList[i]->releaseResources();
    }
}

void LhxPlan::init(
    WeakLhxPlan parentPlanInit,
    uint partitionLevelInit,
    vector<SharedLhxPartition> &partitionsInit,
    bool enableSubPartStat)
{
    /*
     * No filter for this plan.
     */
    shared_ptr<dynamic_bitset<> > joinFilterInit =
        shared_ptr<dynamic_bitset<> >();
    vector<shared_array<uint> > subPartStatsInit;
    vector<uint> filteredRows;

    for (uint i = 0; i < partitionsInit.size(); i ++) {
        subPartStatsInit.push_back(shared_array<uint>());
        filteredRows.push_back(0);
    }

    init(parentPlanInit, partitionLevelInit, partitionsInit,
        subPartStatsInit, joinFilterInit, filteredRows,
        enableSubPartStat, false);
}

void LhxPlan::init(
    WeakLhxPlan parentPlanInit,
    uint partitionLevelInit,
    vector<SharedLhxPartition> &partitionsInit,
    vector<shared_array<uint> > &subPartStats,
    shared_ptr<dynamic_bitset<> > joinFilterInit,
    vector<uint> &filteredRowsInit,
    bool enableSubPartStat,
    bool enableSwing)
{
    uint numInputs = partitionsInit.size();

    partitionLevel = partitionLevelInit;
    parentPlan   = parentPlanInit;

    /*
     * REVIEW(rchen 2006-08-08): if input sides swing, then the new build could
     * be the same as the probe input of the previous partition. This filter
     * will not filter any tuple as it tries to filter the very input the
     * filter is built on.
     */
    joinFilter = joinFilterInit;

    filteredRowCount.reset(new uint[numInputs]);
    inputSize.reset(new uint[numInputs]);
    joinSideToInputMap.reset(new uint[numInputs]);
    subPartToChildMap.reset();

    // REVIEW jvs 26-Aug-2006:  here "will be used" means once someone
    // gets around to true hybrid, right?
    /*
     * After support is added for repartitioning using stats (gathered during
     * previous round of partitioning), a bin-packing algorithm will be used to
     * keep in memory some subpartitions and output the rest to child
     * partitions of similar size.
     */
    for (int i = 0; i < numInputs; i ++) {
        partitions.push_back(partitionsInit[i]);
        filteredRowCount[i] = filteredRowsInit[i];
        joinSideToInputMap[i] = i;

        inputSize[i] = 0;
        shared_array<uint> inputSubPartStat = subPartStats[i];

        if (inputSubPartStat) {
            for (int k = 0; k < LhxSubPartCount; k ++) {
                inputSize[i] += inputSubPartStat[k];
            }
        }
    }

    /*
     * Map join side to input, using partitions stats associated with each
     * input. The input with the smaller side witll be the build side for the
     * join: joinSide for the build will map to the index of this input.
     */
    if (enableSwing &&
        (numInputs == 2) && (inputSize[0] < inputSize[1])) {
        joinSideToInputMap[0] = 1;
        joinSideToInputMap[1] = 0;
    }

    subPartToChildMap.reset();

    if (enableSubPartStat) {
        /*
         * Use build side sub part stat to divide both inputs into child
         * partitions. Needs to be called after the join sides have been
         * assigned.
         */
        mapSubPartToChild(subPartStats);
    }
}

void LhxPlan::mapSubPartToChild(
    vector<shared_array<uint> > &subPartStats)
{
    uint numInputs = partitions.size();
    uint buildIndex =  getBuildInput();
    shared_array<uint> buildSubPartStat = subPartStats[buildIndex];

    if (!buildSubPartStat) {
        return;
    }

    uint i, j, k;

    subPartToChildMap.reset(new uint[LhxSubPartCount]);

    for (i = 0; i < numInputs; i ++) {
        childPartSize.push_back(
            shared_array<uint>(new uint[LhxChildPartCount]));
    }

    shared_array<uint> buildChildPartSize = childPartSize[buildIndex];

    for (i = 0; i < LhxChildPartCount; i ++) {
        buildChildPartSize[i] = 0;
    }

    j = 0;
    for (i = 0; i < LhxSubPartCount; i ++) {
        buildChildPartSize[j] += buildSubPartStat[i];
        subPartToChildMap[i] = j;

        k = 1;
        while (
            (buildChildPartSize[j]
                > buildChildPartSize[(j + k) % LhxChildPartCount])
            && k < LhxChildPartCount)
        {
            k ++;
        }

        if (k == LhxChildPartCount) {
            // If current child partition is bigger than all other child
            // partitions, move to the next child
            j = (j + 1) % LhxChildPartCount;
        }
    }

    /*
     * This is simply stats keeping for the probe side child partitions.
     */
    if (numInputs == 2) {
        uint probeIndex = getProbeInput();
        shared_array<uint> probeChildPartSize = childPartSize[probeIndex];
        shared_array<uint> probeSubPartStat = subPartStats[probeIndex];

        for (i = 0; i < LhxChildPartCount; i ++) {
            probeChildPartSize[i] = 0;
        }

        for (i = 0; i < LhxSubPartCount; i ++) {
            probeChildPartSize[subPartToChildMap[i]] += probeSubPartStat[i];
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
    // REVIEW jvs 26-Aug-2006:  modulo on this is computed below; make
    // sure compiler is optimizing to bitmask, or do it by hand
    uint filterSize = 4096;
    bool isAggregate = (partInfo.numInputs == 1);

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
    shared_array<uint> &filteredRowCountList = partInfo.filteredRowCountList;

    uint &curInputIndex = partInfo.curInputIndex;
    uint otherInputIndex = partInfo.numInputs - curInputIndex - 1;

    TupleData inputTuple;
    TupleDescriptor inputTupleDesc = reader->getTupleDesc();
    inputTuple.compute(inputTupleDesc);

    uint prevHashKey;
    uint hashKey;
    uint nextHashKey;

    uint childPartIndex;
    bool writeToPartition;

    uint statIndex;
    shared_array<uint> curSubPartStat;

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

            hashKey = hashGen.hash(hashTableTuple,
                hashInfo.keyProj[curInputIndex],
                hashInfo.isKeyColVarChar[curInputIndex]);

            childPartIndex = calculateChildIndex(hashKey, curInputIndex);

            if (hashInfo.useJoinFilter[curInputIndex]) {
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
                    if (joinFilter &&
                        joinFilter->test(prevHashKey % filterSize)) {
                        writeToPartition = true;
                    } else {
                        filteredRowCountList[childPartIndex]++;
                    }
                }
            } else {
                /*
                 * Not using filter.
                 */
                writeToPartition = true;
            }

            if (writeToPartition) {
                writerList[childPartIndex]->marshalTuple(hashTableTuple);

                nextHashKey = hashGenNext.hash(hashTableTuple,
                    hashInfo.keyProj[curInputIndex],
                    hashInfo.isKeyColVarChar[curInputIndex]);

                statIndex = nextHashKey % LhxSubPartCount;
                curSubPartStat = subPartStatList[childPartIndex];
                curSubPartStat[statIndex]++;

                /*
                 * Set output filter for the other input.
                 * Note: if the filter is used on the next level, "the other
                 * input" could be the same if join sides are switched.
                 */
                if (!joinFilterList[childPartIndex]) {
                    /*
                     * Filter not allocated yet.
                     */
                    joinFilterList[childPartIndex].reset(
                        new dynamic_bitset<>(filterSize));
                }
                joinFilterList[childPartIndex]->set(hashKey % filterSize);
            }
        }

        if (isAggregate) {
            /*
             * release the agg exec stream hash table scratch pages
             * and initialize writer scratch pages.
             */
            ((partInfo.hashTableReader)->getHashTable())->releaseResources();
            for (int i = 0; i < writerList.size();i ++) {
                writerList[i]->allocateResources();
            }
        }

        /*
         * Done with tuples in the hash table. Next tuples will come from a
         * partition (stream or disk).
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
                if (curInputIndex == getProbeInput()) {
                    /*
                     * Done with partitioning the 0th input.
                     * The current plan is completely partitioned.
                     */
                    return PartitionEndOfData;
                } else {
                    curInputIndex = getProbeInput();
                    otherInputIndex = partInfo.numInputs - curInputIndex - 1;
                    reader->close();
                    reader = &partInfo.probeReader;
                    inputTupleDesc = reader->getTupleDesc();
                    inputTuple.compute(inputTupleDesc);
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
                    if (curInputIndex == getProbeInput()) {
                        /*
                         * Done with partitioning the 0th input.
                         * The current plan is completely partitioned.
                         */
                        return PartitionEndOfData;
                    } else {
                        curInputIndex = getProbeInput();
                        reader->close();
                        reader = &partInfo.probeReader;
                        inputTupleDesc = reader->getTupleDesc();
                        inputTuple.compute(inputTupleDesc);
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

        childPartIndex = calculateChildIndex(hashKey, curInputIndex);

        nextHashKey = hashGenNext.hash(inputTuple,
            hashInfo.keyProj[curInputIndex],
            hashInfo.isKeyColVarChar[curInputIndex]);

        statIndex = nextHashKey % LhxSubPartCount;

        if (!isAggregate) {
            if (hashInfo.useJoinFilter[curInputIndex]) {
                /*
                 * Use input filter if there exists one
                 */
                if (isBuildChildPart(childPartIndex)) {
                    /*
                     * Build input.  Note top level build input does not have
                     * input filter.  Use joinfilter from the probe input of
                     * the previous level.
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
                        } else {
                            filteredRowCountList[childPartIndex]++;
                        }
                    }
                } else {
                    /*
                     * Probe input.
                     * Use join filter from build input of the same
                     * partitioning level.
                     */
                    if (joinFilterList[getBuildChildPart(childPartIndex)] &&
                        joinFilterList[getBuildChildPart(childPartIndex)]->
                        test(hashKey % filterSize)) {
                        writeToPartition = true;
                    } else {
                        filteredRowCountList[childPartIndex]++;
                    }
                }
            } else {
                /*
                 * Not using filter.
                 */
                writeToPartition = true;
            }

            if (writeToPartition) {
                writerList[childPartIndex]->marshalTuple(inputTuple);
                curSubPartStat = subPartStatList[childPartIndex];
                curSubPartStat[statIndex]++;

                /*
                 * Set output filter to be used by the other input.
                 * Note: if the filter is used on the next level, "the other
                 * input" could be the same if join sides are switched.
                 */
                if (!joinFilterList[childPartIndex]) {
                    /*
                     * Filter not allocated yet.
                     */
                    joinFilterList[childPartIndex].reset(
                        new dynamic_bitset<>(filterSize));
                }
                joinFilterList[childPartIndex]->set(hashKey % filterSize);
            }
        } else {
            writerList[childPartIndex]->aggAndMarshalTuple(inputTuple);
            (subPartStatList[childPartIndex])[statIndex]++;
        }

        reader->consumeTuple();
    }
}

void LhxPlan::createChildren(LhxHashInfo const &hashInfo,
    bool enableSubPartStat)
{
    LhxHashGenerator hashGen;
    hashGen.init(partitionLevel + 1);

    uint numInputs = hashInfo.inputDesc.size();

    vector<SharedLhxPartition> destPartitionList(LhxChildPartCount * numInputs);

    LhxPartitionReader reader;
    LhxPartitionWriter writerList[LhxChildPartCount];
    uint childNum, i, j;
    TupleData outputTuple;

    /*
     * Generate partitions for each input.
     */
    for (j = 0; j < numInputs; j ++) {
        reader.open(partitions[j], hashInfo);
        outputTuple.compute(hashInfo.inputDesc[j]);

        for (i = 0; i < LhxChildPartCount; i ++) {
            uint index = j * LhxChildPartCount + i;
            destPartitionList[index].reset(
                new LhxPartition(partitions[j]->pExecStream));
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

        /*
         * Partial aggregate hash tables used inside the writers(one HT
         * for each writer) share the same scratch buffer space.
         * Release the buffer pages used by these hash tables at the end,
         * after all writers have been closed(so that there will be no more
         * scratch page alloc calls).
         */
        for (i = 0; i < LhxChildPartCount; i ++) {
            writerList[i].releaseResources();
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
            WeakLhxPlan(shared_from_this()),
            partitionLevel + 1,
            partitionList,
            enableSubPartStat);

        newChildPlan->addSibling(firstChildPlan);
        firstChildPlan = newChildPlan;
    }
}

void LhxPlan::createChildren(LhxPartitionInfo &partInfo,
    bool enableSubPartStat, bool enableSwing)
{
    uint i, j;

    for (i = 0; i < LhxChildPartCount; i ++) {
        SharedLhxPlan newChildPlan = SharedLhxPlan(new LhxPlan());
        vector<SharedLhxPartition> partitionList;
        vector<shared_array<uint> > subPartStats;
        vector<uint> filteredRows;
        for (j = 0; j < partInfo.numInputs; j ++) {
            partitionList.push_back(
                partInfo.destPartitionList[i + LhxChildPartCount * j]);
            subPartStats.push_back(
                partInfo.subPartStatList[i + LhxChildPartCount * j]);
            filteredRows.push_back(
                partInfo.filteredRowCountList[i + LhxChildPartCount * j]);
        }
        newChildPlan->init(
            WeakLhxPlan(shared_from_this()),
            partitionLevel + 1,
            partitionList,
            subPartStats,
            partInfo.joinFilterList[getProbeChildPart(i)],
            filteredRows,
            enableSubPartStat,
            enableSwing);

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
        WeakLhxPlan parent = this->parentPlan;
        SharedLhxPlan shared_parent = parent.lock();

        if (shared_parent) {
            return shared_parent->getNextLeaf();
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
              << "[       parent     = " << parentPlan.lock().get()  << "]\n"
              << "[       firstChild = " << firstChildPlan.get() << "]\n"
              << "[       sibling    = " << siblingPlan.get()    << "]\n";

    /*
     * Print out the partitions.
     */
    planTrace << "[joinFilter = ";
    if (joinFilter) {
        planTrace << joinFilter.get();
    }
    planTrace << "]\n";

    for (uint i = 0; i < partitions.size(); i ++) {
        planTrace << "[Partition(" << i << ")]\n"
                  << "[       inputIndex     = " << partitions[i]->inputIndex << "]\n"
                  << "[       join side      = " << getJoinSide(partitions[i]->inputIndex) << "]\n"
                  << "[       filteredRows   = " << filteredRowCount[i] << "]\n"
                  << "[       inputSize      = " << inputSize[i] << "]\n";
        planTrace << "[       childPartSize  = ";
        if (childPartSize.size() > i) {
            shared_array<uint> oneChildPartSize = childPartSize[i];
            if (oneChildPartSize) {
                for (uint j = 0; j < LhxChildPartCount; j ++) {
                    planTrace << oneChildPartSize[j] << " ";
                }
            }
        }
        planTrace << "]\n";
    }

    planTrace << "[subPartToChildMap = ";
    if (subPartToChildMap) {
        for (uint i = 0; i < LhxSubPartCount; i ++) {
            planTrace << subPartToChildMap[i] << " ";
        }
    }
    planTrace << "]\n";

    SharedLhxPlan childPlan = firstChildPlan;

    while (childPlan) {
        planTrace << childPlan->toString();
        childPlan = childPlan->siblingPlan;
    }

    return planTrace.str();
}

FENNEL_END_CPPFILE("$Id$");

// End LhxPartition.cpp

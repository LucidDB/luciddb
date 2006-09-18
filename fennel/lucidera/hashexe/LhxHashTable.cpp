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
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/tuple/TuplePrinter.h"
#include <sstream>

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxHashDataAccessor::init(TupleDescriptor const &inputDataDesc)
{
    dataDescriptor = inputDataDesc;
    dataTuple.compute(dataDescriptor);
    dataAccessor.compute(dataDescriptor);
}

void LhxHashDataAccessor::unpack(
    TupleData &outputTuple,
    TupleProjection &destProj)
{
    PBuffer buf = getBuffer();

    assert (buf != NULL);

    if (destProj.size() > 0) {
        // REVIEW jvs 25-Aug-2006:  It looks like there's a potential
        // for unmarshalling unneeded fields here.  If there could
        // be a lot of those, set up a TupleProjectionAccessor
        // and use that (over and over) instead.

        /*
         * Destination positions in the outputTuple should be enough to hold
         * fields returned by dataAccessor.
         */
        uint tupleSize = min(destProj.size(), dataTuple.size());

        /*
         * Set pointers in the tmp tuple, and then pass them on to fields in
         * output tuple.
         */
        dataAccessor.unmarshal(dataTuple);

        for (int i = 0; i < tupleSize; i ++) {
            outputTuple[destProj[i]].copyFrom(dataTuple[i]);
        }
    } else {
        /*
         * Set pointers in the outputTuple.
         */
        dataAccessor.unmarshal(outputTuple);        
    }
}

string LhxHashDataAccessor::toString()
{
    TuplePrinter tuplePrinter;
    ostringstream dataTrace;
    TupleProjection allFields;
    allFields.clear();

    unpack(dataTuple, allFields);
    dataTrace << "[Data Node] ";
    tuplePrinter.print(dataTrace, dataDescriptor, dataTuple);
    return dataTrace.str();
}

LhxHashKeyAccessor::LhxHashKeyAccessor()
    : LhxHashNodeAccessor(
        sizeof(PBuffer) + sizeof(uint8_t) + sizeof(PBuffer *))
{
    firstDataOffset = 0;
    /*
     * firstData pointer is of type PBuffer
     */
    isMatchedOffset = firstDataOffset + sizeof(PBuffer);
    /*
     * isMatched indicator is of type uint8_t
     */
    nextSlotOffset = isMatchedOffset + sizeof(uint8_t);
}

void LhxHashKeyAccessor::init(
    TupleDescriptor const &keyDescInit,
    TupleProjection const &keyColsProjInit,
    TupleProjection const &aggsProjInit)
{
    keyDescriptor = keyDescInit;
    keyTuple.compute(keyDescriptor);
    keyAccessor.compute(keyDescriptor);

    keyColsProj = keyColsProjInit;
    aggsProj = aggsProjInit;

    keyColsDesc.projectFrom(keyDescriptor, keyColsProj);
}

void LhxHashKeyAccessor::addData(PBuffer inputData)
{
    PBuffer firstDataNode = getFirstData();
    /*
     * The original first data node becomes the next.
     */
    firstData.setNext(inputData, firstDataNode);
    setFirstData(inputData);
}

void LhxHashKeyAccessor::unpack(
    TupleData &outputTuple,
    TupleProjection &destProj)
{
    PBuffer buf = getBuffer();

    assert (buf != NULL);

    if (destProj.size() > 0) {
        /*
         * Destination positions in the outputTuple should be enough to hold
         * fields returned by dataAccessor.
         */
        uint tupleSize = min(destProj.size(), keyTuple.size());

        /*
         * Set pointers in the tmp tuple, and then pass them on to fields in
         * output tuple.
         */
        keyAccessor.unmarshal(keyTuple);

        for (int i = 0; i < tupleSize; i ++) {
            outputTuple[destProj[i]].copyFrom(keyTuple[i]);
        }
    } else {
        /*
         * Set pointers in the outputtuple.
         */
        keyAccessor.unmarshal(outputTuple);        
    }
}

bool LhxHashKeyAccessor::matches(
    TupleData const &inputTuple,
    TupleProjection const &inputKeyProj)
{
    assert(inputKeyProj.size() == keyColsProj.size());

    inputKey.projectFrom(inputTuple, inputKeyProj);
    
    keyAccessor.unmarshal(keyTuple);

    currentKey.projectFrom(keyTuple, keyColsProj);

    return (keyColsDesc.compareTuples(keyTuple, keyColsProj,
            inputTuple, inputKeyProj) == 0);
}

string LhxHashKeyAccessor::toString()
{
    TuplePrinter tuplePrinter;
    ostringstream keyTrace;
    TupleProjection allFields;
    allFields.clear();

    keyTuple.compute(keyDescriptor);
    unpack(keyTuple, allFields);
    keyTrace << "[Key Node] ["
             << (isMatched() ? "matched" : "unmatched")
             << " next " << getNextSlot() << "] ";
    tuplePrinter.print(keyTrace, keyDescriptor, keyTuple);
    return keyTrace.str();
}

void LhxHashBlockAccessor::init(uint usablePageSize)
{
    blockUsableSize = usablePageSize - getBufferOffset();
    numSlotsPerBlock = blockUsableSize / sizeof(PBuffer);
}

void LhxHashBlockAccessor::setCurrent(PBuffer blockPtrInit, bool valid,
    bool clearContent) 
{
    LhxHashNodeAccessor::setCurrent(blockPtrInit);
    freePtr = getBuffer();
    assert(freePtr);
    endPtr = freePtr + blockUsableSize;

    if (valid) {
        freePtr = endPtr;
    } else if (clearContent) {
        /*
         * Clear the memory if it is not valid.
         * The next link is not reset however since this block can be part of a
         * reusable block list.
         */
        memset(freePtr, 0, blockUsableSize);
    }

}

PBuffer LhxHashBlockAccessor::allocBuffer(uint bufSize)
{
    PBuffer resultPtr = freePtr;
    
    if (freePtr + bufSize > endPtr) {
        resultPtr = NULL;
    } else {
        freePtr += bufSize;
    }
    return resultPtr;
}

PBuffer *LhxHashBlockAccessor::getSlot(uint slotNum)
{
    assert (getCurrent() != NULL);
    if (slotNum >= numSlotsPerBlock) {
        /*
         * slotNum starts from 0.
         */
        return NULL;
    } else {
        return (PBuffer *)(getBuffer() + slotNum * sizeof(PBuffer));
    }
}

void LhxHashTable::init(
    uint partitionLevelInit,
    LhxHashInfo const &hashInfo,
    uint buildInputIndex)
{
    maxBlockCount = hashInfo.numCachePages;
    assert (maxBlockCount > 1);
    scratchAccessor = hashInfo.memSegmentAccessor;
    partitionLevel = partitionLevelInit;
    bufferLock.accessSegment(scratchAccessor);
    currentBlockCount = 0;

    /*
     * Recompute num slots based on hashInfo.numCachePages
     */
    RecordNum cndKeys = hashInfo.cndKeys[buildInputIndex];
    uint usablePageSize = scratchAccessor.pSegment->getUsablePageSize();
    
    calculateNumSlots(cndKeys, usablePageSize, maxBlockCount);    

    /*
     * special hash table properties.
     */
    filterNull = hashInfo.filterNull[buildInputIndex];
    removeDuplicate = hashInfo.removeDuplicate[buildInputIndex];

    blockAccessor.init(usablePageSize);
    nodeBlockAccessor.init(usablePageSize);
    maxBufferSize = nodeBlockAccessor.getUsableSize();

    hashGen.init(partitionLevel);
    hashGenSub.init(partitionLevel+1);

    uint i;

    /*
     * The last input is the build side.
     */
    TupleDescriptor const &buildTupleDesc = hashInfo.inputDesc[buildInputIndex];
    keyColsProj = hashInfo.keyProj[buildInputIndex];

    /*
     * Initialize varchar type indicator for the build side. (Assumed to be the
     * last input.)
     */
    isKeyColVarChar = hashInfo.isKeyColVarChar[buildInputIndex];
    aggsProj = hashInfo.aggsProj;
    dataProj = hashInfo.dataProj[buildInputIndex];

    isGroupBy = false;

    /*
     * These steps initialize the keyColsProjInKey and aggsProjInKey which are
     * based on the new keyColsAndAggs tuple.
     */
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;
    TupleProjection keyColsProjInKey;
    TupleProjection aggsProjInKey;
    
    uint keyCount = keyColsProj.size();
    for (i = 0; i < keyCount; i++) {
        keyDesc.push_back(buildTupleDesc[keyColsProj[i]]);
        keyColsProjInKey.push_back(i);
    }

    keyColsAndAggsProj = keyColsProj;
    for (i = 0; i < aggsProj.size(); i++) {
        keyColsAndAggsProj.push_back(aggsProj[i]);
        keyDesc.push_back(buildTupleDesc[aggsProj[i]]);
        aggsProjInKey.push_back(i + keyCount);
    }

    hashKeyAccessor.init(keyDesc, keyColsProjInKey, aggsProjInKey);

    for (i = 0; i < dataProj.size(); i++) {
        dataDesc.push_back(buildTupleDesc[dataProj[i]]);
    }

    hashDataAccessor.init(dataDesc);
}

void LhxHashTable::init(
    uint partitionLevelInit,
    LhxHashInfo const &hashInfo,
    AggComputerList *aggList,
    uint buildInputIndex)
{
    init(partitionLevelInit, hashInfo, buildInputIndex);

    aggComputers = aggList;
    /*
     * The last input is the build side. In the group by case, there is only
     * one input.
     */
    aggWorkingTuple.compute(hashInfo.inputDesc[buildInputIndex]);
    aggResultTuple.computeAndAllocate(hashInfo.inputDesc[buildInputIndex]);

    isGroupBy = true;
    
    if (aggList->size() > 0) {
        hasAggregates = true;
    } else {
        hasAggregates = false;
    }
}

PBuffer LhxHashTable::allocBlock()
{
    PBuffer resultBlock;
    
    if (currentBlockCount < maxBlockCount) {
        currentBlockCount ++;
        /*
         * Allocate a new block.
         */
        bufferLock.allocatePage();
        resultBlock = bufferLock.getPage().getWritableData();
        bufferLock.unlock();
        
        /*
         * The new block is not linked in yet.
         */
        blockAccessor.setCurrent(resultBlock, false, false);
        blockAccessor.setNext(NULL);
    } else {
        /*
         * Hash Table reached its maximum size.
         */
        resultBlock = NULL;
    }
    return resultBlock;
}

PBuffer LhxHashTable::allocBuffer(uint bufSize)
{
    PBuffer resultBuf = nodeBlockAccessor.allocBuffer(bufSize);
    
    if (!resultBuf) {
        /*
         * Current block out of memory
         */
        PBuffer nextBlock = nodeBlockAccessor.getNext();
        if (nextBlock) {
            currentBlock = nextBlock;
        } else {
            PBuffer newBlock = allocBlock();
            nodeBlockAccessor.setNext(newBlock);
            currentBlock = newBlock;
        }
        
        if (currentBlock) {
            nodeBlockAccessor.setCurrent(currentBlock, false, false);
            resultBuf = nodeBlockAccessor.allocBuffer(bufSize);
            
            assert (resultBuf);
        }
    }
    
    return resultBuf;
}     

bool LhxHashTable::allocateResources(bool reuse)
{
    assert (numSlots != 0);

    PBuffer newBlock;

    slotBlocks.clear();
    firstSlot = NULL;
    lastSlot = NULL;

    if (!reuse) {
        firstBlock = allocBlock();
    }

    currentBlock = firstBlock;
    
    /*
     * Should be able to allocate at least one block.
     */
    assert (currentBlock != NULL);

    uint numSlotsPerBlock = blockAccessor.getSlotsPerBlock();

    /*
     * Initialize the block (clear all bytes etc).
     */
    nodeBlockAccessor.setCurrent(currentBlock, false, true);
    slotBlocks.push_back(currentBlock);
            
    if (numSlots <= numSlotsPerBlock) {
        /*
         * This will be the first "node block", i.e. it contains key or
         * data nodes.
         * The allocate call sets the freePtr of the currentBlock
         * correctly.
         */
        nodeBlockAccessor.allocSlots(numSlots);
        return true;
    }

    /*
     * Need to allocate more than one block.
     */
    int numSlotsToAlloc = numSlots - numSlotsPerBlock;

    while (numSlotsToAlloc > 0) {
        newBlock = NULL;
        if (reuse) {
            newBlock = nodeBlockAccessor.getNext();
        }

        if (!newBlock) {
            newBlock = allocBlock();
            if (!newBlock) {
                return false;
            }
        }
            
        /*
         * New block is linked to the end of the allocated block list.
         */
        nodeBlockAccessor.setNext(newBlock);
        currentBlock = newBlock;
        nodeBlockAccessor.setCurrent(currentBlock, false, true);
        slotBlocks.push_back(currentBlock);

        if (numSlotsToAlloc <= numSlotsPerBlock) {
            /*
             * This will be the first "node block", i.e. it contains key or
             * data nodes.
             * The allocate call sets the freePtr of the currentBlock
             * correctly.
             */
            nodeBlockAccessor.allocSlots(numSlotsToAlloc);
        }

        numSlotsToAlloc -= numSlotsPerBlock;
    }
    return true;
}

void LhxHashTable::releaseResources(bool reuse)
{
    /*
     * Note: User of hash table needs to supply it with a private
     * scratchAccessor; otherwise, this call here can deallocate pages from
     * other clients of the shared scratchAccessor.
     */
    if (!reuse && scratchAccessor.pSegment) {
        scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID,
            NULL_PAGE_ID);
        firstBlock = NULL;
        currentBlockCount = 0;
    }

    hashKeyAccessor.reset();
    hashDataAccessor.reset();
    blockAccessor.reset();
    nodeBlockAccessor.reset();
    currentBlock = NULL;
}

void LhxHashTable::calculateNumSlots(
    RecordNum cndKeys,
    uint usablePageSize,
    BlockNum numBlocks)
{
    // if we don't have stats for the number of distinct keys, just
    // use a default value
    if (isMAXU(cndKeys)) {
        cndKeys = RecordNum(10000);
    }

    /*
     * Use at least 1%, but no more than 10% of hash table cache pages to store
     * slots.
     */
    uint slotsLow = numBlocks * usablePageSize / sizeof(PBuffer) / 100;
    uint slotsHigh = numBlocks * usablePageSize / sizeof(PBuffer) / 10;

    numSlots =
        max(slotsNeeded(cndKeys), slotsLow);

    numSlots = min(numSlots, slotsHigh);    
}

void LhxHashTable::calculateSize(
    LhxHashInfo const &hashInfo,
    uint inputIndex,
    BlockNum &numBlocks)
{
    uint usablePageSize = 
        (hashInfo.memSegmentAccessor.pSegment)->getUsablePageSize()
        - sizeof(PBuffer);

    // REVIEW jvs 25-Aug-2006:  Why is it necessary to cast
    // away const here?  You should be able to just declare const references.
    
    TupleDescriptor &inputDesc  =
        (TupleDescriptor &)hashInfo.inputDesc[inputIndex];

    TupleProjection &keyProj  =
        (TupleProjection &)hashInfo.keyProj[inputIndex];

    TupleProjection &dataProj  =
        (TupleProjection &)hashInfo.dataProj[inputIndex];

    RecordNum cndKeys = hashInfo.cndKeys[inputIndex];
    RecordNum numRows = hashInfo.numRows[inputIndex];
    // if we don't have stats, don't bother trying to compute the hash table
    // size
    if (isMAXU(cndKeys) || isMAXU(numRows)) {
        numBlocks = MAXU;
        return;
    }

    TupleDescriptor keyDesc;
    keyDesc.projectFrom(inputDesc, keyProj);
    
    TupleDescriptor dataDesc;
    dataDesc.projectFrom(inputDesc, dataProj);

    LhxHashKeyAccessor tmpKey;
    LhxHashDataAccessor tmpData;

    TupleProjection tmpKeyProj;
    TupleProjection tmpAggsProj;

    /*
     * When estimating hash table size, ignore aggregate fields.
     */
    for (int i = 0; i < keyDesc.size(); i ++) {
        tmpKeyProj.push_back(i);
    }

    tmpKey.init(keyDesc, tmpKeyProj, tmpAggsProj);
    tmpData.init(dataDesc);

    double totalBytes = 
        slotsNeeded(cndKeys) * sizeof(PBuffer)
        + cndKeys * tmpKey.getAvgStorageSize()
        + numRows * tmpData.getAvgStorageSize();
    double nBlocks = ceil(totalBytes / usablePageSize);
    if (nBlocks >= BlockNum(MAXU)) {
        numBlocks = BlockNum(MAXU) - 1;
    } else {
        numBlocks = BlockNum(nBlocks);
    }
}


PBuffer *LhxHashTable::getSlot(uint slotNum)
{
    PBuffer *slot;
    uint slotsPerBlock = blockAccessor.getSlotsPerBlock();

    blockAccessor.setCurrent(slotBlocks[slotNum/slotsPerBlock], true, false);
    
    slot = blockAccessor.getSlot(slotNum%slotsPerBlock);

    assert (slot);

    return slot;
}

PBuffer LhxHashTable::findKeyLocation(
    TupleData const &inputTuple,
    TupleProjection const &inputKeyProj,
    bool isProbing,
    bool removeDuplicateProbe)
{
    uint slotNum =
        (hashGen.hash(inputTuple, inputKeyProj, isKeyColVarChar)) % numSlots;
    
    PBuffer *slot = getSlot(slotNum);
    PBuffer keyLocation = (PBuffer)slot;
    PBuffer firstKey = *slot;
    PBuffer nextKey;
    
    if (firstKey) {
        /*
         * Keep searching if the key has already been linked to keys in the
         * same slot.
         */
        hashKeyAccessor.setCurrent(firstKey, true);
        while (!hashKeyAccessor.matches(inputTuple, inputKeyProj)) {
            nextKey = hashKeyAccessor.getNext();
            if (!nextKey) {
                return NULL;
            }

            keyLocation = hashKeyAccessor.getNextLocation();
            hashKeyAccessor.setCurrent(nextKey, true);
        }
    } else {
        return NULL;
    }

    /*
     * Found a matching key
     */
    if (removeDuplicateProbe && hashKeyAccessor.isMatched()) {
        return NULL;
    }

    if (isProbing) {
        hashKeyAccessor.setMatched(true);
    }

    return keyLocation;
}

bool LhxHashTable::addKeyData(TupleData const &inputTuple)
{
    // REVIEW jvs 25-Aug-2006:  If we're not using a power of two to allow
    // for fast modulo, then it should probably be a prime number to
    // reduce collisions.  Broadbase had a table "BBPrime" which
    // allowed it to quickly find the closest prime number after doing
    // other calculations like resource estimation.
    uint slotNum =
        (hashGen.hash(inputTuple, keyColsProj, isKeyColVarChar)) % numSlots;
    
    PBuffer *slot = getSlot(slotNum);
    PBuffer *newLastSlot = NULL;

    if (!firstSlot) {
        firstSlot = slot;
        lastSlot  = slot;
    } else {
        if (!(*slot)) {
            // first time inserting into this slot
            // need to chain the slot in if insertion successful
            newLastSlot = slot;
        }
    }

    PBuffer newNextKey = *slot;

    PBuffer newKey = NULL;

    if (!isGroupBy) {
        tmpKeyTuple.projectFrom(inputTuple, keyColsProj);
        hashKeyAccessor.checkStorageSize(tmpKeyTuple, maxBufferSize);
        uint newKeyLen =
            hashKeyAccessor.getStorageSize(tmpKeyTuple);
        newKey = allocBuffer(newKeyLen);
    } else {
        aggResultTuple.resetBuffer();
        for (int i = 0; i < keyColsProj.size() ; i ++) {
            aggResultTuple[i].copyFrom(inputTuple[keyColsProj[i]]);
        }

        for (int i = 0; i < aggComputers->size(); i ++) {
            (*aggComputers)[i].initAccumulator(
                aggResultTuple[aggsProj[i]], inputTuple);
        }
        hashKeyAccessor.checkStorageSize(aggResultTuple, maxBufferSize);
        newKey =
            allocBuffer(hashKeyAccessor.getStorageSize(aggResultTuple));
    }

    PBuffer newData = NULL;

    if (!isGroupBy) {
        /*
         * Tuple contains data portion. i.e. this is not a group by case.
         */
        tmpDataTuple.projectFrom(inputTuple, dataProj);
        hashDataAccessor.checkStorageSize(tmpDataTuple, maxBufferSize);
        uint newDataLen = hashDataAccessor.getStorageSize(tmpDataTuple);
        newData = allocBuffer(newDataLen);
    }

    if (!newKey || (!isGroupBy && !newData)) {
        /*
         * Ran out of memory.
         */
        return false;
    }

    PBuffer *nextSlot = NULL;

    if (newNextKey) {
        // if slot not empty
        // copy the nextSlot field from newNextKey to newKey
        hashKeyAccessor.setCurrent(newNextKey, true);
        nextSlot = hashKeyAccessor.getNextSlot();
        hashKeyAccessor.setNextSlot(NULL);
    }

    *slot = newKey;
    hashKeyAccessor.setCurrent(newKey, false);
    hashKeyAccessor.setMatched(false);
    hashKeyAccessor.setNext(newNextKey);
    hashKeyAccessor.setNextSlot(nextSlot);
    hashKeyAccessor.setFirstData(NULL);

    if (!isGroupBy) {
        /*
         * Store the key.
         */
        hashKeyAccessor.pack(tmpKeyTuple);

        /*
         * Add data portion to this key.
         */
        hashKeyAccessor.setCurrent(newKey, true);
        hashDataAccessor.setCurrent(newData, false);
        hashDataAccessor.pack(tmpDataTuple);
        hashKeyAccessor.addData(newData);
    } else {
        /*
         * Store the key and the aggs.
         */
        hashKeyAccessor.pack(aggResultTuple);
    }


    /*
     * Link this slot (if inserted to for the first time) into the linked list.
     */
    if (newLastSlot) {
        hashKeyAccessor.setCurrent((*lastSlot), true);
        hashKeyAccessor.setNextSlot(newLastSlot);
        lastSlot = newLastSlot;
    }

    return true;
}

bool LhxHashTable::addData(PBuffer keyNode, TupleData const &inputTuple)
{
    /*
     * REVIEW: optimization possible here if dataProj is empty; i.e. key
     * contains all cols. We can keep a count in the key, instead of storing
     * empty data nodes following the key. See test case
     * LhxHashTableTest::testInsert1Ka().
     * Another case is to support setop ALL in future.
     */
    hashKeyAccessor.setCurrent(keyNode, true);

    tmpDataTuple.projectFrom(inputTuple, dataProj);

    hashDataAccessor.checkStorageSize(tmpDataTuple, maxBufferSize);

    uint newDataLen =
        hashDataAccessor.getStorageSize(tmpDataTuple);
    PBuffer newData = allocBuffer(newDataLen);

    if (!newData) {
        /*
         * Hash table out of memory.
         */
        return false;
    }

    hashDataAccessor.setCurrent(newData, false);
    hashDataAccessor.pack(tmpDataTuple);
    hashKeyAccessor.addData(newData);
    return true;
}

bool LhxHashTable::aggData(PBuffer destKeyLoc, TupleData const &inputTuple)
{
    PBuffer destKey;
    /*
     * Need to copy destKey out as destKeyLoc might not be aligned.
     */
    memcpy((PBuffer)&destKey, destKeyLoc, sizeof(PBuffer));

    hashKeyAccessor.setCurrent(destKey, true);

    aggResultTuple.resetBuffer();

    hashKeyAccessor.unpack(aggWorkingTuple, keyColsAndAggsProj);

    for (int i = 0; i < keyColsProj.size() ; i ++) {
        aggResultTuple[i].copyFrom(inputTuple[keyColsProj[i]]);
    }

    for (int i = 0; i < aggComputers->size(); i ++) {
        (*aggComputers)[i].updateAccumulator(
            aggWorkingTuple[aggsProj[i]],
            aggResultTuple[aggsProj[i]],
            inputTuple);
    }
    
    hashKeyAccessor.checkStorageSize(aggResultTuple, maxBufferSize);

    uint newResultSize =
        hashKeyAccessor.getStorageSize(aggResultTuple);

    uint oldResultSize =
        hashKeyAccessor.getStorageSize(aggWorkingTuple);

    if (newResultSize > oldResultSize) {
        PBuffer newKey = NULL;
        PBuffer newNextKey = hashKeyAccessor.getNext();

        /*
         * The key buffer will not hold the new result. Need to allocate buffer
         * again.
         */
        newKey = allocBuffer(newResultSize);
    
        if (newKey) {
            /*
             * Save the current key's next slot so we can set it in the new
             * key
             */
            PBuffer *nextSlot = hashKeyAccessor.getNextSlot();

            /*
             * The old key buffer is not used any more. Write in the key
             * location the new key buffer.
             */
            memcpy(destKeyLoc, (PBuffer)&newKey, sizeof(PBuffer));

            hashKeyAccessor.setCurrent(newKey, false);
            hashKeyAccessor.setMatched(false);
            hashKeyAccessor.setNext(newNextKey);
            hashKeyAccessor.pack(aggResultTuple);
            hashKeyAccessor.setNextSlot(nextSlot);
            return true;
        } else {
            return false;
        }
    } else {
        /*
         * The key buffer can hold aggResultTuple.
         */
        hashKeyAccessor.pack(aggResultTuple);
        return true;
    }
}

bool LhxHashTable::addTuple(TupleData const &inputTuple)
{
    if (filterNull && inputTuple.containsNull(keyColsProj)) {
        /*
         * When null values are filtered, and this tuple does
         * contain null in its key columns, do not add to hash
         * table.
         */
        return true;
    }

    /*
     * We are building the hash table.
     */
    bool isProbing = false;
    bool removeDuplicateProbe = false;
    PBuffer destKeyLoc =
        findKeyLocation(inputTuple, keyColsProj, isProbing,
            removeDuplicateProbe);

    if (!destKeyLoc) {
        /*
         * Key is not present in the hash table. Add both the key and the data.
         */
        return addKeyData(inputTuple);
    } else if (removeDuplicate) {
        /*
         * Do not add duplicate keys.
         */
        return true;
    } else {
        /*
         * Key is present in the hash table.
         * If hash join, add to the data list corresponding this key.
         * If hash aggregate, aggregate the new data.
         */
        if (!isGroupBy) {
            PBuffer destKey;
            /*
             * Need to copy destKey out as destKeyLoc might not be aligned.
             */
            memcpy((PBuffer*)&destKey, destKeyLoc, sizeof(PBuffer));
            
            assert (destKey);

            return addData(destKey, inputTuple);
        } else {
            if (!hasAggregates) {
                return true;
            }
            return aggData(destKeyLoc, inputTuple);
        }
    }
}

PBuffer LhxHashTable::findKey(
    TupleData const &inputTuple,
    TupleProjection const &inputKeyProj,
    bool removeDuplicateProbe)
{
    PBuffer destKey;
    PBuffer destKeyLoc;
    bool isProbing = true;
    destKeyLoc =
        findKeyLocation(inputTuple, inputKeyProj, isProbing,
            removeDuplicateProbe);
    
    if (destKeyLoc) {
        /*
         * Need to copy destKey out as destKeyLoc might not be aligned.
         */    
        memcpy((PBuffer)&destKey, destKeyLoc, sizeof(PBuffer));
        return destKey;
    } else {
        return NULL;
    }
}

string LhxHashTable::printSlot(uint slotNum)
{    
    ostringstream slotTrace;
    PBuffer *slot = getSlot(slotNum);

    slotTrace << "[Slot] [" << slotNum << "] [" << slot <<"]\n";
    
    /*
     * Print all keys in this slot.
     */
    PBuffer currentHashKey = *slot;
    while (currentHashKey) {
        hashKeyAccessor.setCurrent(currentHashKey, true);
        slotTrace << "     " << hashKeyAccessor.toString() << "\n";

        /*
         * Print all data with the same key.
         */
        PBuffer currentHashData = hashKeyAccessor.getFirstData();
        while (currentHashData) {
            hashDataAccessor.setCurrent(currentHashData, true);
            slotTrace << "          " << hashDataAccessor.toString() << "\n";
            /*
             * next data.
             */
            currentHashData = hashDataAccessor.getNext();
        }

        /*
         * next key.
         */
        currentHashKey = hashKeyAccessor.getNext();
    }
    return slotTrace.str();
}

string LhxHashTable::toString()
{
    ostringstream hashTableTrace;

    hashTableTrace << "\n"
        << "[Hash Table : maximum # blocks = " << maxBlockCount     << "]\n"
        << "[             current # blocks = " << currentBlockCount << "]\n"
        << "[             # slots          = " << numSlots          << "]\n"
        << "[             partition level  = " << partitionLevel    << "]\n"
        << "[             first slot       = " << firstSlot         << "]\n"
        << "[             last  slot       = " << lastSlot          << "]\n";

    for (int i = 0; i < numSlots; i ++) {
        hashTableTrace << printSlot(i);
    }

    return hashTableTrace.str();
}

bool LhxHashTableReader::advanceSlot()
{
    if (!boundKey) {
        curKey = NULL;

        if (!isPositioned) {
            curSlot = hashTable->getFirstSlot();
        } else {
            curSlot = hashTable->getNextSlot(curSlot);
        }

        if (curSlot && *curSlot) {
            curKey = *curSlot;
            if (returnUnMatched) {
                /*
                 * Look at all the keys in this slot.
                 */
                hashKeyAccessor.setCurrent(*curSlot, true);

                /*
                 * Only return unmatched keys
                 */
                while (hashKeyAccessor.isMatched()) {
                    curKey = hashKeyAccessor.getNext();
                    if (!curKey) {
                        curSlot = hashTable->getNextSlot(curSlot);
                        if (curSlot) {
                            curKey = *curSlot;
                        } else {
                            curKey = NULL;
                        }
                    }

                    if (curKey) {
                        hashKeyAccessor.setCurrent(curKey, true);
                    } else {
                        /*
                         * Reached the end of the slot list.
                         */
                        break;
                    }
                }
            }
        }

        if (!curKey) {
            /*
             * Cound not find any slot with fitting key.
             */
            return false;
        }
    }
    
    hashKeyAccessor.setCurrent(curKey, true);

    if (!isGroupBy) {
        curData = hashKeyAccessor.getFirstData();
        assert(curData);
        hashDataAccessor.setCurrent(curData, true);
    }

    return true;
}

bool LhxHashTableReader::advanceKey()
{
    while ((curKey = hashKeyAccessor.getNext())) {
        if (!returnUnMatched) {
            break;
        } else {
            hashKeyAccessor.setCurrent(curKey, true);
            if (!hashKeyAccessor.isMatched()) {
                break;
            }
        }
    }
    
    if (curKey) {
        hashKeyAccessor.setCurrent(curKey, true);
        if (!isGroupBy) {
            curData = hashKeyAccessor.getFirstData();
            assert(curData);
            hashDataAccessor.setCurrent(curData, true);
        }
        return true;
    } else {
        return false;
    }
}

bool LhxHashTableReader::advanceData()
{
    if (isGroupBy) {
        return false;
    }

    curData = hashDataAccessor.getNext();
    if (curData) {
        hashDataAccessor.setCurrent(curData, true);
        return true;
    } else {
        return false;
    }
}

void LhxHashTableReader::produceTuple(TupleData &outputTuple)
{
    hashKeyAccessor.unpack(outputTuple, keyColsAndAggsProj);
    if (!isGroupBy) {
        hashDataAccessor.unpack(outputTuple, dataProj);
    }
}

void LhxHashTableReader::init(
    LhxHashTable *hashTableInit,
    LhxHashInfo const &hashInfo,
    uint buildInputIndex)
{
    /*
     * The last input is the build side.
     */
    TupleDescriptor const &outputTupleDesc =
        hashInfo.inputDesc[buildInputIndex];
    TupleProjection const &keyColsProj = hashInfo.keyProj[buildInputIndex];
    TupleProjection const &aggsProj = hashInfo.aggsProj;

    dataProj = hashInfo.dataProj[buildInputIndex];

    /*
     * These steps initialize the keyColsProjInKey and aggsProjInKey which are
     * based on the new keyColsAndAggs tuple.
     */
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;
    TupleProjection keyColsProjInKey;
    TupleProjection aggsProjInKey;
    uint keyCount = keyColsProj.size();
    uint i;

    for (i = 0; i < keyCount; i++) {
        keyDesc.push_back(outputTupleDesc[keyColsProj[i]]);
        keyColsProjInKey.push_back(i);
    }
    
    keyColsAndAggsProj = keyColsProj;
    uint aggsProjSize = aggsProj.size();

    for (i = 0; i < aggsProjSize; i ++) {
        keyColsAndAggsProj.push_back(aggsProj[i]);
        keyDesc.push_back(outputTupleDesc[aggsProj[i]]);
        aggsProjInKey.push_back(i + keyCount);
    }

    hashKeyAccessor.init(keyDesc, keyColsProjInKey, aggsProjInKey);

    for (i = 0; i < dataProj.size(); i++) {
        dataDesc.push_back(outputTupleDesc[dataProj[i]]);
    }

    hashDataAccessor.init(dataDesc);

    hashTable = hashTableInit;
    isGroupBy = hashTable->isHashGroupBy();
 
    /*
     * By default, this reader is not associated with any key, i.e. it covers
     * the whole hash table.
     */
    bindKey(NULL);
}

bool LhxHashTableReader::getNext(TupleData &outputTuple)
{
    if (!isPositioned) {
        assert (!(boundKey && returnUnMatched));

        /*
         * Position at the first qualifying key of the first slot.
         */
        if (!advanceSlot()) {
            /*
             * Nothing to output.
             */
            return false;
        }
        produceTuple(outputTuple);
        isPositioned = true;
        return true;
    }

    if (advanceData()) {
        produceTuple(outputTuple);        
        return true;
    } else {
        if (boundKey) {
            /*
             * End of data for this key.
             */
            return false;
        } else {
            /*
             * Get the next key in the same slot.
             */
            if (advanceKey()) {
                produceTuple(outputTuple);
                return true;
            } else {
                /*
                 * End of list for keys in the same slot..
                 * Start the next slot.
                 */
                if (advanceSlot()) {
                    produceTuple(outputTuple);
                    return true;
                } else {
                    return false;
                }
            }
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LhxHashTable.cpp

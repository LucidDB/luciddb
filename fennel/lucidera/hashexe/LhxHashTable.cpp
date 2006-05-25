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
#include "fennel/common/FennelExcn.h"
#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/TupleOverflowExcn.h"
#include <sstream>

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxHashDataAccessor::init(TupleDescriptor const &inputDataDesc)
{
    dataDescriptor = inputDataDesc;
    dataTuple.compute(dataDescriptor);
    dataAccessor.compute(dataDescriptor);
}

uint LhxHashDataAccessor::getStorageSize(TupleData const &inputTuple)
{
    return dataAccessor.getByteCount(inputTuple) + getBufferOffset();
}

void LhxHashDataAccessor::checkStorageSize(TupleData const &inputTuple,
    uint maxBufferSize)
{
    uint storageSize = getStorageSize(inputTuple);

    if (storageSize > maxBufferSize) {
        throw TupleOverflowExcn(
            dataDescriptor,
            inputTuple,
            storageSize,
            maxBufferSize);
    }
}

void LhxHashDataAccessor::pack(
    TupleData const &inputTuple)
{
    PBuffer buf = getBuffer();

    assert(buf != NULL && inputTuple.size() == dataAccessor.size());

    /*
     * Copy the input tuple into the buffer associated with this accessor.
     */
    dataAccessor.marshal(inputTuple, buf);
}

void LhxHashDataAccessor::unpack(
    TupleData &outputTuple,
    TupleProjection &destProj)
{
    PBuffer buf = getBuffer();

    assert (buf != NULL);

    if (destProj.size() > 0) {
        /*
         * Destination positions in the outputTuple should be enough to hold fields
         * returned by dataAccessor.
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
         * Set pointers in the outputtuple.
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
    : LhxHashNodeAccessor(sizeof(PBuffer) + sizeof(uint8_t))
{
    firstDataOffset = 0;
    isMatchedOffset = firstDataOffset + sizeof(PBuffer);
}

void LhxHashKeyAccessor::init(
    TupleDescriptor const &keyDescInit,
    TupleProjection const &keyColsProjInit,
    TupleProjection const &aggsProjInit)
{
    keyDescriptor = keyDescInit;
    keyTuple.compute(keyDescriptor);
    keyAccessor.compute(keyDescriptor);

    keyColsProj   = keyColsProjInit;
    aggsProj      = aggsProjInit;

    keyColsDesc.projectFrom(keyDescriptor, keyColsProj);
}

void LhxHashKeyAccessor::addData(PBuffer inputData)
{
    PBuffer firstDataNode = getFirstData();
    if (firstDataNode) {
        /*
         * The original first data node becomes the next.
         */
        LhxHashDataAccessor firstData;
        firstData.setNext(inputData, firstDataNode);
    }
    setFirstData(inputData);
}

uint LhxHashKeyAccessor::getStorageSize(TupleData const &inputTuple)
{
    return keyAccessor.getByteCount(inputTuple) + getBufferOffset();
}

void LhxHashKeyAccessor::checkStorageSize(TupleData const &inputTuple,
    uint maxBufferSize)
{
    uint storageSize = getStorageSize(inputTuple);

    if (storageSize > maxBufferSize) {
        throw TupleOverflowExcn(
            keyDescriptor,
            inputTuple,
            storageSize,
            maxBufferSize);
    }
}

void LhxHashKeyAccessor::pack(TupleData const &inputTuple)
{
    PBuffer buf = getBuffer();

    assert(buf != NULL && inputTuple.size() == keyAccessor.size());

    /*
     * Copy the input tuple into the buffer associated with this accessor.
     */
     keyAccessor.marshal(inputTuple, buf);
}

void LhxHashKeyAccessor::unpack(
    TupleData &outputTuple,
    TupleProjection &destProj)
{
    PBuffer buf = getBuffer();

    assert (buf != NULL);

    if (destProj.size() > 0) {
        /*
         * Destination positions in the outputTuple should be enough to hold fields
         * returned by dataAccessor.
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

    TupleData inputKeyTuple;    
    inputKeyTuple.projectFrom(inputTuple, inputKeyProj);
    
    keyAccessor.unmarshal(keyTuple);

    TupleData currentKey;
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
             << (isMatched() ? "matched" : "unmatched") << "] ";
    tuplePrinter.print(keyTrace, keyDescriptor, keyTuple);
    return keyTrace.str();
}

void LhxHashBlockAccessor::init(uint usablePageSize)
{
    blockUsableSize = usablePageSize - getBufferOffset();
    numSlotsPerBlock = blockUsableSize / sizeof(PBuffer);
}

void LhxHashBlockAccessor::setCurrent(PBuffer blockPtrInit, bool valid) 
{
    LhxHashNodeAccessor::setCurrent(blockPtrInit);
    freePtr = getBuffer();
    assert(freePtr);
    endPtr = freePtr + blockUsableSize;
    if (!valid) {
        /*
         * Clear the memory if it is not valid.
         */
        memset(freePtr, 0, blockUsableSize);
    } else {
        freePtr = endPtr;
    }
}

PBuffer LhxHashBlockAccessor::allocBuffer(uint bufSize)
{
    PBuffer resultPtr = freePtr;
    
    if ( freePtr + bufSize> endPtr) {
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
    AggComputerList *aggList)
{
    LhxHashTable::init(partitionLevelInit, hashInfo);

    aggComputers = aggList;
    /*
     * The last input is the build side. In the group by case, there is only
     * one input.
     */
    aggWorkingTuple.compute(hashInfo.inputDesc.back());
    aggResultTuple.computeAndAllocate(hashInfo.inputDesc.back());

    isAggregate = true;
}

void LhxHashTable::init(
    uint partitionLevelInit,
    LhxHashInfo const &hashInfo)
{
    maxBlockCount = hashInfo.numCachePages;
    assert (maxBlockCount > 1);
    scratchAccessor = hashInfo.memSegmentAccessor;

    partitionLevel = partitionLevelInit;

    bufferLock.accessSegment(scratchAccessor);
    currentBlockCount = 0;

    uint usablePageSize = scratchAccessor.pSegment->getUsablePageSize();
    blockAccessor.init(usablePageSize);
    nodeBlockAccessor.init(usablePageSize);
    maxBufferSize = nodeBlockAccessor.getUsableSize();

    hashGen.init(partitionLevel);
    hashGenSub.init(partitionLevel+1);

    uint i;

    /*
     * The last input is the build side.
     */
    TupleDescriptor const &buildTupleDesc =
        hashInfo.inputDesc.back();
    keyColsProj = hashInfo.keyProj.back();

    /*
     * Initialize varchar type indicator for the build side. (Assumed to be the
     * last input.)
     */
    isKeyColVarChar = hashInfo.isKeyColVarChar.back();

    aggsProj = hashInfo.aggsProj;
    dataProj = hashInfo.dataProj;

    isAggregate = false;

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
        blockAccessor.setCurrent(resultBlock, false);
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
        PBuffer newBlock = allocBlock();
            
        if (newBlock) {
            /*
             * New block is linked to the end of the allocated block list.
             */
            nodeBlockAccessor.setNext(newBlock);
            currentBlock = newBlock;                
            nodeBlockAccessor.setCurrent(currentBlock, false);

            resultBuf = nodeBlockAccessor.allocBuffer(bufSize);

            assert (resultBuf);
        }
    }
    
    return resultBuf;
}        

bool LhxHashTable::allocateResources(uint numSlotsInit)
{
    numSlots = numSlotsInit;
    
    PBuffer newBlock;

    currentBlock = firstBlock = allocBlock();
    
    /*
     * Should be able to allocate at least one block.
     */
    assert (currentBlock != NULL);

    uint numSlotsPerBlock = blockAccessor.getSlotsPerBlock();

    /*
     * Initialize the block(clear all bytes etc).
     */
    nodeBlockAccessor.setCurrent(currentBlock, false);
            
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
     * Need to allocate more than one blocks.
     */
    int numSlotsToAlloc = numSlots - numSlotsPerBlock;

    while (numSlotsToAlloc > 0) {
            
        newBlock = allocBlock();

        if (!newBlock) {
            return false;
        }
            
        /*
         * New block is linked to the end of the allocated block list.
         */
        nodeBlockAccessor.setNext(newBlock);
        currentBlock = newBlock;
        nodeBlockAccessor.setCurrent(currentBlock, false);

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

void LhxHashTable::releaseResources()
{
    /*
     * Note: User of hash table needs to supply it with a private
     * scratchAccessor; otherwise, this call here can deallocate pages from
     * other clients of the shared scratchAccessor.
     */
    scratchAccessor.pSegment->deallocatePageRange(NULL_PAGE_ID, NULL_PAGE_ID);

    hashKeyAccessor.reset();
    hashDataAccessor.reset();
    blockAccessor.reset();
    nodeBlockAccessor.reset();
    currentBlock = firstBlock = NULL;
    currentBlockCount = 0;
}

uint LhxHashTable::blocksNeeded(
    double numRows,
    double cndKeys,
    TupleDescriptor &keyDesc,
    TupleDescriptor &dataDesc,
    uint usablePageSize)
{
    if (usablePageSize == 0) {
        usablePageSize = blockAccessor.getUsableSize();
    } else {
        usablePageSize -= sizeof(PBuffer);
    }

    LhxHashKeyAccessor tmpKey;
    LhxHashDataAccessor tmpData;

    TupleProjection keyColsProj;
    TupleProjection aggsProj;

    /*
     * Pretend there is no aggregate fields.
     */
    for (int i = 0; i < keyDesc.size(); i ++) {
        keyColsProj.push_back(i);
    }

    tmpKey.init(keyDesc, keyColsProj, aggsProj);
    tmpData.init(dataDesc);

    double totalBytes = 
        slotsNeeded(cndKeys) * sizeof(PBuffer)
        + cndKeys * tmpKey.getMaxStorageSize()
        + numRows * tmpData.getMaxStorageSize();
    return (uint)ceil(totalBytes / usablePageSize);
}

uint LhxHashTable::bytesNeeded(
    double numRows,
    double cndKeys,
    TupleDescriptor &keyDesc,
    TupleDescriptor &dataDesc)
{
    return 
        (blocksNeeded(numRows, cndKeys, keyDesc, dataDesc) 
            * blockAccessor.getUsableSize());
}

PBuffer *LhxHashTable::getSlot(uint slotNum)
{
    PBuffer nextBlock;
    PBuffer *slot;
    uint slotsPerBlock = blockAccessor.getSlotsPerBlock();

    blockAccessor.setCurrent(firstBlock, true);
    
    while (!(slot = blockAccessor.getSlot(slotNum))) {
        nextBlock = blockAccessor.getNext();
        assert(nextBlock != NULL);
        blockAccessor.setCurrent(nextBlock, true);
        slotNum -= slotsPerBlock;
    }

    return slot;
}

PBuffer LhxHashTable::findKeyLocation(
    TupleData const &inputTuple,
    TupleProjection const &keyColsProj,
    bool isProbing)
{
    uint slotNum =
        (hashGen.hash(inputTuple, keyColsProj, isKeyColVarChar)) % numSlots;
    
    PBuffer *slot = getSlot(slotNum);
    assert (slot);

    PBuffer keyLocation = (PBuffer)slot;
    PBuffer firstKey = *slot;
    PBuffer nextKey;
    
    if (firstKey) {
        /*
         * Keep searching if the key has already been linked to keys in the
         * same slot.
         */
        hashKeyAccessor.setCurrent(firstKey, true);
        while (!hashKeyAccessor.matches(inputTuple, keyColsProj)) {
            nextKey = hashKeyAccessor.getNext();
            keyLocation = hashKeyAccessor.getNextLocation();

            if (!nextKey) {
                return NULL;
            }
            hashKeyAccessor.setCurrent(nextKey, true);
        }
    } else {
        return NULL;
    }

    /*
     * Found a matching key
     */
    if (isProbing) {
        hashKeyAccessor.setMatched(true);
    }

    return keyLocation;
}

bool LhxHashTable::addKeyData(TupleData const &inputTuple)
{
    uint slotNum =
        (hashGen.hash(inputTuple, keyColsProj, isKeyColVarChar)) % numSlots;
    
    PBuffer *slot = getSlot(slotNum);
    assert (slot);
    PBuffer newNextKey = *slot;

    TupleData tmpKeyTuple;
    PBuffer newKey = NULL;

    if (!isAggregate) {
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

    TupleData tmpDataTuple;
    PBuffer newData = NULL;

    if (!isAggregate) {
        /*
         * Tuple contains data portion. i.e. this is not a group by case.
         */
        tmpDataTuple.projectFrom(inputTuple, dataProj);
        hashDataAccessor.checkStorageSize(tmpDataTuple, maxBufferSize);
        uint newDataLen = hashDataAccessor.getStorageSize(tmpDataTuple);
        newData = allocBuffer(newDataLen);
    }

    if (!newKey || (!isAggregate && !newData)) {
        /*
         * Ran out of memory.
         */
        return false;
    }

    *slot = newKey;
    hashKeyAccessor.setCurrent(newKey, false);
    hashKeyAccessor.setMatched(false);
    hashKeyAccessor.setNext(newNextKey);

    if (!isAggregate) {
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

    return true;
}

bool LhxHashTable::addData(PBuffer keyNode, TupleData const &inputTuple)
{
    /*
     * REVIEW: optimizatin possible here if dataProj is empty; i.e. key
     * contains all cols. We can keep a count in the key, instead of storing
     * empty data nodes following the key. See test case
     * LhxHashTableTest::testInsert1Ka().
     */
    hashKeyAccessor.setCurrent(keyNode, true);

    TupleData tmpDataTuple;
    
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
             * The old key buffer is not used any more. Write in the key
             * location the new key buffer.
             */
            memcpy(destKeyLoc, (PBuffer)&newKey, sizeof(PBuffer));

            hashKeyAccessor.setCurrent(newKey, false);
            hashKeyAccessor.setMatched(false);
            hashKeyAccessor.setNext(newNextKey);
            hashKeyAccessor.pack(aggResultTuple);
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
    /*
     * We are building the hash table.
     */
    bool isProbing = false;
    PBuffer destKeyLoc = findKeyLocation(inputTuple, keyColsProj, isProbing);

    if (!destKeyLoc) {
        /*
         * Key is not presetn in the hash table. Add both the key and the data.
         */
        return addKeyData(inputTuple);
    } else {
        /*
         * Key is present in the hash table.
         * If hash join, add to the data list corresponding this key.
         * If hash aggregate, aggregate the new data.
         */
        if (!isAggregate) {
            PBuffer destKey;
            /*
             * Need to copy destKey out as destKeyLoc might not be aligned.
             */
            memcpy((PBuffer*)&destKey, destKeyLoc, sizeof(PBuffer));
            
            assert (destKey);

            return addData(destKey, inputTuple);
        } else {
            return aggData(destKeyLoc, inputTuple);
        }
    }
}

string LhxHashTable::printSlot(uint slotNum)
{    
    ostringstream slotTrace;
    PBuffer *slot = getSlot(slotNum);
    assert (slot);

    slotTrace << "[Slot] [" << slotNum << "]\n";
    
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
        << "[             partition level  = " << partitionLevel    << "]\n";

    for (int i = 0; i < numSlots; i ++) {
        hashTableTrace << printSlot(i);
    }

    return hashTableTrace.str();
}

bool LhxHashTableReader::advanceSlot()
{
    if (!boundKey) {
        curKey = NULL;

        PBuffer *slot;        
        while (curSlot < hashTable->getNumSlots()) {
            slot = hashTable->getSlot(curSlot);
            curSlot ++;
            
            if (slot && (PBuffer)*slot) {
                if (returnUnMatched) {
                    hashKeyAccessor.setCurrent(*slot, true);
                    /*
                     * Only return unmatched keys
                     */
                    if (!hashKeyAccessor.isMatched()) {
                        curKey = *slot;
                        break;
                    }
                }
                else {
                    /*
                     * Return everything.
                     */
                    curKey = *slot;
                    break;
                }
            }
        }

        if (!curKey) {
            /*
             * cound not find any fitting slot.
             */
            return false;
        }
    }
    
    hashKeyAccessor.setCurrent(curKey, true);

    if (!isAggregate) {
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
        if (!isAggregate) {
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
    if (isAggregate) {
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
    if (!isAggregate) {
        hashDataAccessor.unpack(outputTuple, dataProj);
    }
}

void LhxHashTableReader::init(
    LhxHashTable *hashTableInit,
    LhxHashInfo const &hashInfo)
{
    /*
     * The last input is the build side.
     */
    TupleDescriptor const &outputTupleDesc =
        hashInfo.inputDesc.back();
    TupleProjection const &keyColsProj = hashInfo.keyProj.back();
    TupleProjection const &aggsProj = hashInfo.aggsProj;

    dataProj = hashInfo.dataProj;

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
    isAggregate = hashTable->isHashAggregate();
 
    /*
     * By default, this reader is not associated with any key, i.e. it covers
     * the whole hash table.
     */
    bindKey(NULL);
}

void LhxHashTableReader::bind(PBuffer key)
{
    boundKey = curKey = key;
    curSlot = 0;
    curData = NULL;
    isPositioned = false;
}

void LhxHashTableReader::bindKey(PBuffer key)
{
    returnUnMatched = false;
    bind(key);
}

void LhxHashTableReader::bindUnMatched()
{
    returnUnMatched = true;
    bind(NULL);
}

bool LhxHashTableReader::getNext(TupleData &outputTuple)
{
    if (!isPositioned) {
        assert (!(boundKey && returnUnMatched));

        /*
         * Position at the first key of the first slot.
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

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
#include <algorithm>
#include <sstream>
#include <set>
#include <map>

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

void LhxHashDataAccessor::init(TupleDescriptor const &inputDataDesc)
{
    dataDescriptor = inputDataDesc;
    dataTuple.compute(dataDescriptor);
    dataAccessor.compute(dataDescriptor);
}

uint LhxHashDataAccessor::getStorageSize(TupleData const *inputTuple)
{
    if (!inputTuple) {
        return (dataAccessor.getBufferByteCount(getBuffer())
            + getBufferOffset());
    } else {
        return (dataAccessor.getByteCount(*inputTuple)
            + getBufferOffset());
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
    touchedOffset = firstDataOffset + sizeof(PBuffer);
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

uint LhxHashKeyAccessor::getStorageSize(TupleData const *inputTuple)
{
    if (!inputTuple) {
        return (keyAccessor.getBufferByteCount(getBuffer())
            + getBufferOffset());
    } else {
        return (keyAccessor.getByteCount(*inputTuple) + getBufferOffset());
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
             << (isTouched() ? "touched" : "untouched") << "] ";
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
    SegmentAccessor const& scratchAccessorInit,
    uint maxBlockCountInit,
    uint partitionLevelInit,
    TupleDescriptor const &inputTupleDesc,
    TupleProjection keyColsProj,
    TupleProjection aggsProj,
    TupleProjection const &dataProj)
{
    scratchAccessor = scratchAccessorInit;
    assert (maxBlockCountInit >= 1);
    maxBlockCount = maxBlockCountInit;
    partitionLevel = partitionLevelInit;

    bufferLock.accessSegment(scratchAccessor);
    currentBlockCount = 0;

    uint usablePageSize = scratchAccessor.pSegment->getUsablePageSize();
    blockAccessor.init(usablePageSize);
    nodeBlockAccessor.init(usablePageSize);

    hashGen.init(partitionLevel);
    hashGenSub.init(partitionLevel+1);

    /*
     * These steps change the keyColsProj and aggsProj which are based on
     * original build row into indexes of the the new keyColsAndAggs tuple.
     * REVIEW: This might not be how agg layout in hash table looks like. The
     * aggs will have agg computers, stored within the hash table will be the
     * data buffers for these agg computers.
     */
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;
    uint keyCount = keyColsProj.size();
    uint i;

    for (i = 0; i < keyCount; i++) {
        keyDesc.push_back(inputTupleDesc[keyColsProj[i]]);
        keyColsProj[i] = i;
    }

    for (i = 0; i < aggsProj.size(); i++) {
        keyDesc.push_back(inputTupleDesc[aggsProj[i]]);
        aggsProj[i] = i + keyCount;
    }

    hashKeyAccessor.init(keyDesc, keyColsProj, aggsProj);

    for (int i = 0; i < dataProj.size(); i++) {
        dataDesc.push_back(inputTupleDesc[dataProj[i]]);
    }

    hashDataAccessor.init(dataDesc);
}

PBuffer LhxHashTable::allocBlock()
{
    PBuffer resultBlock = NULL;
    currentBlockCount ++;
    
    if (currentBlockCount > maxBlockCount) {
        /*
         * Return NULL. Keep blockCount as max.
         */
        currentBlockCount = maxBlockCount;

        ostringstream errMsg;
        errMsg << "Hash Table can not fit in memory:"
               << " current # blocks:" << currentBlockCount
               << " maximum # blocks:" << maxBlockCount;
        throw FennelExcn(errMsg.str());            
    } else {
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
     * Need to allocate mroe than one blocks.
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
}

uint LhxHashTable::blocksNeeded(
    double numRows,
    double cndKeys,
    TupleDescriptor &keyDesc,
    TupleDescriptor &dataDesc,
    uint usablePageSize)
{
    LhxHashKeyAccessor tmpKey;
    LhxHashDataAccessor tmpData;

    if (usablePageSize == 0) {
        usablePageSize = blockAccessor.getUsableSize();
    } else {
        usablePageSize -= sizeof(PBuffer);
    }

    double totalBytes = 
        slotsNeeded(cndKeys) * sizeof(PBuffer)
        + cndKeys * tmpKey.getMaxStorageSize(keyDesc)
        + numRows * tmpData.getMaxStorageSize(dataDesc);
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

PBuffer LhxHashTable::findKey(
    TupleData const &inputTuple,
    TupleProjection const &keyColsProj)
{
    uint slotNum = (hashGen.hash(inputTuple, keyColsProj)) % numSlots;
    
    PBuffer *slot = getSlot(slotNum);
    assert (slot);

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
            
            if (!nextKey) {
                return NULL;
            }
            hashKeyAccessor.setCurrent(nextKey, true);
        }
    } else {
        return NULL;
    }
    return (hashKeyAccessor.getCurrent());
}

PBuffer LhxHashTable::addKey(
    TupleData const &inputTuple,
    TupleProjection const &keyColsProj,
    TupleProjection const &aggsProj)
{
    uint slotNum = (hashGen.hash(inputTuple, keyColsProj)) % numSlots;
    
    PBuffer *slot = getSlot(slotNum);
    assert (slot);

    TupleProjection keyColsAndAggsProj = keyColsProj;
    
    for (int i = 0; i < aggsProj.size(); i ++) {
        keyColsAndAggsProj.push_back(aggsProj[i]);
    }

    TupleData tmpKeyTuple;
    
    tmpKeyTuple.projectFrom(inputTuple, keyColsAndAggsProj);

    PBuffer newNextKey = *slot;
    uint newKeyLen = hashKeyAccessor.getStorageSize(&tmpKeyTuple);
    PBuffer newKey = allocBuffer(newKeyLen);

    if (!newKey) {
        /*
         * Ran out of memory.
         */
        return NULL;
    }

    *slot = newKey;
    hashKeyAccessor.setCurrent(newKey, false);
    hashKeyAccessor.pack(tmpKeyTuple);
    hashKeyAccessor.setNext(newNextKey);
    return newKey;
}

bool LhxHashTable::addData(
    PBuffer keyNode,
    TupleData const &inputTuple,
    TupleProjection const &dataProj)
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

    uint newDataLen = hashDataAccessor.getStorageSize(&tmpDataTuple);
    PBuffer newData = allocBuffer(newDataLen);

    if (!newData) {
        /*
         * Ran out of memory.
         */
        return false;
    }

    hashDataAccessor.setCurrent(newData, false);
    hashDataAccessor.pack(tmpDataTuple);
    hashKeyAccessor.addData(newData);
    return true;
}

bool LhxHashTable::addTuple(
    TupleData const &inputTuple,
    TupleProjection const &keyColsProj,
    TupleProjection const &aggsProj,
    TupleProjection const &dataProj)
{
    PBuffer destKey = findKey(inputTuple, keyColsProj);

    if (!destKey) {
        destKey = addKey(inputTuple, keyColsProj, aggsProj);
    }
    
    return ((destKey != NULL) && addData(destKey, inputTuple, dataProj));
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
        PBuffer *slot = NULL;
        while ((!slot || !((PBuffer)*slot)) &&
            (curSlot < hashTable->getNumSlots())) {
            slot = hashTable->getSlot(curSlot);
            curSlot ++;
        }

        if (!slot || !((PBuffer)*slot)) {
            return false;
        } else {
            curKey = *slot;
        }
    }
    
    hashKeyAccessor.setCurrent(curKey, true);
    curData = hashKeyAccessor.getFirstData();
    assert(curData);
    hashDataAccessor.setCurrent(curData, true);
    return true;
}

bool LhxHashTableReader::advanceKey()
{
    curKey = hashKeyAccessor.getNext();
    if (curKey) {
        hashKeyAccessor.setCurrent(curKey, true);
        curData = hashKeyAccessor.getFirstData();
        assert(curData);
        hashDataAccessor.setCurrent(curData, true);        
        return true;
    } else {
        return false;
    }
}

bool LhxHashTableReader::advanceData()
{
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
    hashDataAccessor.unpack(outputTuple, dataProj);
}

void LhxHashTableReader::init(
    LhxHashTable *hashTableInit,
    TupleDescriptor const &outputTupleDesc,
    TupleProjection keyColsProj,
    TupleProjection aggsProj,
    TupleProjection const &dataProjInit)
{
    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;
    uint keyCount = keyColsProj.size();
    uint i;

    keyColsAndAggsProj = keyColsProj;
    for (int i = 0; i < keyCount; i++) {
        keyDesc.push_back(outputTupleDesc[keyColsProj[i]]);
        keyColsProj[i] = i;
    }
    
    for (i = 0; i < aggsProj.size(); i ++) {
        keyColsAndAggsProj.push_back(aggsProj[i]);
        keyDesc.push_back(outputTupleDesc[aggsProj[i]]);
        aggsProj[i] = i + keyCount;
    }

    hashKeyAccessor.init(keyDesc, keyColsProj, aggsProj);

    dataProj = dataProjInit;
    for (int i = 0; i < dataProj.size(); i++) {
        dataDesc.push_back(outputTupleDesc[dataProj[i]]);
    }

    hashDataAccessor.init(dataDesc);

    hashTable = hashTableInit;
 
    /*
     * By default, this reader is not associated with any key, i.e. it covers
     * the whole hash table.
     */
    bindKey(NULL);
}

void LhxHashTableReader::bindKey(PBuffer key)
{
    boundKey = curKey = key;
    curSlot = 0;
    curData = NULL;
    started = false;
}

bool LhxHashTableReader::getNext(TupleData &outputTuple)
{
    if (!started) {
        /*
         * Position at th first key of the first slot.
         */
        if (!advanceSlot()) {
            /*
             * Nothing to output.
             */
            return false;
        }
        produceTuple(outputTuple);
        started = true;
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

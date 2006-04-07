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

#include "fennel/lucidera/hashexe/LhxHashTable.h"
#include <iomanip>
#include <sstream>
#include <set>
#include <map>

using namespace std;

FENNEL_BEGIN_CPPFILE("$Id$");

static string printDatum(TupleDatum const &tupleDatum)
{
    ostringstream byteStr;
    PConstBuffer ptr = tupleDatum.pData;
    uint size = tupleDatum.cbData;

    if (ptr) {        
        /*
         * pData stores bytes backwards.
         */
        PConstBuffer tmpPtr = ptr + size;
        while (size > 0) {
            tmpPtr --;
            byteStr << hex << setw(2) << setfill('0')  << (uint)(*tmpPtr);
            size --;
        }
        assert(tmpPtr == ptr);
    }

    return byteStr.str();
}

static string printTuple(TupleData const &tupleData)
{
    ostringstream byteStr;

    for (int i = 0; i < tupleData.size(); i ++) {
        byteStr << printDatum(tupleData[i]) << " ";
    }
    return byteStr.str();
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

void LhxHashDataAccessor::unpack(TupleData &outputTuple)
{
    PBuffer buf = getBuffer();

    assert(buf != NULL && outputTuple.size() == dataAccessor.size());

    /*
     * Set pointers in the outputtuple.
     */
    dataAccessor.unmarshal(outputTuple);
}

string LhxHashDataAccessor::toString()
{
    TupleData outputTuple;
    ostringstream dataTrace;
    
    outputTuple.compute(dataDescriptor);
    unpack(outputTuple);
    dataTrace << "[Data Node] [" << printTuple(outputTuple) << "]";
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

    keyColsProj   = keyColsProjInit;
    aggsProj      = aggsProjInit;

    keyAccessor.compute(keyDescriptor);
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

void LhxHashKeyAccessor::unpack(TupleData &outputTuple)
{
    PBuffer buf = getBuffer();

    assert(buf != NULL && outputTuple.size() == keyAccessor.size());

    /*
     * Set pointers in the outputtuple.
     */
    keyAccessor.unmarshal(outputTuple);
}

bool LhxHashKeyAccessor::matches(
    TupleData const &inputTuple,
    TupleProjection const &inputKeyProj)
{
    assert(inputKeyProj.size() == keyColsProj.size());

    TupleData inputKeyTuple;    
    inputKeyTuple.projectFrom(inputTuple, inputKeyProj);
    
    TupleData currentTuple;
    currentTuple.compute(keyDescriptor);
    keyAccessor.unmarshal(currentTuple);

    TupleData currentKey;
    currentKey.projectFrom(currentTuple, keyColsProj);

    TupleDescriptor keyColsDesc;
    keyColsDesc.projectFrom(keyDescriptor, keyColsProj);
 
   return (keyColsDesc.compareTuples(currentKey, inputKeyTuple) == 0);
}

string LhxHashKeyAccessor::toString()
{
    TupleData outputTuple;
    ostringstream keyTrace;

    outputTuple.compute(keyDescriptor);
    unpack(outputTuple);
    keyTrace << "[Key Node] ["
             << (isTouched() ? "touched" : "untouched") << "] ["
             << printTuple(outputTuple) << "] ";
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
    endPtr = freePtr + blockUsableSize;
    if (!valid) {
        /*
         * Clear the memory if it is not valid.
         */
        memset(getCurrent(), 0, blockUsableSize + getBufferOffset());
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
    if (slotNum > numSlotsPerBlock) {
        return NULL;
    } else {
        return (PBuffer *)(getBuffer() + slotNum * sizeof(PBuffer ));
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

    TupleDescriptor keyDesc;
    TupleDescriptor dataDesc;
    set<uint> keyAndAggSet;
    set<uint>::iterator iter;
    map<uint, uint> colMap;
    uint i;

    /*
     * REVIEW: Here we assume that the key cols set and the aggs set do not
     * intersect. If they might intersect, duplicates need to be removed from
     * the resulting keyDesc; also keyColsProj and aggsProj need to be
     * recomputed based on the new keyDesc.
     */
    for (i = 0; i < keyColsProj.size(); i++) {
        keyAndAggSet.insert(keyColsProj[i]);
    }

    for (i = 0; i < aggsProj.size(); i++) {
        keyAndAggSet.insert(aggsProj[i]);
    }
    
    for (iter = keyAndAggSet.begin(), i = 0;
         iter != keyAndAggSet.end();
         iter ++, i++) {
        keyDesc.push_back(inputTupleDesc[*iter]);
        colMap[*iter] = i;
    }
    
    for (int i = 0; i < keyColsProj.size(); i++) {
        keyColsProj[i] = colMap[keyColsProj[i]];
    }

    for (int i = 0; i < aggsProj.size(); i++) {
        aggsProj[i] = colMap[aggsProj[i]];
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
        blockAccessor.setNext(resultBlock, NULL);
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
            nodeBlockAccessor.setCurrent(newBlock);
            nodeBlockAccessor.setNext(currentBlock);
            currentBlock = newBlock;                

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
    nodeBlockAccessor.setCurrent(currentBlock);
            
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
         * Initialize the block(clear all bytes etc).
         */
        nodeBlockAccessor.setCurrent(newBlock);
        nodeBlockAccessor.setNext(currentBlock);
        currentBlock = newBlock;

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
     * REVIEW : this has the potential of dealloacting all scratch pages.
     * To truly dealloc the scratch pages used for hash table building, the
     * client of Segment class needs to keep track of all PageIds allocated,
     * and then only deallocate those belonging to this client.
     *
     * An alternative will be to keep a list of allocated buffers(and never
     * call pSegment->deallocatePageRange()), so that allocated pages can be
     * reused between restarts(between two allocResources() calls) of the hash
     * table. That way hash table only uses memory up to the maximum hash table
     * size.
     */
    scratchAccessor.pSegment->deallocatePageRange(
        NULL_PAGE_ID, NULL_PAGE_ID);
}

uint LhxHashTable::blocksNeeded(
    double numRows,
    double cndKeys,
    TupleDescriptor &keyDesc,
    TupleDescriptor &dataDesc)
{
    LhxHashKeyAccessor tmpKey;
    LhxHashDataAccessor tmpData;

    double totalBytes = 
        slotsNeeded(cndKeys) * sizeof(PBuffer)
        + cndKeys * tmpKey.getMaxStorageSize(keyDesc)
        + numRows * tmpData.getMaxStorageSize(dataDesc);
    return (uint)ceil(totalBytes / blockAccessor.getUsableSize());
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
    assert (slot != NULL);

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
    assert (slot != NULL);

    TupleProjection keyColsAndAggsProj = keyColsProj;
    
    for (int i = 0; i < aggsProj.size(); i ++) {
        keyColsAndAggsProj.push_back(aggsProj[i]);
    }

    TupleData keyTuple;
    
    keyTuple.projectFrom(inputTuple, keyColsAndAggsProj);

    PBuffer newNextKey = *slot;
    uint newKeyLen = hashKeyAccessor.getStorageSize(&keyTuple);
    PBuffer newKey = allocBuffer(newKeyLen);

    if (!newKey) {
        /*
         * Ran out of memory.
         */
        return NULL;
    }

    *slot = newKey;
    hashKeyAccessor.setCurrent(newKey, false);
    hashKeyAccessor.pack(keyTuple);
    hashKeyAccessor.setNext(newNextKey);
    return newKey;
}

bool LhxHashTable::addData(
    PBuffer keyNode,
    TupleData const &inputTuple,
    TupleProjection const &dataProj)
{
    hashKeyAccessor.setCurrent(keyNode);

    TupleData dataTuple;
    
    dataTuple.projectFrom(inputTuple, dataProj);

    uint newDataLen = hashDataAccessor.getStorageSize(&dataTuple);
    PBuffer newData = allocBuffer(newDataLen);

    if (!newData) {
        /*
         * Ran out of memory.
         */
        return false;
    }

    hashDataAccessor.setCurrent(newData, false);
    hashDataAccessor.pack(dataTuple);
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

    slotTrace << "[Slot] [" << slotNum << "]\n";
    
    /*
     * Print all keys in this slot.
     */
    PBuffer currentHashKey = *slot;
    while (currentHashKey) {
        hashKeyAccessor.setCurrent(currentHashKey);
        slotTrace << "     " << hashKeyAccessor.toString() << "\n";

        /*
         * Print all data with the same key.
         */
        PBuffer currentHashData = hashKeyAccessor.getFirstData();
        while (currentHashData) {
            hashDataAccessor.setCurrent(currentHashData);
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

FENNEL_END_CPPFILE("$Id$");

// End LhxHashTable.cpp

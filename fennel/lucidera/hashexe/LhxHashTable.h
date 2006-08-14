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

#ifndef Fennel_LhxHashTable_Included
#define Fennel_LhxHashTable_Included

#include "fennel/lucidera/hashexe/LhxHashGenerator.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/lucidera/hashexe/LhxHashBase.h"
#include "fennel/exec/AggComputer.h"

#include "math.h"

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Class implementing the hash table used in Hybrid Hash Join.
 * The hash table class also has the ability to aggregate in place.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LhxHashNodeAccessor
{
    /*
     * Offset to the field storing the pointer to the next node.
     */
    uint nextNodeOffset;

    /*
     * Offset to the buffer storing the payload of this node.
     */
    uint nodeBufferOffset;

    /*
     * Node that is currently associated with this accessor.
     */
    PBuffer nodePtr;

public:
    LhxHashNodeAccessor();

    /**
     * Constructor for derived classes with fields before the next node
     * pointer. The idea is to store the variable length payload at the end.
     *
     * @param[in] nextNodeOffsetInit offset to store the next node pointer.
     */
    LhxHashNodeAccessor(uint nextNodeOffsetInit);

    /**
     * @return current node associated with this accessor
     */
    PBuffer getCurrent();

    /**
     * Set the current node pointer for this accessor.
     *
     * @param[in] nodePtrInit pointer to node that will be associated with this
     * accessor
     */
    void setCurrent(PBuffer nodePtrInit);

    /**
     * Reset the node pointer to NULL.
     */
    void reset();

    /**
     * @return buffer to the payload in this node
     */
    PBuffer getBuffer(); 

    /**
     * @return the next node
     */    
    PBuffer getNext();

    /**
     * @return the locatin which stored the next node pointer
     */    
    PBuffer getNextLocation();

    /**
     * Set the next node pointer for node associated with this accessor.
     *
     * @param[in] nextNode pointer to the next node
     */
    void setNext(PBuffer nextNode);

    /**
     * Set the next node pointer for an input node.
     *
     * @param[in] inputNode input node to set the next node pointer
     * @param[in] nextNode pointer to the next node
     */
    void setNext(PBuffer inputNode, PBuffer nextNode);
    
    /**
     * @return number of bytes required to store the next node pointer
     */
    uint getNextFieldSize();

    /**
     * @return offset to the . This is equivalent to the total space
     * used for the variable length payload.
     */
    uint getBufferOffset();
};

class LhxHashDataAccessor : public LhxHashNodeAccessor
{
    /*
     * Shape of the data tuple stored.
     */
    TupleDescriptor dataDescriptor;
    /*
     * Temporary tuple for holding the unmarshaled data.
     */
    TupleData       dataTuple;
    /*
     * Accessor for the data tuple stored.
     */
    TupleAccessor   dataAccessor;

public:
    LhxHashDataAccessor() : LhxHashNodeAccessor() {}

    /**
     * Set the shape of the data tuple to be stored via this accessor.
     *
     * @param[in] inputDataDesc
     */
    void init(TupleDescriptor const &inputDataDesc);

    /**
     * Set the current node pointer for this accessor.
     *
     * @param[in] nodePtrInit pointer to node that will be associated with this
     * accessor
     * @param[in] valid whether buffer content is valid.
     */
    void setCurrent(PBuffer nodePtrInit, bool valid);

    /**
     * Get max buffer size required to store all the fields based on
     * TupleDescriptor information previously passed in init().
     * This function needs to be called after calling init().
     */
    inline uint getMaxStorageSize();

    /**
     * Get actual buffer size required to store all the fields.
     *
     * @param[in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getStorageSize(TupleData const &inputTuple);

    /**
     * Get actual disk size required to store all the fields.
     *
     * @param[in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getDiskStorageSize(TupleData const &inputTuple);

    /**
     * Check that buffer size required to store all the fields does not exceed
     * scratch buffer size.
     *
     * @param[in] inputTuple if NULL get the tuple storage size for the buffer
     * associated with this accessor; else get the storage size for the
     * inputTuple.
     */
    void checkStorageSize(TupleData const &inputTuple, uint maxBufferSize);

    /**
     * Store a tuple in the buffer associated with this accessor.
     *
     * @param[in] inputTuple
     */
    void pack(TupleData const &inputTuple);

    /**
     * Retrieve the data stored in the buffer. Upon return, outputTuple will
     * point into the buffer associated with this accessor.
     *
     * @param[out] outputTuple
     * @param[out] destProj fields to copy to in the outputTuple
     */
    void unpack(TupleData &outputTuple, TupleProjection &destProj);

    /**
     * Print the content of the node associated with this accessor.
     */
    string toString();
};

class LhxHashKeyAccessor : public LhxHashNodeAccessor
{
    /*
     * Offsets to the fields in the node
     */
    uint firstDataOffset;
    uint isMatchedOffset;

    /*
     * Shape of the data tuple stored.
     */
    TupleDescriptor keyDescriptor;
    /*
     * Temporary tuple for holding the unmarshaled data.
     */
    TupleData keyTuple;
    /*
     * Accessor for the data tuple stored.
     */
    TupleAccessor keyAccessor;

    /*
     * Projection containing the key cols
     */
    TupleProjection keyColsProj;
    TupleDescriptor keyColsDesc;

    /*
     * Projection containing the aggregate columns
     */
    TupleProjection aggsProj;


public:
    LhxHashKeyAccessor();

    /**
     * Set the shape of the data tuple to be stored via this accessor.
     *
     * @param[in] keyDescInit
     * @param[in] keyColsProjInit the key fields
     * @param[in] aggsProjInit the aggregate fields
     */
    void init(
        TupleDescriptor const &keyDescInit,
        TupleProjection const &keyColsProjInit,
        TupleProjection const &aggsProjInit);

    /**
     * Set the current node pointer for this accessor.
     *
     * @param[in] nodePtrInit pointer to node that will be associated with this
     * accessor
     * @param[in] valid whether buffer content is valid.
     */
    void setCurrent(PBuffer nodePtrInit, bool valid);

    /**
     * Get the first data node off this key node.
     *
     * @return pointer to the first data node.
     */
    PBuffer getFirstData();

    /**
     * Set pointer to the first data node.
     *
     * @param[in] inputFirstData
     */
    void setFirstData(PBuffer inputFirstData);

    /**
     * Check if this key has been matched before.
     *
     * @return true if this key has been seen.
     */
    bool isMatched();

    /**
     * Set if this key has been seen.
     *
     * @param[in] touched
     */
    void setMatched(bool matched);

    /**
     * Add data node to this key.
     *
     * @param[in] inputData
     */
    void addData(PBuffer inputData);

    /**
     * Get max buffer size required to store all the fields based on
     * TupleDescriptor information previously passed in init().
     * This function needs to be called after calling init().
     */
    inline uint getMaxStorageSize();

    /**
     * Get actual buffer size required to store all the fields.
     *
     * @param[in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getStorageSize(TupleData const &inputTuple);

    /**
     * Get actual disk size required to store all the fields.
     *
     * @param[in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getDiskStorageSize(TupleData const &inputTuple);

    /**
     * Check that buffer size required to store all the fields does not exceed
     * scratch buffer size.
     *
     * @param[in] inputTuple if NULL get the tuple storage size for the buffer
     * associated with this accessor; else get the storage size for the
     * inputTuple.
     */
    void checkStorageSize(TupleData const &inputTuple, uint maxBufferSize);

    /**
     * Store a tuple in the buffer associated with this accessor.
     * Has to be called before trying to match any input with this key.
     *
     * @param[in] inputTuple
     */
    void pack(TupleData const &inputTuple);

    /**
     * Retrieve the data stored in the buffer. Upon return, outputTuple will
     * point into the buffer associated with this accessor.
     *
     * @param[out] outputTuple
     * @param[out] destProj fields to copy to in the outputTuple
     */
    void unpack(TupleData &outputTuple, TupleProjection &destProj);

    /*
     * Compare if inputTuple has the same key.
     *
     * @param[in] inputTuple
     * @param[out] inputKeyProj the key fields.
     */
    bool matches(TupleData const &inputTuple,
        TupleProjection const &inputKeyProj);

    /**
     * Print the content of the node associated with this accessor.
     */
    string toString();
};

class LhxHashBlockAccessor : public LhxHashNodeAccessor
{
    /**
     * Size of the buffer that a client can use.
     */
    uint        blockUsableSize;

    /**
     * Maximum number of slots per block.
     */
    uint        numSlotsPerBlock;
    
    /**
     * Free space in the current block
     */
    PBuffer freePtr;

    /**
     * Free space in the current block
     */
    PBuffer endPtr;

public:
    LhxHashBlockAccessor() : LhxHashNodeAccessor() {};

    /**
     * Set the size of the block.
     *
     * @param[in] usablePageSize
     */
    void init(uint usablePageSize);

    /**
     * Set the current node pointer for this accessor.
     *
     * @param[in] nodePtrInit pointer to node that will be associated with this
     * accessor
     * @param[in] valid whether buffer content is valid.
     */
    void setCurrent(PBuffer blockPtrInit, bool valid);

    /**
     * Reset the node pointer to NULL.
     */
    void reset();

    /**
     * @return the size of the block that a client can use.
     */
    uint getUsableSize() { return blockUsableSize; }

    /**
     * @return the maximum number of slots per block.
     */
    uint getSlotsPerBlock() { return numSlotsPerBlock; }
    
    /**
     * Allocate a buffer from this block.
     *
     * @param[in] bufSize
     *
     * @return null if no more space left in this block.
     */
    PBuffer allocBuffer(uint bufSize);

    /*
     * Allocate slots. A slot is a pointer to a hash key. There is no boundray
     * check as the number of slots that can fit into a block is calculated
     * beforehand.
     *
     * @param[in] slotCount number of slots to allocate.
     */
    void allocSlots(uint slotCount = 1);

    /**
     * Get the slot indexed by slotNum from this block.
     *
     * @param[in] slotNum
     *
     * @return pointer to the slot. NULL if this slot does not exist on the
     * current block.
     */
    PBuffer *getSlot(uint slotNum);
};

class LhxHashTable
{
    /**
     * Inputs to LhxHashTable
     */

    /**
     * Size of the hash table, i.e. number of slots
     */
    uint              numSlots;
    
    /**
     * Scratch accessor for allocating large buffer pages
     */
    SegmentAccessor   scratchAccessor;

    /**
     * maximum number of blocks to use for building this hash table.
     */
    uint  maxBlockCount;


    /**
     * Storage for the hash table.
     */

    /**
     * Linked list of blocks to fit the hash entry array and hash value nodes
     * in. A new block is linked to the head of the list.
     */
    PBuffer firstBlock;
    
    PBuffer currentBlock;

    /**
     * This block accessor can be associated with any block.
     */
    LhxHashBlockAccessor blockAccessor;

    /**
     * This block accessor is associated with the first block that
     * contains key or data nodes.
     */
    LhxHashBlockAccessor nodeBlockAccessor;

    /**
     * Lock on scratch page
     */
    SegPageLock bufferLock;

    /**
     * current number of scratch buffers in use.
     */
    uint  currentBlockCount;

    /**
     * special hash table properties.
     */
    bool filterNull;
    bool removeDuplicate;

    /**
     * The hash generators used by this hash table: one for the current level;
     * one for the sub partition level(==partitionLevl+1).
     */
    uint partitionLevel;
    LhxHashGenerator hashGen;
    LhxHashGenerator hashGenSub;

    /**
     * Fields in the inputTuple parameter to addTuple() method that will hold
     * keyCols, Aggs, and data columns. inputTuple should have the same shape
     * as hashInfo.inputDesc[1] used in the init() method.
     */
    TupleProjection keyColsAndAggsProj;
    TupleProjection keyColsProj;
    TupleProjection aggsProj;
    TupleProjection dataProj;
    vector<bool>    isKeyColVarChar;

    /**
     * Accessors for the content of this hash table.
     */
    LhxHashKeyAccessor  hashKeyAccessor;
    LhxHashDataAccessor hashDataAccessor;

    /**
     * The maximum number of bytes writable in a scratch page.
     */
    uint maxBufferSize;

    /**
     * Marks if this hash table is built for Group-by. Group-by hash
     * table only contains keys(group by keys plus aggregates) and does not
     * contain data portion.
     */
    bool isGroupBy;

    /**
     * For group-bys, marks if there are any aggregates.
     */
    bool hasAggregates;

    /**
     * aggregate computers passed in from the agg exec stream.
     */
    AggComputerList *aggComputers;
    TupleData        aggWorkingTuple;
    TupleDataWithBuffer aggResultTuple;

    /**
     * Allocate a block.
     *
     * @return pointer to the block. NULL if maxBlockCount is exceeded.
     */
    PBuffer allocBlock();

    /**
     * Allocate a buffer of size bufSize.
     *
     * @param[in] bufSize
     *
     * @return pointer to the buffer. NULL if no more space left.
     */
    PBuffer allocBuffer(uint bufSize);

    /**
     * Print the content of a slot, i.e. the content of all the keys and
     * their data nodes.
     *
     * @param[in] slotNum
     */
    string printSlot(uint slotNum);

    /**
     * Add a key node, with data.
     *
     * @param[in] inputTuple
     *
     * @return false if hash table is out of memory.
     */
    bool addKeyData(TupleData const &inputTuple);

    /**
     * Add a data node, following an existing keyNode.
     *
     * @param[in] keyNode the key node for this data node
     * @param[in] inputTuple
     * @param[in] keyColsProj the key fields
     * @param[in] aggsProj the aggregate fields
     *
     * @return false if hash table is out of memory.
     */
    bool addData(PBuffer keyNode, TupleData const &inputTuple);

    /**
     * Aggregate a new tuple.
     *
     * @param[in] keyNode the matching key node
     * @param[in] inputTuple
     */
    bool aggData(PBuffer destKeyLoc, TupleData const &inputTuple);

    /**
     * Compute the number of slots required to hold "cndKeys" keys without
     * significant collisions.
     *
     * @param[in] cndKeys number of distinct keys
     *
     * @return number of slots needed.
     */
    static inline uint slotsNeeded(double cndKeys);

    /**
     * Find location that stores the key node based on key cols.
     *
     * @param[in] inputTuple
     * @param[in] inputKeyColsProj key columns from the inputTuple.
     * @param[in] isProbing whether the hash table is being probed.
     * @param[in] removeDuplicate
     *
     * @return the buffer which stored the address of the key
     */
    PBuffer findKeyLocation(
        TupleData const &inputTuple,
        TupleProjection const &inputKeyProj,
        bool isProbing,
        bool removeDuplicateProbe);
public:

    static const uint LhxHashTableMinPages = 2;

    /**
     * Initialize the hash table.
     *
     * @param[in] partitionLevelInit recursive partitioning level
     * @param[in] hashInfo
     * @param[in] aggList pointer to list of agg computers.
     * @param[in] buildInputIndex which input is the build side.
     *
     */
    void init(
        uint partitionLevelInit,
        LhxHashInfo const &hashInfo,
        AggComputerList *aggList,
        uint buildInputIndex);

    /**
     * Initialize the hash table.
     *
     * @param[in] partitionLevelInit recursive partitioning level
     * @param[in] hashInfo
     * @param[in] buildInputIndex which input is the build side.
     */
    void init(
        uint partitionLevelInit,
        LhxHashInfo const &hashInfo,
        uint buildInputIndex);

    /**
     * Allocate blocks to hold the number of slots needed for this hash table.
     *
     * @param[in] reuse if true, reuse the blocks already allocated to this
     * hash table.
     *
     * @return status. false if no more space left in the hash table.
     */
    bool allocateResources(bool reuse=false);

    /**
     * Release the blocks allocated.
     *
     * @param[in] reuse if true do not release the scratch pages back to the
     * cache.
     */
    void releaseResources(bool reuse=false);

    /**
     * Compute the number of blocks and slots required by the hash table and
     * its contents for "nRows" rows with "cndKeys" distinct key values, for
     * the specified key(aggs included) and data descriptions.
     *
     * @param[in] hashInfo
     * @param[in] inputIndex which input is the the hash table building on
     *
     * @param[out] numBlocks max number of blocks for this hash table.
     */
    void calculateSize(
        LhxHashInfo const &hashInfo,
        uint inputIndex,
        uint &numBlocks);

    /**
     * Compute the number of slots required by this hash table to store
     * "cndKeys" distinct key values.
     *
     * @param[in] cndKeys
     * @param[in] usablePageSize indicate the usable page size.
     * @param[in] numBlocks maximum number of blocks budgeted for this
     * hash table
     */
    void calculateNumSlots(
        double cndKeys,
        uint usablePageSize,
        uint numBlocks);
    
    /**
     * Find key node based on key cols.
     *
     * @param[in] inputTuple
     * @param[in] keyColsProj key columns from the inputTuple.
     * @param[in] removeDuplicateProbe
     *
     * @return the buffer which stored the address of the key
     */
    PBuffer findKey(
        TupleData const &inputTuple,
        TupleProjection const &inputKeyProj,
        bool removeDuplicateProbe);

    /**
     * Insert a new tuple.
     *
     * @param[in] inputTuple
     */
    bool addTuple(TupleData const &inputTuple);

    /**
     * Get the slot indexed by slotNum.
     *
     * @param[in] slotNum
     *
     * @return pointer to the slot.
     */
    PBuffer *getSlot(uint slotNum);

    /**
     * @return number of slots.
     */
    inline uint getNumSlots() const;

    /**
     * @return if this hash table aggregates input
     */
    inline bool isHashGroupBy() const;

    /**
     * Print the content of the hash table.
     *
     * @return the string representation of this hash table.
     */
    string toString();
};

class LhxHashTableReader
{
    /**
     * Underlying hash table to read from.
     */
    LhxHashTable *hashTable;
    
    /**
     * Marks if this hash table is built for aggregation. Aggregating hash
     * table only contains keys(group by keys plus aggregates) and does not
     * contain data portion. The reader behavior will thus be different from
     * reading a hash table built for joins.
     */
    bool isGroupBy;

    /**
     * Current read location.
     */
    uint    curSlot;
    PBuffer curKey;
    PBuffer curData;

    /**
     * If not NULL, only read tuple with matching keys.
     * Not compatible with returnUnMatched
     */
    PBuffer boundKey;

    /**
     * If true, only return tuples with unmatch keys.
     * This is not compatible with boundKey.
     */
    bool    returnUnMatched;

    /**
     * Whether reader is positioned.
     */
    bool    isPositioned;

    /**
     * Accessors for the content of this hash table.
     */
    LhxHashKeyAccessor  hashKeyAccessor;
    LhxHashDataAccessor hashDataAccessor;

    /**
     * Fields in the outputTuple that will hold keyCols and Aggs,
     * and data columns. output tuple should have the same shape as
     * outputTupleDesc used in the init() method. 
     */
    TupleProjection keyColsAndAggsProj;
    TupleProjection dataProj;

    /**
     * Locate the next slot to produce tuples from. Set up the curKey and
     * curData pointers.
     *
     * @return false if there is no more slots with keys.
     */
    bool advanceSlot();

    /**
     * Locate the next key to produce tuples from. Set up the curKey and
     * curData pointers.
     *
     * @return false if there is no more keys in the same slot.
     */
    bool advanceKey();

    /**
     * Locate the next data to produce tuples from. Set up the curData
     * pointers.
     *
     * @return false if there is no more data with the same key.
     */
    bool advanceData();
    
    /**
     * Produce the curKey + curData into outputTuple.
     */
    void produceTuple(TupleData &outputTuple);

    /**
     * Helper fundtion for bindKey() and bindUnMatched().
     */
    void bind(PBuffer key);
public:
    /**
     * Initialize the hash table reader.
     *
     * @param[in] hashTableInit the underlying hash table to read from
     * @param[in] hashInfo
     * @param[in] buildInputIndex which input is the build side.
     */
    void init(
        LhxHashTable *hashTableInit,
        LhxHashInfo const &hashInfo,
        uint buildInputIndex);

    /**
     * Bind this reader to a certain key. Only tuples with the same key are
     * returned.
     *
     * @param[in] key key node to bind this reader to.
     */
    void bindKey(PBuffer key);

    /**
     * Bind this reader to unmatched keys. Only rows with unmatched keys will
     * be returned.
     */
    void bindUnMatched();

    /**
     * Get the next outputTuple.
     *
     * @params[out] outputTuple.
     *
     * @return false if no more tuples to output.
     */
    bool getNext(TupleData &outputTuple);

    inline LhxHashTable *getHashTable();
};

inline LhxHashNodeAccessor::LhxHashNodeAccessor()
{
    nextNodeOffset = 0;
    nodeBufferOffset = nextNodeOffset + getNextFieldSize();
}

inline LhxHashNodeAccessor::LhxHashNodeAccessor(uint nextNodeOffsetInit)
{
    nextNodeOffset = nextNodeOffsetInit;
    nodeBufferOffset = nextNodeOffset + getNextFieldSize();
}

inline PBuffer LhxHashNodeAccessor::getCurrent()
{
    return nodePtr;
}

inline PBuffer LhxHashNodeAccessor::getBuffer()
{
    return (nodePtr + nodeBufferOffset);
}

inline void LhxHashNodeAccessor::setCurrent(PBuffer nodePtrInit)
{ 
    nodePtr = nodePtrInit;
}

inline void LhxHashNodeAccessor::reset()
{ 
    setCurrent(NULL);
}

inline PBuffer LhxHashNodeAccessor::getNext()
{
    /*
     * nodePtr+nextNodeOffset might not be aligned so copy the pointer
     * value out.
     */
    PBuffer returnPtr;
    memcpy((PBuffer)&returnPtr, nodePtr+nextNodeOffset, sizeof(PBuffer));
    return returnPtr;
}

inline PBuffer LhxHashNodeAccessor::getNextLocation()
{
    return nodePtr+nextNodeOffset;
}

inline void LhxHashNodeAccessor::setNext(PBuffer nextNode)
{
    memcpy(nodePtr+nextNodeOffset, (PBuffer)&nextNode, getNextFieldSize());
}

inline void LhxHashNodeAccessor::setNext(PBuffer inputNode, PBuffer nextNode)
{
    memcpy(inputNode+nextNodeOffset, (PBuffer)&nextNode, getNextFieldSize());
}

inline uint LhxHashNodeAccessor::getNextFieldSize()
{ 
    return sizeof(PBuffer);
}

inline uint LhxHashNodeAccessor::getBufferOffset()
{
    return nodeBufferOffset;
}

inline void LhxHashDataAccessor::setCurrent(PBuffer nodePtrInit, bool valid)
{ 
    LhxHashNodeAccessor::setCurrent(nodePtrInit);
    dataAccessor.setCurrentTupleBuf(getBuffer(), valid);
}

inline uint LhxHashDataAccessor::getMaxStorageSize()
{   
    return (dataAccessor.getMaxByteCount() + getBufferOffset());
}

inline uint LhxHashDataAccessor::getStorageSize(TupleData const &inputTuple)
{
    return dataAccessor.getByteCount(inputTuple) + getBufferOffset();
}

inline uint LhxHashDataAccessor::getDiskStorageSize(
    TupleData const &inputTuple)
{   
    return dataAccessor.getByteCount(inputTuple);
}

inline void LhxHashKeyAccessor::setCurrent(PBuffer nodePtrInit, bool valid)
{
    LhxHashNodeAccessor::setCurrent(nodePtrInit);
    keyAccessor.setCurrentTupleBuf(getBuffer(), valid);
}

inline uint LhxHashKeyAccessor::getMaxStorageSize()
{   
    return (keyAccessor.getMaxByteCount() + getBufferOffset());
}

inline uint LhxHashKeyAccessor::getStorageSize(TupleData const &inputTuple)
{
    return keyAccessor.getByteCount(inputTuple) + getBufferOffset();
}

inline uint LhxHashKeyAccessor::getDiskStorageSize(TupleData const &inputTuple)
{   
    return keyAccessor.getByteCount(inputTuple);
}

inline PBuffer LhxHashKeyAccessor::getFirstData()
{
    PBuffer returnPtr;
    memcpy(
        (PBuffer)&returnPtr,
        (PBuffer)(getCurrent()+firstDataOffset),
        sizeof(PBuffer));
    return returnPtr;
}

inline void LhxHashKeyAccessor::setFirstData(PBuffer inputFirstData)
{
    memcpy((PBuffer)(getCurrent()+firstDataOffset), (PBuffer)&inputFirstData,
            sizeof(PBuffer));
}

inline bool LhxHashKeyAccessor::isMatched()
{
    return (*(uint8_t *)(getCurrent() + isMatchedOffset) == 1);
}

inline void LhxHashKeyAccessor::setMatched(bool matched)
{
    *(getCurrent() + isMatchedOffset) = (matched ? 0x01 : 0);
}

inline void LhxHashBlockAccessor::reset()
{
    LhxHashNodeAccessor::reset();
    freePtr = endPtr = NULL;
}

inline void LhxHashBlockAccessor::allocSlots(uint slotCount)
{
    /*
     * A slot is a pointer to a hash key.
         */
    PBuffer bufPtr = allocBuffer(slotCount * sizeof(PBuffer ));
    assert (bufPtr != NULL);
}

inline uint LhxHashTable::slotsNeeded(double cndKeys)
{
    return uint(ceil(cndKeys * 1.2));
}

inline uint LhxHashTable::getNumSlots() const { return numSlots; }

inline bool LhxHashTable::isHashGroupBy() const { return isGroupBy; }

inline LhxHashTable *LhxHashTableReader::getHashTable() {return hashTable;}

FENNEL_END_NAMESPACE

#endif

// End LhxHashTable.h

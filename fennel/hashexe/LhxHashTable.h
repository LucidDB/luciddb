/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2006-2009 LucidEra, Inc.
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

#include "fennel/hashexe/LhxHashGenerator.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleProjectionAccessor.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/hashexe/LhxHashBase.h"
#include "fennel/exec/AggComputer.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/tuple/TupleOverflowExcn.h"

#include "math.h"

using namespace std;

FENNEL_BEGIN_NAMESPACE

// REVIEW jvs 25-Aug-2006:  This class comment is misplaced.  And
// the other classes could use some comments to explain their purpose
// (or else a reference to a doc with structure diagrams).

/**
 * Class implementing the hash table used in Hybrid Hash Join.
 * The hash table class also has the ability to aggregate in place.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class FENNEL_HASHEXE_EXPORT LhxHashNodeAccessor
{
    // REVIEW jvs 25-Aug-2006: These should be doxygen comments.

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

    // REVIEW jvs 25-Aug-2006: Fennel convention is to use the keyword
    // "explict" for all constructors, except where implicit conversion is
    // actually desired.  For example, the second constructor here means that a
    // uint can silently be converted into a LhxHasNodeAccessor, which is
    // almost certainly not what one would ever want.  Same comment
    // applies everywhere.

    LhxHashNodeAccessor();

    /**
     * Constructor for derived classes with fields before the next node
     * pointer. The idea is to store the variable length payload at the end.
     *
     * @param [in] nextNodeOffsetInit offset to store the next node pointer.
     */
    LhxHashNodeAccessor(uint nextNodeOffsetInit);

    /**
     * @return current node associated with this accessor
     */
    PBuffer getCurrent();

    /**
     * Set the current node pointer for this accessor.
     *
     * @param [in] nodePtrInit pointer to node that will be associated with this
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
     * @return the location which stores the next node pointer
     */
    PBuffer getNextLocation();

    /**
     * Set the next node pointer for node associated with this accessor.
     *
     * @param [in] nextNode pointer to the next node
     */
    void setNext(PBuffer nextNode);

    /**
     * Set the next node pointer for an input node.
     *
     * @param [in] inputNode input node to set the next node pointer
     * @param [in] nextNode pointer to the next node
     */
    void setNext(PBuffer inputNode, PBuffer nextNode);

    /**
     * @return number of bytes required to store the next node pointer
     */
    uint getNextFieldSize();

    // REVIEW jvs 25-Aug-2006: offset to the...what?

    /**
     * @return offset to the . This is equivalent to the total space
     * used for the variable length payload.
     */
    uint getBufferOffset();
};

// REVIEW jvs 25-Aug-2006: Seems like a lot of duplication between
// LhxHashKeyAccessor and LhxHashDataAccessor; maybe it's unavoidable.

class FENNEL_HASHEXE_EXPORT LhxHashDataAccessor
    : public LhxHashNodeAccessor
{
    /*
     * Shape of the data tuple stored.
     */
    TupleDescriptor dataDescriptor;

    /*
     * Temporary tuple for holding the unmarshaled data.
     */
    TupleData dataTuple;

    /*
     * Accessor for the data tuple stored.
     */
    TupleAccessor dataAccessor;

public:
    LhxHashDataAccessor() : LhxHashNodeAccessor() {}

    /**
     * Set the shape of the data tuple to be stored via this accessor.
     *
     * @param [in] inputDataDesc
     */
    void init(TupleDescriptor const &inputDataDesc);

    /**
     * Set the current node pointer for this accessor.
     *
     * @param [in] nodePtrInit pointer to node that will be associated with this
     * accessor
     * @param [in] valid whether buffer content is valid.
     */
    void setCurrent(PBuffer nodePtrInit, bool valid);

    /**
     * Get the avg buffer size required to store all the fields based on
     * TupleDescriptor information previously passed in init().
     * This function needs to be called after calling init().
     */
    inline uint getAvgStorageSize();

    // REVIEW jvs 25-Aug-2006: what's the distinction between
    // buffer size and disk size?  I think buffer size accounts
    // for the in-memory overhead of the hash table pointers,
    // but it would be nice if that were spelled out here.

    /**
     * Get actual buffer size required to store all the fields.
     *
     * @param [in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getStorageSize(TupleData const &inputTuple);

    /**
     * Get actual disk size required to store all the fields.
     *
     * @param [in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getDiskStorageSize(TupleData const &inputTuple);

    /**
     * Check that buffer size required to store all the fields does not exceed
     * scratch buffer size.
     *
     * @param [in] inputTuple if NULL get the tuple storage size for the buffer
     * associated with this accessor; else get the storage size for the
     * inputTuple.
     * @param [in] maxBufferSize maximum buffer size
     */
    inline void checkStorageSize(
        TupleData const &inputTuple,
        uint maxBufferSize);

    /**
     * Store a tuple in the buffer associated with this accessor.
     *
     * @param [in] inputTuple
     */
    inline void pack(TupleData const &inputTuple);

    /**
     * Retrieve the data stored in the buffer. Upon return, outputTuple will
     * point into the buffer associated with this accessor.
     *
     * @param [out] outputTuple
     * @param [out] destProj fields to copy to in the outputTuple
     */
    void unpack(TupleData &outputTuple, TupleProjection &destProj);

    /**
     * Print the content of the node associated with this accessor.
     */
    string toString();
};

class FENNEL_HASHEXE_EXPORT LhxHashKeyAccessor
    : public LhxHashNodeAccessor
{
    /*
     * Offsets to the fields in the node
     */
    uint firstDataOffset;

    /*
     * Offset to "matched" indicator. This indicator is used in both anti join
     * and duplicate removal during set operations(semi join).
     */
    uint isMatchedOffset;

    /*
     * Offset to keys in the next slot.
     */
    uint nextSlotOffset;

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

    /*
     * Temp data used to project input key tuple.
     */
    TupleData inputKey;

    /*
     * Temp data used to project stored key tuple in the hash table.
     */
    TupleData currentKey;

    /*
     * Temp data accessor for inserting new data fragments.
     */
    LhxHashDataAccessor firstData;

public:
    LhxHashKeyAccessor();

    /**
     * Set the shape of the data tuple to be stored via this accessor.
     *
     * @param [in] keyDescInit
     * @param [in] keyColsProjInit the key fields
     * @param [in] aggsProjInit the aggregate fields
     */
    void init(
        TupleDescriptor const &keyDescInit,
        TupleProjection const &keyColsProjInit,
        TupleProjection const &aggsProjInit);

    /**
     * Set the current node pointer for this accessor.
     *
     * @param [in] nodePtrInit pointer to node that will be associated with this
     * accessor
     * @param [in] valid whether buffer content is valid.
     */
    void setCurrent(PBuffer nodePtrInit, bool valid);

    /**
     * Get the first data node off this key node.
     *
     * @return pointer to the first data node.
     */
    inline PBuffer getFirstData();

    /**
     * Set pointer to the first data node.
     *
     * @param [in] inputFirstData
     */
    inline void setFirstData(PBuffer inputFirstData);

    /**
     * Get the first data node off this key node.
     *
     * @return pointer to the first data node.
     */
    inline PBuffer *getNextSlot();

    /**
     * Set pointer to the first data node.
     *
     * @param [in] nextSlot
     */
    inline void setNextSlot(PBuffer *nextSlot);

    /**
     * Check if this key has been matched before.
     *
     * @return true if this key has been seen.
     */
    inline bool isMatched();

    /**
     * Set if this key has been seen.
     *
     * @param [in] matched
     */
    inline void setMatched(bool matched);

    /**
     * Add data node to this key.
     *
     * @param [in] inputData
     */
    void addData(PBuffer inputData);

    /**
     * Get avg buffer size required to store all the fields based on
     * TupleDescriptor information previously passed in init().
     * This function needs to be called after calling init().
     */
    inline uint getAvgStorageSize();

    /**
     * Get actual buffer size required to store all the fields.
     *
     * @param [in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getStorageSize(TupleData const &inputTuple);

    /**
     * Get actual disk size required to store all the fields.
     *
     * @param [in] inputTuple get the storage size for this inputTuple.
     */
    inline uint getDiskStorageSize(TupleData const &inputTuple);

    /**
     * Check that buffer size required to store all the fields does not exceed
     * scratch buffer size.
     *
     * @param [in] inputTuple if NULL get the tuple storage size for the buffer
     * associated with this accessor; else get the storage size for the
     * inputTuple.
     * @param [in] maxBufferSize maximum buffer size
     */
    inline void checkStorageSize(
        TupleData const &inputTuple,
        uint maxBufferSize);

    /**
     * Store a tuple in the buffer associated with this accessor.
     * Has to be called before trying to match any input with this key.
     *
     * @param [in] inputTuple
     */
    inline void pack(TupleData const &inputTuple);

    /**
     * Retrieve the data stored in the buffer. Upon return, outputTuple will
     * point into the buffer associated with this accessor.
     *
     * @param [out] outputTuple
     * @param [out] destProj fields to copy to in the outputTuple
     */
    void unpack(TupleData &outputTuple, TupleProjection &destProj);

    /*
     * Compare if inputTuple has the same key.
     *
     * @param [in] inputTuple
     * @param [out] inputKeyProj the key fields.
     */
    bool matches(
        TupleData const &inputTuple,
        TupleProjection const &inputKeyProj);

    /**
     * Print the content of the node associated with this accessor.
     */
    string toString();
};

class FENNEL_HASHEXE_EXPORT LhxHashBlockAccessor
    : public LhxHashNodeAccessor
{
    /**
     * Size of the buffer that a client can use.
     */
    uint blockUsableSize;

    /**
     * Maximum number of slots per block.
     */
    uint numSlotsPerBlock;

    /**
     * Free space in the current block
     */
    PBuffer freePtr;

    /**
     * Free space in the current block
     */
    PBuffer endPtr;

public:
    LhxHashBlockAccessor() : LhxHashNodeAccessor()
    {
    }

    /**
     * Set the size of the block.
     *
     * @param [in] usablePageSize
     */
    void init(uint usablePageSize);

    /**
     * Set the current node pointer for this accessor.
     *
     * @param [in] blockPtrInit pointer to node that will be associated with
     * this accessor
     * @param [in] valid whether buffer content is valid.
     * @param [in] clearContent whether to clear invalid content
     */
    void setCurrent(PBuffer blockPtrInit, bool valid, bool clearContent);

    /**
     * Reset the node pointer to NULL.
     */
    void reset();

    /**
     * @return the size of the block that a client can use.
     */
    uint getUsableSize()
    {
        return blockUsableSize;
    }

    /**
     * @return the maximum number of slots per block.
     */
    uint getSlotsPerBlock()
    {
        return numSlotsPerBlock;
    }

    /**
     * Allocate a buffer from this block.
     *
     * @param [in] bufSize
     *
     * @return null if no more space left in this block.
     */
    PBuffer allocBuffer(uint bufSize);

    /*
     * Allocate slots. A slot is a pointer to a hash key. There is no boundary
     * check as the number of slots that can fit into a block is calculated
     * beforehand.
     *
     * @param [in] slotCount number of slots to allocate.
     */
    void allocSlots(uint slotCount = 1);

    /**
     * Get the slot indexed by slotNum from this block.
     *
     * @param [in] slotNum
     *
     * @return pointer to the slot. NULL if this slot does not exist on the
     * current block.
     */
    PBuffer *getSlot(uint slotNum);
};

class FENNEL_HASHEXE_EXPORT LhxHashTable
{
    /*
     * Inputs to LhxHashTable
     */

    /**
     * Size of the hash table, i.e. number of slots
     */
    uint numSlots;

    /**
     * Array of page buffers which have been allocated as index buffers.  These
     * contain arrays of pointers to tuple data stored in separate data
     * buffers.  Order is significant, since an index entry is decomposed into
     * a page and a position on that page.
     */
    std::vector<PBuffer> slotBlocks;

    PBuffer *firstSlot;
    PBuffer *lastSlot;

    /**
     * Scratch accessor for allocating large buffer pages
     */
    SegmentAccessor scratchAccessor;

    /**
     * maximum number of blocks to use for building this hash table.
     */
    uint maxBlockCount;


    /*
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
     * special hash table properties: hash table filtered null keys.
     */
    bool filterNull;
    TupleProjection filterNullKeyProj;

    /**
     * special hash table properties: hash table should remove duplicates.
     */
    bool removeDuplicate;

    // TODO rchen fix doxygen comments

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
    vector<LhxHashTrim> isKeyColVarChar;

    /**
     * Accessors for the content of this hash table.
     */
    LhxHashKeyAccessor hashKeyAccessor;
    LhxHashDataAccessor hashDataAccessor;

    /**
     * The maximum number of bytes writable in a scratch page.
     */
    uint maxBufferSize;

    /**
     * Marks if this hash table is built for Group-by. Group-by hash
     * table only contains keys (group by keys plus aggregates) and does not
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
    TupleData aggWorkingTuple;

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
     * @param [in] bufSize
     *
     * @return pointer to the buffer. NULL if no more space left.
     */
    PBuffer allocBuffer(uint bufSize);

    /**
     * Print the content of a slot, i.e. the content of all the keys and
     * their data nodes.
     *
     * @param [in] slotNum
     */
    string printSlot(uint slotNum);

    /**
     * Add a key node, with data.
     *
     * @param [in] inputTuple
     *
     * @return false if hash table is out of memory.
     */
    bool addKeyData(TupleData const &inputTuple);

    /**
     * Add a data node, following an existing keyNode.
     *
     * @param [in] keyNode the key node for this data node
     * @param [in] inputTuple
     *
     * @return false if hash table is out of memory.
     */
    bool addData(PBuffer keyNode, TupleData const &inputTuple);

    /**
     * Aggregate a new tuple.
     *
     * @param [in] destKeyLoc pointer to the destination key
     * @param [in] inputTuple
     */
    bool aggData(PBuffer destKeyLoc, TupleData const &inputTuple);

    /**
     * Compute the number of slots required to hold "cndKeys" keys without
     * significant collisions.
     *
     * @param [in] cndKeys number of distinct keys
     *
     * @return number of slots needed.
     */
    static inline uint slotsNeeded(RecordNum cndKeys);

    /**
     * Find location that stores the key node based on key cols.
     *
     * @param [in] inputTuple
     * @param [in] inputKeyProj key columns from the inputTuple.
     * @param [in] isProbing whether the hash table is being probed.
     * @param [in] removeDuplicateProbe
     *
     * @return the buffer which stored the address of the key
     */
    PBuffer findKeyLocation(
        TupleData const &inputTuple,
        TupleProjection const &inputKeyProj,
        bool isProbing,
        bool removeDuplicateProbe);

    TupleData tmpKeyTuple;
    TupleData tmpDataTuple;

public:

    // REVIEW jvs 25-Aug-2006:  No need to repeat LhxHashTable qualifier
    // in name here; Fennel convention should be something like
    // MIN_SCRATCH_PAGES.  Same comment applies elsewhere
    // (e.g. LhxSubPartCount).
    static const uint LhxHashTableMinPages = 2;

    /**
     * Initialize the hash table.
     *
     * @param [in] partitionLevelInit recursive partitioning level
     * @param [in] hashInfo
     * @param [in] aggList pointer to list of agg computers.
     * @param [in] buildInputIndex which input is the build side.
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
     * @param [in] partitionLevelInit recursive partitioning level
     * @param [in] hashInfo
     * @param [in] buildInputIndex which input is the build side.
     */
    void init(
        uint partitionLevelInit,
        LhxHashInfo const &hashInfo,
        uint buildInputIndex);

    // REVIEW jvs 25-Aug-2006:  Every place this is called, the caller
    // asserts that the return code is true.  So, it would probably
    // be better to return void, and fail inside of this method.  In
    // particular, this is a rather serious error if it occurs, so
    // probably use permAssert or permFail so that it will fail-fast
    // in an optimized build too.
    /**
     * Allocate blocks to hold the number of slots needed for this hash table.
     *
     * @param [in] reuse if true, reuse the blocks already allocated to this
     * hash table.
     *
     * @return status. false if no more space left in the hash table.
     */
    bool allocateResources(bool reuse = false);

    /**
     * Release the blocks allocated.
     *
     * @param [in] reuse if true do not release the scratch pages back to the
     * cache.
     */
    void releaseResources(bool reuse = false);

    /**
     * Compute the number of blocks and slots required by the hash table and
     * its contents for "nRows" rows with "cndKeys" distinct key values, for
     * the specified key(aggs included) and data descriptions.
     *
     * @param [in] hashInfo
     * @param [in] inputIndex which input is the the hash table building on
     *
     * @param [out] numBlocks max number of blocks for this hash table.  If
     * < 0, no stats are available to compute this value.
     */
    void calculateSize(
        LhxHashInfo const &hashInfo,
        uint inputIndex,
        BlockNum &numBlocks);

    /**
     * Compute the number of slots required by this hash table to store
     * "cndKeys" distinct key values.
     *
     * @param [in] cndKeys
     * @param [in] usablePageSize indicate the usable page size.
     * @param [in] numBlocks maximum number of blocks budgeted for this
     * hash table
     */
    void calculateNumSlots(
        RecordNum cndKeys,
        uint usablePageSize,
        BlockNum numBlocks);

    /**
     * Find key node based on key cols.
     *
     * @param [in] inputTuple
     * @param [in] inputKeyProj key columns from the inputTuple.
     * @param [in] removeDuplicateProbe
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
     * @param [in] inputTuple
     */
    bool addTuple(TupleData const &inputTuple);

    /**
     * Get the slot indexed by slotNum.
     *
     * @param [in] slotNum
     *
     * @return pointer to the slot.
     */
    PBuffer *getSlot(uint slotNum);

    /**
     * @return number of slots.
     */
    inline uint getNumSlots() const;

    /**
     * @return the first slot in a chain of slots.
     */
    inline PBuffer *getFirstSlot() const;

    /**
     * @return the next slot following curSlot in the slot chain.
     */
    inline PBuffer *getNextSlot(PBuffer *curSlot);

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

class FENNEL_HASHEXE_EXPORT LhxHashTableReader
{
    /**
     * Underlying hash table to read from.
     */
    LhxHashTable *hashTable;

    // REVIEW jvs 25-Aug-2006: Can't this be derived from hashTable->isGroupBy?
    /**
     * Marks if this hash table is built for aggregation. Aggregating hash
     * table only contains keys (group by keys plus aggregates) and does not
     * contain data portion. The reader behavior will thus be different from
     * reading a hash table built for joins.
     */
    bool isGroupBy;

    /**
     * Current read location.
     */
    PBuffer *curSlot;
    PBuffer curKey;
    PBuffer curData;

    // REVIEW jvs 25-Aug-2006: Matching key means matching the key
    // marshalled in boundKey?  Or is it a node pointer?
    /**
     * If not NULL, only read tuple with matching keys.
     * Not compatible with returnUnMatched
     */
    PBuffer boundKey;

    /**
     * If true, only return tuples with unmatched keys.
     * This is not compatible with boundKey.
     */
    bool returnUnMatched;

    /**
     * Whether reader is positioned.
     */
    bool isPositioned;

    /**
     * Accessors for the content of this hash table.
     */
    LhxHashKeyAccessor hashKeyAccessor;
    LhxHashDataAccessor hashDataAccessor;

    // REVIEW jvs 25-Aug-2006: Aren't these also redundant with
    // corresponding fields in hashTable?
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
     * Helper function for bindKey() and bindUnMatched().
     */
    inline void bind(PBuffer key);

public:
    /**
     * Initialize the hash table reader.
     *
     * @param [in] hashTableInit the underlying hash table to read from
     * @param [in] hashInfo
     * @param [in] buildInputIndex which input is the build side.
     */
    void init(
        LhxHashTable *hashTableInit,
        LhxHashInfo const &hashInfo,
        uint buildInputIndex);

    /**
     * Bind this reader to a certain key. Only tuples with the same key are
     * returned.
     *
     * @param [in] key key node to bind this reader to.
     */
    inline void bindKey(PBuffer key);

    /**
     * Bind this reader to unmatched keys. Only rows with unmatched keys will
     * be returned.
     */
    inline void bindUnMatched();

    /**
     * Get the next outputTuple.
     *
     * @param [out] outputTuple
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
    // REVIEW jvs 25-Aug-2006:  Under what circumstances would
    // alignment be off?  Tuple sizes are always aligned.  Is it
    // because of the match indicator?  If so, it might make sense to
    // fold that into the tuple.
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
    return nodePtr + nextNodeOffset;
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

inline uint LhxHashDataAccessor::getAvgStorageSize()
{
    // compute the average based on the min and max
    // TODO - use stats to compute a more realistic average
    return
        ((dataAccessor.getMaxByteCount() +
            dataAccessor.getMinByteCount()) / 2) +
        getBufferOffset();
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

inline void LhxHashDataAccessor::checkStorageSize(
    TupleData const &inputTuple,
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

inline void LhxHashKeyAccessor::pack(TupleData const &inputTuple)
{
    PBuffer buf = getBuffer();

    assert(buf != NULL && inputTuple.size() == keyAccessor.size());

    /*
     * Copy the input tuple into the buffer associated with this accessor.
     */
     keyAccessor.marshal(inputTuple, buf);
}

inline void LhxHashDataAccessor::pack(
    TupleData const &inputTuple)
{
    PBuffer buf = getBuffer();

    assert(buf != NULL && inputTuple.size() == dataAccessor.size());

    /*
     * Copy the input tuple into the buffer associated with this accessor.
     */
    dataAccessor.marshal(inputTuple, buf);
}

inline void LhxHashKeyAccessor::setCurrent(PBuffer nodePtrInit, bool valid)
{
    LhxHashNodeAccessor::setCurrent(nodePtrInit);
    keyAccessor.setCurrentTupleBuf(getBuffer(), valid);
}

inline uint LhxHashKeyAccessor::getAvgStorageSize()
{
    return
        ((keyAccessor.getMaxByteCount() + keyAccessor.getMinByteCount()) / 2) +
        getBufferOffset();
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
        (PBuffer) &returnPtr,
        (PBuffer) (getCurrent() + firstDataOffset),
        sizeof(PBuffer));
    return returnPtr;
}

inline void LhxHashKeyAccessor::setFirstData(PBuffer inputFirstData)
{
    memcpy(
        (PBuffer)(getCurrent() + firstDataOffset),
        (PBuffer)&inputFirstData,
        sizeof(PBuffer));
}

inline PBuffer *LhxHashKeyAccessor::getNextSlot()
{
    PBuffer *returnPtr;
    memcpy(
        (PBuffer) &returnPtr,
        (PBuffer) (getCurrent() + nextSlotOffset),
        sizeof(PBuffer *));
    return returnPtr;
}

inline void LhxHashKeyAccessor::setNextSlot(PBuffer *nextSlot)
{
    memcpy(
        (PBuffer)(getCurrent() + nextSlotOffset),
        (PBuffer)&nextSlot,
        sizeof(PBuffer*));
}

inline bool LhxHashKeyAccessor::isMatched()
{
    return (*(uint8_t *)(getCurrent() + isMatchedOffset) == 1);
}

inline void LhxHashKeyAccessor::setMatched(bool matched)
{
    *(getCurrent() + isMatchedOffset) = (matched ? 0x01 : 0);
}

inline void LhxHashKeyAccessor::checkStorageSize(
    TupleData const &inputTuple,
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
    PBuffer bufPtr = allocBuffer(slotCount * sizeof(PBuffer));
    assert (bufPtr != NULL);
}

inline uint LhxHashTable::slotsNeeded(RecordNum cndKeys)
{
    RecordNum cKeys = RecordNum(ceil(cndKeys * 1.2));
    if (cKeys >= uint(MAXU)) {
        return uint(MAXU) - 1;
    } else {
        return uint(cKeys);
    }
}

inline uint LhxHashTable::getNumSlots() const
{
    return numSlots;
}

inline PBuffer *LhxHashTable::getFirstSlot() const
{
    return firstSlot;
}

inline PBuffer *LhxHashTable::getNextSlot(PBuffer *curSlot)
{
    hashKeyAccessor.setCurrent((*curSlot), true);
    return hashKeyAccessor.getNextSlot();
}

inline bool LhxHashTable::isHashGroupBy() const
{
    return isGroupBy;
}

inline LhxHashTable *LhxHashTableReader::getHashTable()
{
    return hashTable;
}

inline void LhxHashTableReader::bind(PBuffer key)
{
    boundKey = curKey = key;
    curSlot = NULL;
    curData = NULL;
    isPositioned = false;
}

inline void LhxHashTableReader::bindKey(PBuffer key)
{
    returnUnMatched = false;
    bind(key);
}

inline void LhxHashTableReader::bindUnMatched()
{
    returnUnMatched = true;
    bind(NULL);
}

FENNEL_END_NAMESPACE

#endif

// End LhxHashTable.h

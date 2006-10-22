/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_LcsHash_Included
#define Fennel_LcsHash_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDataWithBuffer.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/UnalignedAttributeAccessor.h"
#include "fennel/lucidera/colstore/LcsClusterNodeWriter.h"


FENNEL_BEGIN_NAMESPACE

/************************************************
  classes and defines used only by LcsHash class
 ************************************************/

/**
 * This class implements the hash value ordinal which is the index into the
 * hash value nodes array stored in LcsHashTable class. The highest bit of the
 * value is used to indicate whether this value is part of the current batch.
 */
class LcsHashValOrd
{
private:

    /**
     * Index into the value nodes array.
     */
    uint16_t    valOrd;

public:
    
    LcsHashValOrd() {}
    ~LcsHashValOrd() {}
    
    inline explicit LcsHashValOrd(LcsHashValOrd const &other);
    
    /**
     * Copy assignment. Used to cast an uint16_t to LcsHashValOrd.
     */
    inline LcsHashValOrd& operator=(uint16_t valOrdInit);
  
    /**
     * Gets fields in LcsHashValOrd struct.
     * The highest bit in the value ordinal indicates whether or not value is
     * part of current batch.
     */
    inline uint16_t getValOrd();
    
    /**
     * Sets fields in LcsHashValOrd struct.
     * The highest bit in the value ordinal indicates whether or not value is
     * part of current batch.
     */
    inline void setValOrd(uint16_t valOrdInit);

    /**
     * Checks if this value node is part of the current batch.
     */
    inline bool isValueInBatch();
        
    /**
     * Marks that this value node is part of the current batch.
     */
    inline void setValueInBatch();
    
    /**
     * Marks that this value node is not part of the current batch.
     */
    inline void clearValueInBatch();
};


/**
 * LcsHashValNode contains information on every value inserted into the hash
 * table.
 */
struct LcsHashValueNode
{
    /**
     * Index to the next hash value node with this hash key.
     */
    uint16_t      next;

    /**
     * Hash value ordinal contains index into valueNodes array for this hash
     * value node. The highest bit indicates value part of batch.
     */
    LcsHashValOrd valueOrd;

    /**
     * Offset of value stored in the cluster block.
     */
    uint16_t      valueOffset;

    /**
     * The sorted order of value offsets. This is used when preparing
     * compressed batch.
     */
    uint16_t      sortedOrd;
};


/**
 * LcsHashTable implements a hash table that fits the hash entries and the overflow
 * nodes into one scratch buffer of size hashBlockSize.
 * The overflow nodes are of type LcsHashValueNode.
 */
class LcsHashTable
{
private:

    /**
     * Buffer to fit the hash entry array and hash value nodes in.
     */
    PBuffer           hashBlock;

    /**
     * Size of the buffer.
     */
    uint              hashBlockSize;

    /**
     * Size of the hash table, i.e. size of the hash entry array.
     */
    uint              hashTableSize;

    /**
     * Index of next hash value node to allocate.
     */
    uint              nextValueNode;

public:

    /**
     * Array indexed by the hash key value. Each entry points(using offset) to
     * a list of hash value nodes which have the same hash key.
     *
     * Entry array is indexed by the hash kay and stores the first offsets of
     * value nodes with the hash key. Though the offset value does not need to
     * be uint16_t, it saves memory to use uint16_t than uint(32 or 64 bit)
     * since the number of entries(i.e. hash table size) is limited to the
     * numbers that can fit into a block. This also implied that the next field
     * in LcsHashValueNode needs to be uint16_t.
     */
    uint16_t          *entry;

    /**
     * List of hash value nodes.
     */
    LcsHashValueNode  *valueNodes;

    inline explicit LcsHashTable();

    /**
     * Sets up fields in LcsHashTable.
     *
     * @param [in] hashBlockInit buffer to fit the variable length fields entry
     * and valueNodes
     *
     * @param [in] hashBlockSizeInit size of the buffer
     */
    void init(PBuffer hashBlockInit, uint hashBlockSizeInit);
    
    /**
     * Resets the hash table to prepare for encoding the next page.
     */
    inline void resetHash();

    /**
     * Resets the entries present in the current batch to prepare for encoding
     * the next batch.
     */
    inline void resetBatch();

    /**
     * Returns the hash table size.
     */
    inline uint numHashEntries();

    /**
     * Gives out the next hash value node for the caller to fill in interesting
     * information.
     */
    inline LcsHashValueNode* getNewValueNode();

    /**
     * Inserts a new node into the overflow chain.
     *
     * @param [in] key hash key of the newNode
     *
     * @param [in] newNode the new node to insert
     */
    inline void insertNewValueNode(uint key,  LcsHashValueNode *newNode);

    /**
     * Undoes the most recent insert.
     *
     * @param [in] key hash key of the node to remove. The most recently
     * inserted value nodes is always at the beginning of the overflow list
     * for a key
     */
    inline void undoNewValueNode(uint key);

    /**
     * Returns the first value node having a certain key value.
     *
     * @param [in] key hash key to locate
     *
     * @return pointer to the first value node
     */ 
    inline LcsHashValueNode* getFirstValueNode(uint key);
    
    /**
     * Returns the next value node following a value node
     *
     * @param [in] pValueNode pointer to the current LcsHashValueNode
     *
     * @return pointer to the next value node
     */ 
    inline LcsHashValueNode* getNextValueNode(LcsHashValueNode *pValueNode);

    /**
     * Checks if hash table is full.
     *
     * @param [in] leftOvers additional value nodes to accommodate with a
     * default value of 0
     *
     * @return true if hash table is full
     */
    inline bool isFull(uint leftOvers = 0);
};


/**
 * Actions for undoInsert()
 */
enum LcsUndoState { NOTHING, NEWENTRY, NEWBATCHVALUE };


/**
 * Context for undoing the most recent insert into LcsHash
 */
struct LcsUndoType
{
    /**
     * Action to be performed by undoInsert()
     */
    LcsUndoState     what;

    /**
     * The hash key value of the most recently inserted value.
     */
    uint             key;

    /**
     * The previous max value size for the undo to rollback to.
     */
    uint             origMaxValueSize;

    /**
     * The most recently inserted LcsHashValueNode
     */
    LcsHashValueNode *vPtr;

    /**
     * Constructor to reset all fields.
     */
    inline LcsUndoType();

    /**
     * Sets fields.
     *
     * @param [in] whatInit undo action
     *
     * @param [in] keyInit key value to undo
     *
     * @param [in] origMaxValueSizeInit maxValueSize to roll back to
     *
     * @param [in] vPtrInit points to value nodes to undo
     */
    inline void set(
        LcsUndoState whatInit,
        uint keyInit,
        uint origMaxValueSizeInit,
        LcsHashValueNode *vPtrInit);

    /**
     * Resets fields.
     */
    inline void reset();
};


/**
 * This class implements the compare logic to sort the indices to the key
 * offset vector,  by comparing the data values stored at thse offsets.
 * The sorted indices can then be used to encode the offsets when preparing
 * a compressed batch to write out.
 */
class LcsCompareColKeyUsingOffsetIndex
{
private:

    /**
     * Reference to the Hash Table where cluster column values are inserted into.
     */
    LcsHashTable            *hashTable;

    /**
     * Reference to Node Writer to access the cluster column value.
     */
    SharedLcsClusterNodeWriter    clusterBlockWriter;

    /**
     * Column Id for this cluster column.
     * Node Writer is for the whole cluster. It needs the column ID to get
     * the value for a column.
     */
    uint                     columnId;

    /**
     * Two tuple instances to store the values being compared
     */     
    TupleDataWithBuffer      colTuple1;
    TupleDataWithBuffer      colTuple2;

    /**
     * Tuple descriptor for the tuples being compared.
     */
    TupleDescriptor          colTupleDesc;
  
    UnalignedAttributeAccessor attrAccessor;

public:

    /**
     * Constructor.
     *
     * @param [in] clusterBlockWriterInit reference to clsuter node writer
     *
     * @param [in] hashTableInit reference to the hash table
     *
     * @param [in] colTupleDescInit reference to column tuple descriptor
     *
     * @param [in] columnIdInit which column in the cluster is being compared
     *
     * @param [in] attrAccessorInit attribute accessor of the column
     */
    explicit LcsCompareColKeyUsingOffsetIndex(
        SharedLcsClusterNodeWriter clusterBlockWriterInit,
        LcsHashTable *hashTableInit,
        TupleDescriptor const &colTupleDescInit,
        uint columnIdInit,
        UnalignedAttributeAccessor const &attrAccessor);

    ~LcsCompareColKeyUsingOffsetIndex() {}

    /**
     * Compares the two values stored at the offsets located by the two indices.
     * The implementation uses type information saved in colTupleDesc when
     * performing the comparison.
     *
     * @param [in] colKeyOffsetIndex1 index into the offset of the first value
     *
     * @param [in] colKeyOffsetIndex2 index into the offset of the second value
     *
     * @return true if value at offset located at colKeyOffsetIndex1 is
     * less than value at offset loocated at colKeyOffsetIndex2
     *
     */
    bool lessThan(const uint16_t colKeyOffsetIndex1,
        const uint16_t colKeyOffsetIndex2);
};

/**
 * Theis class is passed to std::sort. It implements the "less than" operator.
 */
class LcsCompare
{
private:

    /**
     * Reference to the class that keeps the compare context and
     * implements the comparison method.
     */
    SharedLcsCompareColKeyUsingOffsetIndex compareInstance;

public:

    /**
     * Constructor.
     *
     * @param [in] compareInstanceInit reference to the class
     * LcsCompareColKeyUsingOffsetIndex
     */
    inline explicit LcsCompare(
        SharedLcsCompareColKeyUsingOffsetIndex compareInstanceInit);

    /**
     * The less than operator.
     *
     * @param [in] colKeyOffsetIndex1 index into the offset of the first value
     *
     * @param [in] colKeyOffsetIndex2 index into the offset of the second value
     *
     * @return true if value at offset located at colKeyOffsetIndex1 is
     * less than value at offset loocated at colKeyOffsetIndex2
     *
     */
    inline bool operator()(const uint16_t colKeyOffsetIndex1,
        const uint16_t colKeyOffsetIndex2);
};


/**
 * LcsHash class is used by LcsclusterAppendExecStream.
 * LcsClusterAppendExecStream splits up the columns in a cluster and passes
 * column tuples(consisting of only one column) to LcsHash, which tranforms the
 * tuple and sends to the LcsClusterNodeWriter data pointers with encoded length
 * information.
 */
class LcsHash
{
private:
    
    /**
     * column for which this LcsHash structure is built.
     */
    uint                  columnId;

    /**
     * LcsHashTable object contains logic to fit the data strcture into one
     * block as well as logic to manage the hash entries and hash value nodes
     * in the hash table.
     */
    LcsHashTable          hash;

    /**
     * Block writer object used to add new value to a cluster block.
     */
    SharedLcsClusterNodeWriter clusterBlockWriter;

    /**
     * The column tuple descriptor.
     */
    TupleDescriptor       colTupleDesc;

    /**
     * The column currently begin compressed.
     */
    TupleDataWithBuffer   colTuple;

    /**
     * Attribute accessor of the column
     */
    UnalignedAttributeAccessor attrAccessor;

    /**
     * The column being compared against.
     */
    TupleDataWithBuffer   searchTuple;
    
    /**
     * Scratch memory to store the current column value being compressed.
     */
    boost::scoped_array<FixedBuffer> colTupleBuffer;
    
    /**
     * Number of unique values in the current batch.
     * The type of this field should be the same as that of LcsHashValOrd.
     */
    uint16_t              valCnt;

    /**
     * Largest value size in the current batch. The size includes the bytes
     * encoding the length information.
     */
    uint                  maxValueSize;

    /**
     * Structure containing info for undoing the most recent hash insert.
     */
    LcsUndoType           undo;

    /**
     * Hash seed values.
     */
    uint8_t              *magicTable;

    /**
     * Hash table statistics: number of hash key checks.
     */
    uint                  numChecks;

    /**
     * Hash table statistics: number of hash key matches.
     */
    uint                  numMatches;

    /**
     * Helper class to LcsCompare. It stores the comparison context.
     */
    SharedLcsCompareColKeyUsingOffsetIndex compareInst;

    /**
     * Compue hash key from value.
     *
     * @param [in] dataWithLen pointer to buffer with value and length info
     * encoded at the first 1 or 2 bytes.
     *
     * @return hash key
     */
    uint computeKey(PBuffer dataWithLen);
  
    /**
     * Search for ordinal using hash key and column data value.
     *
     * @param [in] key hash key to locate
     *
     * @param [in] dataWithLen pointer to data buffer with length info encoded
     *
     * @param [out] valOrd hash value node ordinal number
     *
     * @param [out] v hash value node if value is previously inserted
     *
     * @return true if a match in both the key and the data value is found
     */
    bool search(uint key, PBuffer dataWithLen,
        LcsHashValOrd *valOrd, LcsHashValueNode **v);
    
public:
    explicit LcsHash();
    ~LcsHash() {};

    /**
     * Initializes the LcsHash object.
     *
     * @param [in] hashBlockInit block to hold the hash table
     *
     * @param [in] clusterBlockWriterInit reference to the node writer
     *
     * @param [in] colTupleDescInit tuple descriptor for the column tuple
     *
     * @param [in] columnIdInit column ID for which this LcsHash is compressing.
     *
     * @param [in] blockSizeInit block size
     */
    void init(
        PBuffer hashBlockInit,
        SharedLcsClusterNodeWriter clusterBlockWriterInit,
        TupleDescriptor const &colTupleDescInit,            
        uint columnIdInit,
        uint blockSizeInit);
  
    /**
     * Inserts a single column tuple into the hash table. It also causes the
     * column value to be inserted into the cluster block if needed.
     *
     * @param [in] colTupleDatum column tuple to insert
     *
     * @param [out] valOrd hash value node ordinal
     *
     * @param [out] undoInsert true if this insert should be undone
     */
    void insert(
        TupleDatum &colTupleDatum,
        LcsHashValOrd *valOrd,
        bool *undoInsert);
    
    /**
     * Inserts a data buffer of a column into the hash table. It also causes
     * the column value to be inserted into the cluster block if needed.
     *
     * @param [in] dataWithLen data buffer of column tuple to insert
     *
     * @param [out] valOrd hash value node ordinal
     *
     * @param [out] undoInsert true if this insert should be undone
     */
    void insert(
        PBuffer dataWithLen,
        LcsHashValOrd *valOrd,
        bool *undoInsert);

    /**
     * Undoes the previous insert of a column tuple. This will be called if we
     * are trying to add all of the columns in a cluster and at least one can't
     * fit we will remove previously added cluster column values.
     *
     * @param [in] colTupleDatum column tuple just inserted
     */
    void undoInsert(TupleDatum &colTupleDatum);
    
    /**
     * Undoes the previous insert of a column data buffer.
     *
     * @param [in] dataWithLen data buffer to column tuple just inserted
     */
    void undoInsert(PBuffer dataWithLen);
    

    /**
     * Prepares a fixed or variable batch to be written to the cluster block.
     *
     * @param [in, out] rowArray upon input, this array holds value node
     * ordinals; at output, the array holds offsets for values in a batch
     *
     * @param [in] numRows number of values in a batch
     */
    void prepareFixedOrVariableBatch(uint8_t *rowArray, uint numRows);
    
    /**
     * Prepares a compressed batch to be written to the cluster block.
     *
     * @param [in, out] rowArray upon input, this array holds value nodes
     * ordinals; at output, it holds the indices to the offset array of the
     * column value stored on a cluster block.
     *
     * @param [in] numRows number of values in a batch
     *
     * @param [in] numVals (out) number of distinct values in a batch
     *
     * @param [out]  offsetIndexVector  at output, it holds the offsets of
     * the column value stored on a cluster block.
     */
    void prepareCompressedBatch(
        uint8_t *rowArray,
        uint     numRows,
        uint16_t *numVals,
        uint16_t *offsetIndexVector);
    
    /**
     * Clears the fixed values from batch to indicate the offset is not longer
     * useful because the key storage can be relocated between batches.
     */
    void clearFixedEntries();
    
    /**
     * Prepares LcsHash object for a new batch.
     *
     * @param [in] leftOvers number of left over hash value nodes from the
     * previous batch. The new batch needs to leave room apriori for these
     * nodes.
     */
    void startNewBatch(uint leftOvers);
    
    /**
     * Sets up hash with values from an existing cluster block. This is called
     * when appending to an existing block.
     *
     * @param [in] numVals number of values for this column
     *
     * @param [in] lastValOff offset of the last value stored for this column 
     *
     */
    void restore(uint numVals, uint16_t lastValOff);

    /**
     * Gets the maximum value length.
     *
     * @return data value length, including the bytes encoding the length
     * information.
     */
    inline uint getMaxValueSize();

    /**
     * Checks if the hash table is full.
     *
     * @param [in] leftOvers number of left over hash value nodes from the
     * previous batch. The next batch need to leave room apriori for these
     * nodes.
     *
     * @return true if hash table is full
     */
    inline bool isHashFull(uint leftOvers = 0);
};


/*******************************************************
  Definitions of inline methods for class LcsHashValOrd
 *******************************************************/

inline LcsHashValOrd::LcsHashValOrd(LcsHashValOrd const &other)
{
    valOrd = other.valOrd;
}

inline LcsHashValOrd& LcsHashValOrd::operator=(uint16_t valOrdInit)
{
    setValOrd(valOrdInit);
    return *this;
}

inline uint16_t LcsHashValOrd::getValOrd()
{
    return (uint16_t)(valOrd & ~(1<<15));
}

inline void LcsHashValOrd::setValOrd(uint16_t valOrdInit)
{
    valOrd = (valOrdInit & ~(1<<15));
}

inline bool LcsHashValOrd::isValueInBatch()
{
    return (valOrd & (1<<15));
}

inline void LcsHashValOrd::setValueInBatch()
{
    valOrd |= 1<<15;
}

inline void LcsHashValOrd::clearValueInBatch()
{
    valOrd &= ~(1<<15);
}   


/*****************************************************
  Definitions of inline methods for class LcsUndoType
 *****************************************************/

inline LcsUndoType::LcsUndoType() 
{
    reset();
}

inline void LcsUndoType::set(
    LcsUndoState whatInit,
    uint keyInit,
    uint origMaxValueSizeInit,
    LcsHashValueNode *vPtrInit)
{
    what = whatInit;
    key = keyInit;
    origMaxValueSize = origMaxValueSizeInit;;
    vPtr = vPtrInit;    
}

inline void LcsUndoType::reset()
{
    what = NOTHING;
    key = 0;
    origMaxValueSize = 0;
    vPtr = 0;    
}


/******************************************************
  Definitions of inline methods for class LcsHashTable
 ******************************************************/

inline LcsHashTable::LcsHashTable()    
{
    hashBlock = NULL;
    hashTableSize = 0;
    nextValueNode = 0;
}

inline void LcsHashTable::resetHash()
{
    memset(hashBlock, 0, hashBlockSize);
    nextValueNode = 0;
}

inline void LcsHashTable::resetBatch()
{
    for(int i=0; i < nextValueNode; i++) {
        (&(valueNodes[i].valueOrd))->clearValueInBatch();
    }
}

inline uint LcsHashTable::numHashEntries()
{
    return hashTableSize;
}

inline LcsHashValueNode* LcsHashTable::getNewValueNode()
{
    return &(valueNodes[nextValueNode]);
}

inline void LcsHashTable::insertNewValueNode(uint key,  LcsHashValueNode *newNode)
{
    newNode->next = (uint16_t)entry[key];
    
    /*
      Insert at the beginning.
    */
    entry[key] = (uint16_t)((uint8_t*)newNode - hashBlock);
    
    /*
      Bump up the value for the next node to give out.
    */
    nextValueNode++;        
}

inline void LcsHashTable::undoNewValueNode(uint key)
{
    entry[key] =(uint16_t) 
        ((LcsHashValueNode*)(hashBlock + entry[key]))->next;
    nextValueNode--;
}

inline LcsHashValueNode* LcsHashTable::getFirstValueNode(uint key)
{
    uint16_t offset = entry[key];
    
    if (offset)
        return (LcsHashValueNode*) (hashBlock + offset);
    else
        return NULL;
}

inline LcsHashValueNode* LcsHashTable::getNextValueNode(LcsHashValueNode *pValueNode)
{
    uint16_t offset = pValueNode->next;
    
    if (offset) 
        return (LcsHashValueNode*) (hashBlock + offset);
    else
        return NULL;
}

inline bool LcsHashTable::isFull(uint leftOvers)
{
    /*
      need to leave one spot for sorting and leftsOvers that
      will be added
    */
    return ((PBuffer)(&(valueNodes[nextValueNode])
            + (leftOvers + 1) * sizeof(LcsHashValueNode))
        >= (PBuffer ) (hashBlock + hashBlockSize));
}    


/****************************************************
  Definitions of inline methods for class LcsCompare
 ****************************************************/

inline LcsCompare::LcsCompare(
    SharedLcsCompareColKeyUsingOffsetIndex compareInstanceInit)
{
    compareInstance = compareInstanceInit;
}

inline bool LcsCompare::operator()(const uint16_t colKeyOffsetIndex1,
    const uint16_t colKeyOffsetIndex2)
{
    return compareInstance->lessThan(colKeyOffsetIndex1, colKeyOffsetIndex2);
}


/*************************************************
  Definitions of inline methods for class LcsHash
 *************************************************/

inline uint LcsHash::getMaxValueSize()
{
    return maxValueSize;
}

inline bool LcsHash::isHashFull(uint leftOvers)
{
    return hash.isFull(leftOvers);
}   

FENNEL_END_NAMESPACE

#endif

// End LcsHash.h

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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/colstore/LcsHash.h"

FENNEL_BEGIN_CPPFILE("$Id$");

/*
 * need to play with several value to get a good MagicTable
 */
static uint8_t MagicTable[256] = 
{
    1,    87,   49,   12,   176,  178,  102,  166,  121,  193,  6,    84,
    249,  230,  44,   163,  14,   197,  213,  181,  161,  85,   218,  80,
    64,   239,  24,   226,  236,  142,  38,   200,  110,  177,  104,  103,
    141,  253,  255,  50,   77,   101,  81,   18,   45,   96,   31,   222,
    25,   107,  190,  70,   86,   237,  240,  34,   72,   242,  20,   226,
    236,  142,  38,   235,  97,   234,  57,   22,   60,   250,  82,   175,
    208,  5,    127,  199,  111,  62,   135,  248,  174,  169,  211,  58,
    66,   154,  106,  195,  245,  171,  17,   187,  182,  179,  0,    243,
    132,  56,   148,  75,   128,  133,  158,  100,  130,  126,  91,   13,
    153,  246,  216,  219,  119,  68,   223,  78,   83,   88,   201,  99,
    122,  11,   92,   32,   136,  114,  52,   10,   138,  30,   48,   183,
    156,  35,   61,   26,   143,  74,   251,  94,   129,  162,  63,   152,
    170,  7,    115,  167,  241,  206,  3,    150,  55,   59,   151,  220,
    90,   53,   23,   131,  125,  173,  15,   238,  79,   95,   89,   16,
    105,  137,  225,  224,  217,  160,  37,   123,  118,  73,   2,    157,
    46,   116,  9,    145,  134,  228,  207,  212,  202,  215,  69,   229,
    27,   188,  67,   124,  168,  252,  42,   4,    29,   108,  21,   247,
    19,   205,  39,   203,  233,  40,   186,  147,  198,  192,  155,  33,
    164,  191,  98,   204,  165,  180,  117,  76,   140,  36,   210,  172,
    41,   54,   159,  8,    185,  232,  113,  196,  231,  47,   146,  120,
    51,   65,   28,   144,  254,  221,  93,   189,  194,  139,  112,  43,
    71,   109,  184,  209
};


/********************************************
 * Public member functions of LcsHash class *
 ********************************************/

LcsHash::LcsHash()
{
    columnId                = 0;
    valCnt                  = 0;
    maxValueSize            = 0;
    magicTable              = MagicTable;
    numMatches              = 0;
    numChecks               = 0;
}

void LcsHash::init(
    PBuffer hashBlockInit,
    SharedLcsClusterNodeWriter clusterBlockWriterInit,
    TupleDescriptor const &colTupleDescInit,
    uint columnIdInit,
    uint blockSizeInit)
{
    /*
     * clears and sets up hash block
     */
    memset(hashBlockInit,0,blockSizeInit);
    hash.init(hashBlockInit,  blockSizeInit);
    
    columnId                = columnIdInit;
    valCnt                  = 0;
    maxValueSize            = 0;
    clusterBlockWriter      = clusterBlockWriterInit;

    assert(colTupleDescInit.size() == 1);
    colTupleDesc            = colTupleDescInit;
    attrDesc = colTupleDesc[0];

    /*
     * Temporary in-memory representation for the tuple this LcsHash
     * is working on.
     */
    colTuple.computeAndAllocate(colTupleDesc);
    searchTuple.computeAndAllocate(colTupleDesc);

    /*
     * colTupleBuffer provides storage for storing the length as well the data
     * buffer in a compat format described in TupleData.h. The length could take
     * up to 2 bytes.
     */
    colTupleBuffer.reset(
        new FixedBuffer[colTupleDesc[0].getMaxLcsLength()]);
    
    compareInst = SharedLcsCompareColKeyUsingOffsetIndex(
        new LcsCompareColKeyUsingOffsetIndex(
            clusterBlockWriter, &hash, colTupleDesc, columnId, attrDesc));
}

void LcsHash::insert(
    TupleDatum &colTupleDatum,
    LcsHashValOrd   *valOrd,
    bool       *undoInsert)
{
    /*
     * gets the data buffer with length encoded
     */
    PBuffer dataWithLen = colTupleBuffer.get();
    colTupleDatum.storeLcsDatum(dataWithLen, attrDesc);

    insert(dataWithLen, valOrd, undoInsert);
}


void LcsHash::insert(
    PBuffer     dataWithLen,
    LcsHashValOrd   *valOrd,
    bool       *undoInsert)
{
    uint        key;
    uint16_t    newValueOffset;
    LcsHashValueNode  *vPtr=0;
    TupleStorageByteLength storageLength;
    
    /*
     * Compression mode could change dynamically so we have to check everytime.
     * If this batch will not be compressed, then there is no reason to
     * generate a real hash code.  Hash code generation is expensive, so
     * try to avoid it.
     */
    bool noCompress = clusterBlockWriter->noCompressMode(columnId);
    key = noCompress ? 0 : computeKey(dataWithLen);

    *undoInsert     = false;
        
    /*
     * If value is not in hash, or
     * if we are not in compress mode
     * (in which case we allow duplicates in the hash table),
     * then adds value to the hash.
     */
    if(noCompress || !search(key, dataWithLen, valOrd, &vPtr))
    {
        LcsHashValueNode       *newNode;

        /*
         * If hash table is full,  or
         * if the cluster page is full
         * then returns and indicates the need to undo the insert.
         */
        *undoInsert =
            hash.isFull() ||
            !clusterBlockWriter->addValue(columnId, dataWithLen,
                &newValueOffset);

        if (*undoInsert)
        {
            /*
             * Prepares undo action.
             */
            undo.set(NOTHING, key, maxValueSize, 0);
            return;
        }

        /*
         * Inserts a new node only when the above call does not return undoInsert.
         * If a new node is inserted but the undoInsert above is true, the
         * subsequent undoInsert() call will not roll back the new node
         * correctly if undo.what is not set to NEWENTRY(the default value is
         * NOTHING).
         */
        newNode = hash.getNewValueNode();
        newNode->valueOffset = newValueOffset;
        *valOrd = valCnt ++;
        valOrd->setValueInBatch(); 
        newNode->valueOrd = *valOrd;

        hash.insertNewValueNode(key, newNode);

        /*
         * Prepares undo action.
         */
        undo.set(NEWENTRY, key, maxValueSize, 0);
        
        storageLength = TupleDatum().getLcsLength(dataWithLen, attrDesc);
        
        if (storageLength > maxValueSize)
            maxValueSize = storageLength;

    }

    /*
     * We found the value in the hash (from the Search() call above),
     * so it is already in the block,
     * but it still may not be part of the current batch.
     * Whether it is or not, call addValue(), so that we can adjust
     * space left in the block.
     */
    else
    {
        bool bFirstTimeInBatch = !valOrd->isValueInBatch();

        *undoInsert = !clusterBlockWriter->addValue(columnId, bFirstTimeInBatch);
        
        if(*undoInsert)
        {
            /*
             * Prepares undo action.
             */
            undo.set(NOTHING, key, maxValueSize, 0);
            return;
        }

        (vPtr->valueOrd).setValueInBatch();
        *valOrd = vPtr->valueOrd;

        /*
         * Prepares undo action.
         */
        if ( bFirstTimeInBatch )
        {
            undo.set(NEWBATCHVALUE, key, maxValueSize, vPtr);
        }
        else
        {
            undo.set(NOTHING, key, maxValueSize, 0);
        }
    }
    /*
     * Otherwise the value is already in the hash, and the current batch
     * already has a pointer to that value, so don't do anything.
     */
}

void LcsHash::undoInsert(TupleDatum &colTupleDatum)
{
    /*
     * gets the data buffer with length encoded
     */
    PBuffer dataWithLen = colTupleBuffer.get();
    colTupleDatum.storeLcsDatum(dataWithLen, attrDesc);

    undoInsert(dataWithLen);

}

void LcsHash::undoInsert(PBuffer dataWithLen)
{
    switch(undo.what)
    {
    case NOTHING:
        {
            /*
             * Value already existed in the batch.
             */
            clusterBlockWriter->undoValue(columnId, NULL, false); 
            break;
        }
    case NEWENTRY:
        {
            /*
             * First time in block.
             *
             * To remove the a new value entry
             * 1) decrements the total count, 
             * 2) resets location where next value entry will gox
             * 3) removes entry from hash
             * 4) resets maximum value size
             * 5) removes value from block
             */
            valCnt--;
            hash.undoNewValueNode(undo.key);
            maxValueSize = undo.origMaxValueSize;
            clusterBlockWriter->undoValue(columnId, dataWithLen, true); 
            break;
        }
    case NEWBATCHVALUE:
        {
            /*
             * Already in block but first time in batch.
             * Need to remove value from batch
             */
            clusterBlockWriter->undoValue(columnId, NULL, true); 
            (&undo.vPtr->valueOrd)->clearValueInBatch();        
            break;
        }
    }
    undo.reset();
}

bool LcsHash::search(
    uint      key,
    PBuffer   dataWithLen,
    LcsHashValOrd *valOrd,
    LcsHashValueNode **vNode = 0)
{
    LcsHashValueNode       *valueNode;
    bool    compareRes;

    colTuple[0].loadLcsDatum(dataWithLen, attrDesc);
    
    for( valueNode = hash.getFirstValueNode(key);
         valueNode != NULL;
         valueNode = hash.getNextValueNode(valueNode))
     {
        numChecks++;

        /*
         * Skips invalid hash entries.
         * Entries were invalidated by clearFixedEntries.
         */
        if( valueNode->valueOffset == 0 )
            continue;

        searchTuple[0].loadLcsDatum(
            clusterBlockWriter->getOffsetPtr(columnId, valueNode->valueOffset),
            attrDesc);

        compareRes = colTupleDesc.compareTuples(colTuple, searchTuple);

        /*
         * Prepare for next loadLcsDatum.
         */
        searchTuple.resetBuffer();
        
        if (compareRes == 0)
        {
            numMatches++;
            *valOrd = valueNode->valueOrd;
            if(vNode) *vNode = valueNode;
            colTuple.resetBuffer();
            return true;
        }
    }
    
    colTuple.resetBuffer();    
    /*
     * No Match.
     */
    return false;
}


void LcsHash::prepareCompressedBatch(
    uint8_t *rowArray,
    uint     numRows,
    uint16_t *numVals,
    uint16_t *offsetIndexVector)
{
    uint16_t    i;
    uint16_t    *rowWORDArray=(uint16_t*)rowArray;
    *numVals        = 0;
  
    /*
     * Puts all value ordinals in batch in Vals array.
     */
    for(i=0; i < valCnt; i++) {
        if ((hash.valueNodes[i].valueOrd).isValueInBatch()) {
            hash.valueNodes[i].sortedOrd = *numVals;
            offsetIndexVector[(*numVals)++] = i;
        }
    }

    /*
     * Sorts the value ordinals based on the key values.
     */
    std::sort(offsetIndexVector, offsetIndexVector + (*numVals),
              LcsCompare(compareInst));
    
    /*
     * Now OffsetIndexVector is sorted. Sets sortedOrd,  which is basically index
     * into the OffsetIndexVector,  in valueNodes array.
     */
    for( i = 0; i < *numVals; i++ ) {   
        hash.valueNodes[offsetIndexVector[i]].sortedOrd = i;
    }

    /*
     * Having stored the sortedOrd away, replaces value Ordinals in
     * OffsetIndexVector array with offset from the valueNodes array. Now
     * OffsetIndexVector will contain offsets sorted based on the values they
     * point to.
     */
    for( i = 0; i < *numVals; i++) {
        offsetIndexVector[i] =
            hash.valueNodes[offsetIndexVector[i]].valueOffset;
    }
  
    /*
     * Stores the index to OffsetIndexVector in the Row array. Now the
     * rowWORDArray contains indices to the OffsetIndexVector,  which conains
     * offsets sorted based on the column values they point to.
     */
    for( i = 0; i < numRows; i++ ) {
        rowWORDArray[i] = hash.valueNodes[rowWORDArray[i]].sortedOrd;
    }
}

void LcsHash::prepareFixedOrVariableBatch(
    uint8_t *rowArray,
    uint     numRows)
{
    uint            i;
    uint16_t        *rowWORDArray=(uint16_t*)rowArray;
    LcsHashValueNode       *pValueNodes;

    pValueNodes = hash.valueNodes;

    /*
     * Stores the offset to the column values in rowWORDArray
     */
    for( i = 0; i < numRows; i++ )
        rowWORDArray[i] = pValueNodes[rowWORDArray[i]].valueOffset;
}


void LcsHash::clearFixedEntries()
{
    /*
     * Only clears entries if the next batch is not guaranteed to be fixed mode.
     */
    if (!clusterBlockWriter->noCompressMode(columnId)) {
        for (uint i = 0; i < valCnt; i++) {
            if ((hash.valueNodes[i].valueOrd).isValueInBatch()) {
                hash.valueNodes[i].valueOffset=0;
            }
        }
    }
}

void LcsHash::restore(uint numVals, uint16_t lastValOff)
{
    uint            i;
    uint            key;
    PBuffer         dataWithLen;
    LcsHashValueNode      *newNode;
    LcsHashValOrd   dummy;
    LcsHashValOrd   valOrd;
    TupleStorageByteLength storageLength;

    /*
     * Compression mode could change dynamically so we have to check everytime.
     * If this batch will not be compressed, then there is no reason to
     * generate a real hash code.  Hash code generation is expensive, so
     * try to avoid it.
     */
    bool noCompress = clusterBlockWriter->noCompressMode(columnId);
  
    for( i = 0; i < numVals && !(hash.isFull()); i++ )
    {
        dataWithLen = clusterBlockWriter->getOffsetPtr(columnId,lastValOff);
        key = noCompress ? 0 : computeKey(dataWithLen);
  
        /*
         * If value is not in hash, or if we are not in compress mode (in which
         * case we allow duplicates in the hash table), then adds value to the
         * hash.
         */
        if (noCompress || !search(key, dataWithLen, &dummy))
        {
            newNode = hash.getNewValueNode();
            
            valOrd = valCnt++;
            newNode->valueOrd = valOrd; 
            newNode->valueOffset=(uint16_t)lastValOff;

            hash.insertNewValueNode(key,  newNode);

            storageLength = TupleDatum().getLcsLength(dataWithLen, attrDesc);
            if( storageLength > maxValueSize )
                maxValueSize = storageLength;
        }
      
        lastValOff = clusterBlockWriter->getNextVal(columnId,
            (uint16_t)lastValOff);
    }
}

void LcsHash::startNewBatch(uint leftOvers)
{
    /*
     * If the hash is full we need to start over. Otherwise just clear the
     * entries used in building th eprevious batch.
     */
    if(clusterBlockWriter->noCompressMode(columnId) ||
        hash.isFull(leftOvers))
    {
        hash.resetHash();
        valCnt       = 0;
        maxValueSize = 0;
    }
    else {
        hash.resetBatch();
    }
}


/*********************************************
 * Private member functions of LcsHash class *
 *********************************************/

uint LcsHash::computeKey(PBuffer dataWithLen)
{
    uint8_t  keyVal[2] = {0,0}, oldKeyVal[2]={0,17};
    uint     i, colSize = TupleDatum().getLcsLength(dataWithLen, attrDesc);

    /*
     * Compute the hash key over all the bytes, inlcuding the length
     * bytes. This saves the implicit memcpy in loadLcsDatum.
     */
    for( i = 0;
         i < colSize;
         oldKeyVal[0]=keyVal[0], oldKeyVal[1]=keyVal[1], i++, dataWithLen++)
    {
        keyVal[0] = magicTable[oldKeyVal[0] ^ *dataWithLen];
        keyVal[1] = magicTable[oldKeyVal[1] ^ *dataWithLen];
    }

    return ((keyVal[1]<<8) + keyVal[0]) % hash.numHashEntries();
}


/************************************
  Member functions of helper classes
 ************************************/

void LcsHashTable::init(PBuffer hashBlockInit, uint hashBlockSizeInit)
{
    hashBlock     = hashBlockInit;
    hashBlockSize = hashBlockSizeInit;
    memset(hashBlock, 0, hashBlockSize);
    
    /*
     * hashTableSize is the size for entry array. Reserve space assuming no
     * one valueNode for each entry(no empty entry, and no overflow).
     */
    hashTableSize = hashBlockSize/(sizeof(uint16_t) + sizeof(LcsHashValueNode));

    /*
     * entry points to the beginning of the hashBlock
     */
    entry = (uint16_t *)hashBlock;

    /*
     * valueNodes follow the entry array.
     */
    valueNodes = (LcsHashValueNode *)(hashBlock + sizeof(uint16_t) * hashTableSize);

    /*
     * Starts from the very first valueNodes.
     */
    nextValueNode = 0;
}


LcsCompareColKeyUsingOffsetIndex::LcsCompareColKeyUsingOffsetIndex(
    SharedLcsClusterNodeWriter clusterBlockWriterInit,
    LcsHashTable *hashTableInit,
    TupleDescriptor const &colTupleDescInit,
    uint columnIdInit,
    TupleAttributeDescriptor const &attrDescInit)
{
    hashTable      = hashTableInit;
    clusterBlockWriter = clusterBlockWriterInit;
    columnId = columnIdInit;
    colTupleDesc = colTupleDescInit;
    colTuple1.computeAndAllocate(colTupleDesc);
    colTuple2.computeAndAllocate(colTupleDesc);
    attrDesc = attrDescInit;
    
    /*
     * Both tuples should have just one column.
     */
    assert((colTuple1.size() == 1) && (colTuple2.size() == 1));
}


bool LcsCompareColKeyUsingOffsetIndex::lessThan(
    const uint16_t colKeyOffsetIndex1,
    const uint16_t colKeyOffsetIndex2)
{
    bool isLessThan = false;
    
    /*
     * Using index, locates the offset in the hash table then using offset,
     * constructs TupleDatum of the column "columnId".
     */
    colTuple1[0].loadLcsDatum(
        clusterBlockWriter->getOffsetPtr(
            columnId, hashTable->valueNodes[colKeyOffsetIndex1].valueOffset),
        attrDesc);
    
    colTuple2[0].loadLcsDatum(
        clusterBlockWriter->getOffsetPtr(
            columnId, hashTable->valueNodes[colKeyOffsetIndex2].valueOffset),
        attrDesc);
    
    /*
     * The std::sort interface requires a "less than" operator. Returns true if
     * first value is less than the second value.
     */
    isLessThan = colTupleDesc.compareTuples(colTuple1, colTuple2) < 0;

    colTuple1.resetBuffer();
    colTuple2.resetBuffer();

    return (isLessThan);
}

FENNEL_END_CPPFILE("$Id$");

// End LcsRowScanExecStream.cpp

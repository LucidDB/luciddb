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

#include "fennel/lucidera/bitmap/LbmEntry.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmEntry::LbmEntry()
{
    scratchBuffer = NULL;

    /*
     * Reset buffers and offsets
     */
    pSegDescStart = NULL;
    pSegStart = NULL;

    resetSegment();
}


void LbmEntry::init(
    PBuffer scratchBufferInit,
    uint scratchBufferSizeInit,
    TupleDescriptor const &tupleDesc)
{
    scratchBuffer = scratchBufferInit;
    /*
     * Leave room to write the last segment descriptor which has an optional
     * length field of 3 bytes.
     */
    scratchBufferSize = scratchBufferSizeInit;
    scratchBufferUsableSize = scratchBufferSizeInit - LbmZeroLengthExtended;

#ifdef DEBUG
    /*
     * Overwrite the buffer with FFs.
     */
    memset(scratchBuffer, 0xFF, scratchBufferSizeInit);
#endif

    /*
     * Set up the entryTuple
     */
    entryTuple.compute(tupleDesc);
    currentEntrySize = keySize = 0;
    /**
     * Compute keysize based on max space that can be occupied by
     * the keys
     */
    for (uint i = 0; i < tupleDesc.size() - 2; i++) {
        keySize += tupleDesc[i].cbStorage;
    }
}


void LbmEntry::setEntryTuple(TupleData const &indexTuple)
{
    /*
     * RID field
     */
    uint RIDField = entryTuple.size() - 3;

    /*
     * Next to the last.
     */
    uint segmentDescField = entryTuple.size() - 2;

    /*
     * Last field.
     */
    uint segmentField = entryTuple.size() - 1;

    /*
     * Now copy everything from indexTuple to entryTuple, using scratchBuffer
     * to store tuple.
     */
    currentEntrySize = 0;

    for (int i = 0; i < entryTuple.size(); i ++) {
        
        if (i != segmentField) {
            entryTuple[i].pData = scratchBuffer + currentEntrySize;
        } else {
            /*
             * Move the segments to the end of the buffer. Segments grow
             * backward in memory.
             */
            entryTuple[i].pData = scratchBuffer + scratchBufferSize 
                                  - indexTuple[i].cbData;
        }
        
        entryTuple[i].memCopyFrom(indexTuple[i]);

        if (entryTuple[i].isNull()) {
            entryTuple[i].cbData = 0;
        }
        /**
         * Once we hit the rid field, bump up the currentEntrySize past
         * the max space used by the keys
         */
        if (i == RIDField) {
            currentEntrySize = keySize;
        } else {
            currentEntrySize += entryTuple[i].cbData;
        }
    }

    /*
     * We only know the available buffer size after getting the
     * key values since the column(s) might be of variable size.
     *
     * The current entry might have been initialized from an existing entry
     * in which case, the last bytes reserved for extended zero length might
     * have been used. The total length in this case will exceed
     * scratchBufferUsableSize but not scratchBufferSize.
     */
    assert(currentEntrySize <= scratchBufferSize);

    /*
     * Set up the Segment and Segment descriptor pointers. There are a few
     * cases when LBmEntry is initialized, and the buffer setup is different
     * for each case.
     * 1. LbmGenerator calls init with <key1>...<keyN><actualRID><NULL><NULL>
     * 2. LbmSplicer calls init with <Key1>..<keyN><SingletonRID><NULL><NULL>
     * 3. LbmSplicer calls init with <Key1>..<keyN><StartRID><value><value>
     * 4. LbmSplicer calls init with <Key1>..<keyN><StartRID><NULL><value>
     *
     * For case 1 and 2, the setup for the variable part(segmentDesc/segment)
     * is the same(point to begin and end of the available buffer).
     */

    startRID = *((LcsRid *)entryTuple[RIDField].pData);

    if (isSingleton(indexTuple)){
        /*
         * Case 1, or 2
         */
        pSegDescStart = pSegDescEnd = scratchBuffer + keySize;
        pSegStart     = pSegEnd     = scratchBuffer + scratchBufferSize;

        resetSegment();
    } else {
        /*
         * Case 3, or 4(single bitmap)
         * From a bitmap entry. Check startRID is well-formed.
         */
        assert((startRID) % LbmOneByteSize == (LcsRid)0);

        uint segLength = entryTuple[segmentField].cbData;
        /*
         * Check if this is a single bitmap, or a compressed bitmap.
         */
        if (isSingleBitmap(entryTuple)) {
            /*
             * Add segment descriptors to single bitmaps.
             * It should fit since an on-disk single bitmap always come
             * from an in-memory compressed bitmap; so reverting a single bitmap
             * to compressed bitmap should always succeed.
             */
            pSegDescStart = pSegDescEnd = scratchBuffer + keySize;
            uint reservedSpace = 0;
            bool ret = addSegDesc(reservedSpace, segLength);
            assert (ret);
        } else {
            pSegDescStart = (PBuffer)entryTuple[segmentDescField].pData;
            pSegDescEnd = pSegDescStart + entryTuple[segmentDescField].cbData;
        }

        pSegStart = scratchBuffer + scratchBufferSize;
        pSegEnd = pSegStart - segLength;
        
        resetSegment();
    }
}


bool LbmEntry::setRIDNewSegment(LcsRid rid)
{
    if (!openNewSegment(rid)) {
        return false;
    }

    setRIDCurrentSegByte(rid);
    return true;    
}


bool LbmEntry::setRIDAdjacentSegByte(LcsRid rid)
{
    assert (!isSingleBitmap());

    if (currSegLength == LbmMaxSegSize) {
        /*
         * Current segment is full.
         * Need New segement and new segment descriptor.
         * First close the current segment in this entry. Use the
         * closeCurrentSegment() interface which does not encode any zeros
         * since the next segment is adjacent.
         */
        closeCurrentSegment();
        return setRIDNewSegment(rid);
    }

    if (currentEntrySize + 1 > scratchBufferUsableSize) {
        closeCurrentSegment();
        return false;
    }

    pSegEnd--;
    currSegByte = pSegEnd;
    currSegByteStartRID = roundToByteBoundary(rid);
    currSegLength ++;

    *currSegByte = (uint8_t)(1 << (opaqueToInt(rid) % LbmOneByteSize));

    currentEntrySize += 1;
    /*
     * We are growing the current segment. If there is a descriptor for this
     * segment, add one more segment byte.
     */
    if (isSegmentOpen()) {
        setSegLength(*currSegDescByte, currSegLength);
    }

    return true;
}


bool LbmEntry::openNewSegment(LcsRid rid)
{
    assert (!isSegmentOpen());
    assert (pSegDescEnd);

    if (currentEntrySize + 2 > scratchBufferUsableSize) {
        return false;
    }
    
    pSegEnd--;
    currSegByte = pSegEnd;
    *currSegByte = 0;

    currSegDescByte = pSegDescEnd;
    pSegDescEnd ++;

    currSegByteStartRID = roundToByteBoundary(rid);
    currSegLength = 1;

    /*
     * This is a new segment, set number of segment byte to be one.
     * Note that the stored length is actually (length - 1). So we
     * store value zero here.
     */
    setSegLength(*currSegDescByte, currSegLength);
    currentEntrySize += 2;

    return true;
}


void LbmEntry::openLastSegment()
{
    assert (!isSegmentOpen());
    assert (pSegEnd);

    uint lastZeroRIDs = 0;
    uint rowCount = getRowCount(currSegDescByte, lastZeroRIDs);
    
    pSegDescEnd = currSegDescByte + 1;
    currSegLength = getSegLength(*currSegDescByte);
    setSegLength(*currSegDescByte, currSegLength);

    // still point to the same last segment byte
    currSegByte = pSegEnd;
    currSegByteStartRID = 
        startRID + rowCount - lastZeroRIDs - LbmOneByteSize;

    // backtrack to exclude the extended zero length bytes
    currentEntrySize -= getZeroLengthByteCount(*currSegDescByte);
}


void LbmEntry::closeCurrentSegment()
{
    assert (isSegmentOpen());
    assert (currSegLength >= 1 && currSegLength <= LbmMaxSegSize);

    setSegLength(*currSegDescByte, currSegLength);
    resetSegment();
}


bool LbmEntry::closeCurrentSegment(LcsRid rid)
{
    if (!isSegmentOpen()) {
        // segment already closed.
        return true;
    }

    int ridDistance = 0;

    assert(currSegLength >= 1 && currSegLength <= LbmMaxSegSize);

    setSegLength(*currSegDescByte, currSegLength);
    
    /*
     * Count of zero bits in bytes. Each byte represents 8 RIDs.
     */
    ridDistance = 
        opaqueToInt(roundToByteBoundary(rid) - currSegByteStartRID) 
        / LbmOneByteSize - 1;

    if (ridDistance <= 0) {
        /*
         * Boundary case: request writing the directory even though the rid is
         * in the same seg, or in an adjacent segment. In both cases, there is
         * no rid gap between the two segments.
         */
    } else {
        if (ridDistance <= LbmZeroLengthCompact) {
            /*
             * Can encode the zero bits directly in the segment descriptor.
             */
            *(currSegDescByte) |= (uint8_t) (ridDistance & LbmZeroLengthMask);
        } else {
            uint lengthBytes = value2ByteArray(ridDistance, 
                                               pSegDescEnd,
                                               LbmZeroLengthExtended);
            
            if (lengthBytes) {
                /*
                 * No check of remaining buffer size here since we always
                 * leaves LbmZeroLengthExtended bytes in the buffer to encode
                 * the zero bits. 
                 */
                *(currSegDescByte) |= 
                    (uint8_t) ((lengthBytes + LbmZeroLengthCompact) 
                               & LbmZeroLengthMask);
                pSegDescEnd += lengthBytes;
                currentEntrySize += lengthBytes;
            } else {
                /*
                 * The ridDistance can not be encoded in LbmZeroLengthExtended
                 * bytes.
                 *
                 * In this special case, this segment descriptor will be the
                 * last one on the current LbmEntry. The descriptor will encode
                 * that 0 bits follows the current segment. The caller will
                 * construct a new LbmEntry with rid, and very likely there's a
                 * gap between the last RID encoded by the current LbmEntry and
                 * the startRID of this new LbmEntry. This means that RIDs
                 * might not be densely encoded in an ordered sequence of 
                 * LbmEntries.
                 */
                resetSegment();
                return false;
            }
        }
    }
    resetSegment();
    return true;
}


uint LbmEntry::getRowCount()
{
    PBuffer lastSegDescByte = NULL;
    uint lastZeroRIDs = 0;

    return getRowCount(lastSegDescByte, lastZeroRIDs);
}


uint LbmEntry::getCompressedRowCount(
    PBuffer pDescStart,
    PBuffer pDescEnd,
    PBuffer &lastSegDescByte,
    uint &lastZeroRIDs)
{
    PBuffer p1 = pDescStart;
    uint lastLengthDesc = 0;
    uint lastLengthDescBytes, lastZeroBytes;
    uint rowCount = 0;
    
    while (p1 < pDescEnd) {
        /*
         * Count the RIDs in bitmaps.
         */
        rowCount += getSegLength(*p1) * LbmOneByteSize;
        
        /*
         * Count the RIDs in Descriptors.
         */
        lastSegDescByte = p1;
        lastLengthDesc = *p1 & LbmZeroLengthMask;
        p1 ++;
        
        if (lastLengthDesc <= LbmZeroLengthCompact) {
            lastZeroBytes = lastLengthDesc;
        } else {
            lastLengthDescBytes = lastLengthDesc - LbmZeroLengthCompact;
            /*
             * Translate number of bytes into number of RIDs(bits).
             */
            lastZeroBytes = 
                byteArray2Value(p1, lastLengthDescBytes);
            p1 += lastLengthDescBytes;
        }
        
        lastZeroRIDs = lastZeroBytes * LbmOneByteSize;
        rowCount += lastZeroRIDs;
    }
    return rowCount;
}


uint LbmEntry::getRowCount(
    PBuffer &lastSegDescByte,
    uint &lastZeroRIDs)
{
    assert (!isSingleBitmap());

    uint rowCount = 0;

    if (isSingleton()) {
        rowCount = 1;
        lastSegDescByte = NULL;
        lastZeroRIDs = 0;
    } else {
        rowCount = 
            getCompressedRowCount(pSegDescStart, pSegDescEnd, 
                lastSegDescByte, lastZeroRIDs);
    }
    return rowCount;
}


uint LbmEntry::getRowCount(TupleData const &inputTuple)
{
    uint rowCount = 0;

    if (isSingleton(inputTuple)) {
        rowCount = 1;
    } else if (isSingleBitmap(inputTuple)) {
        /*
         * A single bitmap
         */
        rowCount = 
            inputTuple[inputTuple.size() - 1].cbData * LbmOneByteSize;
    } else {
        PBuffer lastSegDescByte = NULL;
        uint lastZeroRIDs = 0;
        PBuffer pDescStart = (PBuffer)inputTuple[inputTuple.size() - 2].pData;
        PBuffer pDescEnd   = 
            (PBuffer)(inputTuple[inputTuple.size() - 2].pData +
                      inputTuple[inputTuple.size() - 2].cbData);
        rowCount = 
            getCompressedRowCount(pDescStart, pDescEnd, 
                lastSegDescByte, lastZeroRIDs);
    }
    return rowCount;
}


bool LbmEntry::singleton2Bitmap() 
{
    if (setRIDNewSegment(startRID)) {
        startRID = roundToByteBoundary(startRID);
        entryTuple[entryTuple.size()-2].pData = pSegDescStart;
        return true;
    } else {
        return false;
    }
}


bool LbmEntry::setRID(LcsRid rid)
{
    assert (!isSingleBitmap());

    /*
     * First prepare the current LbmEntry for insert.
     */
    if (isSingleton()) {
        
        /*
         * If adding RID to a singleton LbmEntry, change the singleton to 
         * bitmap entry
         */
        if (!singleton2Bitmap()) {
            /*
             * Current LbmEntry cannot be appended to.
             * Current LbmEntry remains a singleton.
             */
            return false;
        }
        /*
         * Now the current entry is changed to a bitmap. We are ready to
         * insert the new rid.
         */
    } else if (!isSegmentOpen()) {
        /*
         * It's a bitmap entry, but the currSegDescByte is not set up yet.
         * This is the case when Splicer initializes a LbmEntry with an existing
         * entryTuple.
         * Need to search to the end of the segment descriptors to grow the
         * entry, if there're intervening zeros between the last RID in the
         * buffer and the rid which is the input.
         * Reserve 1 byte to add the RID in the new segment byte.
         */
        openLastSegment();
    }

    assert (isSegmentOpen());

    /*
     * Now insert the new RID.
     */
    if (!isSegmentOpen()) {
        /*
         * First time appending to this LbmEntry.
         */
        return setRIDNewSegment(rid);
    } else {
        /*
         * Distance in bits
         */
        uint distance = opaqueToInt(rid - currSegByteStartRID);

        assert(distance>=0);

        if (distance < LbmOneByteSize) {
            /*
             * Easiest one. Same byte.
             */
            setRIDCurrentSegByte(rid);
        } else if (distance < LbmOneByteSize * 2) {
            /*
             * Adjacent byte.
             */
            return setRIDAdjacentSegByte(rid);
        } else {
            /*
             * Further away. Need to write the Descriptor.
             */
            if (closeCurrentSegment(rid)) {
                /*
                 * Then figure out if there's space left to begin a new 
                 * segment and segment descriptor
                 */
                if (!setRIDNewSegment(rid))
                    return false;
            } else {
                /*
                 * No room for descriptor in the same LbmEntry, tell caller to
                 * start a new entry.
                 */
                return false;
            }
        }
    }
    return true;
}


int LbmEntry::compareEntry(
    TupleData const &inputTuple,
    TupleDescriptor const &tupleDesc) const
{
    return tupleDesc.compareTuplesKey(entryTuple,
                                      inputTuple, 
                                      entryTuple.size()-3);
}


bool LbmEntry::addSegDesc(uint reservedSpace, uint bitmapLength)
{
    uint leftOverSpace = 
        scratchBufferUsableSize - currentEntrySize - reservedSpace;

    uint segDescCount = bitmapLength / LbmMaxSegSize;
    uint lastSegLength = bitmapLength % LbmMaxSegSize;
    uint addedSegDescBytes = segDescCount + (lastSegLength ? 1 : 0);

    if (addedSegDescBytes > leftOverSpace) {
        return false;
    } else {
        int i;

        /*
         * Set segment descriptor pointers.
         */
        for (i = 0; i < segDescCount; i ++) {
            setSegLength(*pSegDescEnd, LbmMaxSegSize);
            pSegDescEnd ++;
        }

        if (lastSegLength) {
            setSegLength(*pSegDescEnd, lastSegLength);
            pSegDescEnd ++;
        }

        resetSegment();

        // number of bytes added to this entry
        currentEntrySize += addedSegDescBytes;

        return true;
    }
}

uint LbmEntry::getMergeSpaceRequired(TupleData const &inputTuple)
{
    uint mergeSpaceRequired = 0;

    /*
     * This seems to be a really strange compiler problem: if
     * "inputSegDescLength" is replaced with the following
     * expression, the calculation is incorrect.
     */
    
    uint inputSegDescLength = 
        inputTuple[inputTuple.size() - 2].pData ? 
        inputTuple[inputTuple.size() - 2].cbData : 0;
    uint inputSegLength = 
        inputTuple[inputTuple.size() - 1].pData ? 
        inputTuple[inputTuple.size() - 1].cbData : 0;

    if (isSingleton(inputTuple)) {
        // singleton
        mergeSpaceRequired = 2;
    } else if (isSingleBitmap(inputTuple)) {
        // single bitmap
        uint segDescCount = (inputSegLength /LbmMaxSegSize) + 1;
        mergeSpaceRequired = segDescCount + inputSegLength;
    } else {
        // compressed bitmap
        mergeSpaceRequired = inputSegDescLength + inputSegLength;
    }

    return mergeSpaceRequired;
}

bool LbmEntry::growEntry(LcsRid rid, uint reserveSpace)
{
    /*
     * See if an existing(rather than an entry which is forming) entry can be
     * grown to encode additional zeros until the byte that encodes rid.
     */
    int leftOverSpace =
        scratchBufferUsableSize - currentEntrySize - reserveSpace;

    if (leftOverSpace < 0) {
        return false;
    }

    if (isSingleton()) {
        if (!singleton2Bitmap()) {
            return false;
        }
    }
    
    if (!isSegmentOpen()) {
        openLastSegment();
    }

    /*
     * Find the last RID.
     */
    uint rowCount = getRowCount();

    /*
     * Bitmap entries encode 8 RIDs at a time. When Splicer calls setRID,
     * this new rid must not have been encoded by the current LbmEntry.
     */
    LcsRid endRID = startRID + rowCount - 1;

    assert(rid >= endRID + 1);

    /*
     * If this rid is not in the next 8 RIDs. We need to "grow" the entry by
     * - remembering the rid gap in the current segment descriptor.
     * - open a new segment for appending.
     */
    if (!closeCurrentSegment(rid)) {
        return false;
    }

    return true;
}

bool LbmEntry::adjustEntry(TupleData &inputTuple)
{
    /*
     * The current entry and the input entry can not be singletons at the same
     * time, as entries come sorted on (index keys, startRID).
     */
    assert(!(isSingleton() && isSingleton(inputTuple)));

    LcsRid &inputStartRID = *((LcsRid*)inputTuple[inputTuple.size() - 3].pData);
    
    if (isSingleton(inputTuple)) {
        /*
         * The current entry must be either compressed bitmap or single
         * bitmap. In either case, just need to set the last byte of the
         * current entry.
         */
        currSegByte = pSegEnd;
        setRIDCurrentSegByte(inputStartRID);
        return true;
    } else if (isSingleton()) {
        // This can only happen if the inputTuple startRID == the singleton RID
        // which means both RIDs are on byte boundary.
        assert ((opaqueToInt(startRID) == opaqueToInt(inputStartRID)) &&
                (opaqueToInt(startRID) % LbmOneByteSize == 0));

        // use the input as the current and set the bit at inputStartRID.
        setEntryTuple(inputTuple);

        // remember to set the bit corresponding startRID
        setRIDSegByte(pSegStart - 1, startRID);

        // the two entries are already merged
        return true;
    } else {
        currSegByte = pSegEnd;
        uint8_t inputFirstSegByte = 
            *(uint8_t *)(inputTuple[inputTuple.size() - 1].pData +
                inputTuple[inputTuple.size() - 1].cbData - 1);
        *currSegByte |= inputFirstSegByte;
            
        /*
         * Everything in the current entry remains the same, since only the
         * last byte is modified. Need to update the fields in inputTuple
         * to reflect that the current entry no longer contains the first
         * byte.
         */

        /*
         * First,
         * modify the segment field. It loses the first byte which is
         * located at the end of the segment storage portion. The length
         * of segment storage is reduced by one, but the pointer to the
         * (end of the) segment storage portion does not change.
         */
        inputTuple[inputTuple.size() -1].cbData --;
        
        /*
         * Then,
         * modify the segment descriptor for the first segment.
         * The descriptor might not be needed if the first segment only has
         * one byte.
         * Also, don't need to modify descriptor if there's none(the single
         * bitmap case).
         */
        PBuffer pFirstDesc = (PBuffer)inputTuple[inputTuple.size() - 2].pData;
        /*
         * Moved a byte worth(=8) of RIDs from the input entry(into
         * the current entry).
         */
        uint numRIDsMoved = LbmOneByteSize;
        
        if (pFirstDesc) {
            uint8_t firstSegLength  = ((*pFirstDesc) >> LbmHalfByteSize) + 1;
            
            if (firstSegLength >= 2) {
                /*
                 * Still need the first segment desscriptor.
                 */
                firstSegLength --;
                /*
                 * Clear out the segment length in the descriptor.
                 */
                *pFirstDesc &= LbmZeroLengthMask;
                /*
                 * Set the new segment length in the descriptor.
                 */
                *pFirstDesc |= (firstSegLength -1) << LbmHalfByteSize;
            } else {
                /*
                 * No need for the first descriptor now.
                 * Figure out the number of RIDs removed from the input
                 * entry. Move the descriptor pointer accordingly.
                 */
                uint firstZeroLength = (*pFirstDesc) & LbmZeroLengthMask;
                pFirstDesc ++;
                
                inputTuple[inputTuple.size() - 2].cbData --;
                
                if (firstZeroLength > LbmZeroLengthCompact) {
                    uint firstZeroLengthBytes = 
                        firstZeroLength - LbmZeroLengthCompact;
                    /*
                     * Translate number of bytes into number of RIDs(bits).
                     */
                    firstZeroLength = 
                        byteArray2Value(pFirstDesc, firstZeroLengthBytes);
                    /*
                     * Number of bytes used to store length of zeros.
                     */
                    pFirstDesc += firstZeroLengthBytes;
                    inputTuple[inputTuple.size() - 2].cbData -= 
                        firstZeroLengthBytes;
                }
                inputTuple[inputTuple.size() - 2].pData = pFirstDesc;
                numRIDsMoved += firstZeroLength * LbmOneByteSize;
            }
        }
        /*
         * Adjust the startRID in the input tuple.
         */
        inputStartRID += numRIDsMoved;
    }
    return false;
}

bool LbmEntry::mergeEntry(TupleData &inputTuple)
{
    assert (!isSingleBitmap());

    /*
     * MergeEntry needs to make sure that no entries of the same index
     * key have overlapping rid ranges. Overlap could happen when the last byte
     * encoded by the first entry is also encoded by the first byte in the next
     * entry.
     * So the first thing mergeEntry does is to adjust the current entry to
     * include the first byte of the input entry. After that, the rid ranges of
     * the current entry and the input entry would not overlap, and the merging
     * logic takes place.
     */

    /*
     * Find the last RID of this entry.
     */
    uint rowCount = getRowCount();

    /*
     * Bitmap entries encode 8 RIDs at a time. When Splicer calls setRID,
     * this new rid might have been encoded by the last segment byte of the
     * current LbmEntry.
     */
    LcsRid endRID = startRID + rowCount - 1;
    LcsRid &inputStartRID = *((LcsRid*)inputTuple[inputTuple.size() - 3].pData);

    /*
     * Only the last segment byte in the current entry and the first segment
     * byte in the input entry can overlap.
     */
    assert(inputStartRID >= roundToByteBoundary(endRID));

    /*
     * First, if there is overlap, move the first byte of the input entry to
     * the last byte of this entry. Modify inputTuple, inputStartRID.
     */
    if (inputStartRID <= endRID) {
        if (adjustEntry(inputTuple)) {
            return true;
        }
    }

    /*
     * After adjustEntry(), there should be no overlap between the current
     * entry and the (newly modified) input entry tuple.
     */
    assert(inputStartRID > endRID);

    uint mergeSpaceRequired = getMergeSpaceRequired(inputTuple);

    /*
     * If the new inputTuple is a singleton, use the setRID interface.
     */
    if (isSingleton(inputTuple)) { 
        return setRID(inputStartRID);
    }

    if (!growEntry(inputStartRID, mergeSpaceRequired)) {
        /*
         * If either the combined entry is bigger than maximum entry size,
         * or if the distance between the last rid of current entry and the
         * start rid of the input entry can not be encoded by
         * LbmZeroLengthExtended bytes, tell caller to start a new entry.
         */
        return false;
    }

    /*
     * Now merge the two pieces of bitmaps together.
     */
    uint inputSegDescLength = 
        inputTuple[inputTuple.size() - 2].pData ? 
        inputTuple[inputTuple.size() - 2].cbData : 0;
    uint inputSegLength = 
        inputTuple[inputTuple.size() - 1].pData ? 
        inputTuple[inputTuple.size() - 1].cbData : 0;

    if (!isSingleBitmap(inputTuple)) {
        /*
         * Copy the segment descriptors only if the tuples are not single
         * bitmaps. Single bitmaps do not have segment descriptors.
         */
        memcpy(pSegDescEnd, inputTuple[inputTuple.size() - 2].pData,
            inputSegDescLength);
        pSegDescEnd += inputSegDescLength;
        currentEntrySize += inputSegDescLength;
    } else {
        // Need to add descriptor for inputSegLength and then copy
        // inputSegLength worth of  bitmap segment.
        uint reservedSpace = inputSegLength;
        if (!addSegDesc(reservedSpace, inputSegLength)) {
            return false;
        }
    }
    
    /*
     * segment grows backwards.
     */
    pSegEnd -= inputSegLength;
    memcpy(pSegEnd,
           inputTuple[inputTuple.size() - 1].pData,
           inputSegLength);
    currentEntrySize += inputSegLength;
    return true;
}


TupleData const &LbmEntry::produceEntryTuple()
{
    /*
     * If singleton, just return the tuple.
     */
    if (isSingleton())
        return entryTuple;

    /*
     * Set up all the data pointers in entryTuple.
     * Make sure that closeCurrentSegment is called for this entry.
     */
    if (isSegmentOpen()) {
        closeCurrentSegment();
    }
    
    /*
     * RID field
     */
    uint RIDField = entryTuple.size() - 3;
    *((LcsRid *)entryTuple[RIDField].pData) = startRID;

    /*
     * The last field is the segment field.
     */
    uint segmentField = entryTuple.size() - 1;
    entryTuple[segmentField].cbData = pSegStart - pSegEnd;
    entryTuple[segmentField].pData = pSegEnd;

    /*
     * Next to the last is segment descriptor field.
     */
    uint segmentDescField = entryTuple.size() - 2;

    /*
     * Find out if the segment descriptors are necessary.
     * If all segments are contiguous, they can be stored at one big bitmap
     * segment without any descriptors.
     */
    
    if ((pSegDescStart == pSegDescEnd) || (pSegDescStart == NULL)) {
        /*
         * There is no segment descriptor, which means the entry must have a
         * single bitmap.
         */
        entryTuple[segmentDescField].cbData = 0;
        entryTuple[segmentDescField].pData  = NULL;
    } else {
        /*
         * There are segment descriptors. Check if these descriptors can be
         * removed(when all the segments are contiguous).
         */
        uint rowCount = getRowCount();

        if (rowCount == entryTuple[segmentField].cbData*8) {
            /*
             * All the RIDs are in the bitmap segments. There is no "gap" 
             * between segments. Do not need to store the segment descriptors
             * since there is only one big bitmap segment inthis entry.
             *
             * The last entry in the stream must remain compressed, otherwise
             * future append might fail. Passing lastRID < 0 bypasses this
             * check.
             */
            entryTuple[segmentDescField].cbData = 0;
            entryTuple[segmentDescField].pData  = NULL;
        } else {
            // zero out the trailing zero length in the last segment descriptor
            // since it should be zero
            openLastSegment();
            closeCurrentSegment();
            entryTuple[segmentDescField].cbData = pSegDescEnd - pSegDescStart;
            entryTuple[segmentDescField].pData  = pSegDescStart;
        }
    }

    return entryTuple;
}

string LbmEntry::printBuffer(PConstBuffer ptr, uint size)
{
    ostringstream byteStr;
    char *pbuf = new char(2);

    if (ptr) {        
        PConstBuffer tmpPtr = ptr + size;
        while (size > 0) {
            tmpPtr --;
            sprintf(pbuf, "%02x", *((uint8_t *)tmpPtr));    
            size --;
            byteStr << pbuf;
        }
    }

    return byteStr.str();
}

string LbmEntry::printByte(uint8_t byte)
{
    ostringstream byteStr;
    uint byteLength = 8;
    
    while (byteLength > 0) {
        byteStr << byte % 2 << " ";
        byte = byte >> 1;
        byteLength --;
    }
    return byteStr.str();
}

string LbmEntry::dumpSeg(PBuffer segDesc, PBuffer segDescEnd, PBuffer seg)
{
    uint segBytes;
    ostringstream entryLine;
    uint zeroBytes;
    uint startPos = 0;
    uint i;

    while (segDesc < segDescEnd) {
        /*
         * Print the bitmaps.
         */
        readSegDescAndAdvance(segDesc, segBytes, zeroBytes);

        /*
         * Print the bitmap segment.
         */
        for (i = 0; i < segBytes + startPos; i++) {
            seg --;
            entryLine << printByte((uint8_t)(*seg));
            if (i % 5 == 4) {
                entryLine << "\n";
            } else {
                entryLine << " ";
            }
        }

        /*
         * Print the trailing zeros.
         */
        for (; i < zeroBytes + segBytes + startPos; i++) {
            entryLine << "0 0 0 0 0 0 0 0 ";
            if (i % 5 == 4) {
                entryLine << "\n";
            } else {
                entryLine << " ";
            }
        }
    }
    return entryLine.str();
}

string LbmEntry::dumpSegRID(
    PBuffer segDesc,
    PBuffer segDescEnd,
    PBuffer seg,
    string prefix,
    LcsRid srid)
{
    uint segBytes;
    uint zeroBytes;
    ostringstream entryTrace;
    uint8_t bitmapByte;
    uint byteLength;
    uint i;

    while (segDesc < segDescEnd) {
        /*
         * Print the bitmaps.
         */
        readSegDescAndAdvance(segDesc, segBytes, zeroBytes);

        for (i = 0; i < segBytes; i++) {
            seg --;
            bitmapByte = *(uint8_t *)seg;
            for(byteLength = 8; byteLength > 0; byteLength --) {
                if (bitmapByte % 2) {
                    entryTrace << prefix << opaqueToInt(srid) << "]\n";
                }
                bitmapByte = bitmapByte >> 1;
                srid ++;
            }
        }

        /*
         * Skip the zeros, but need to update srid.
         */
        srid += zeroBytes * LbmOneByteSize;
    }
    return entryTrace.str();
}

string LbmEntry::dumpBitmap(PBuffer seg, uint segBytes)
{
    ostringstream entryLine;
    /*
     * Print the bitmap.
     */
    for (uint i = 0; i < segBytes; i++) {
        seg --;
        entryLine << printByte((uint8_t)(*seg));
        if (i % 5 == 4) {
            entryLine << "\n";
        } else {
            entryLine << " ";
        }
    }
    entryLine << "\n";        
    return entryLine.str();
}

string LbmEntry::dumpBitmapRID(
    PBuffer seg,
    uint segBytes,
    string prefix,
    LcsRid srid)
{
    ostringstream entryTrace;
    uint byteLength;
    uint8_t bitmapByte;

    /*
     * Print all the RIDs in the bitmap, formatted as:
     * Key [ .... ] RID [...]
     */
    for (uint i = 0; i < segBytes; i++) {
        seg --;
        bitmapByte = *(uint8_t *)seg;
        for(byteLength = 8; byteLength > 0; byteLength --) {
            if (bitmapByte % 2) {
                entryTrace << prefix << opaqueToInt(srid) << "]\n";
            }
            bitmapByte = bitmapByte >> 1;
            srid ++;
        }
    }
    return entryTrace.str();
}

string LbmEntry::toString(TupleData const&inputTuple, bool printRID)
{
    if (printRID) {
        return toRIDString(inputTuple);
    } else {
        return toBitmapString(inputTuple);
    }
}

string LbmEntry::toBitmapString(TupleData const&inputTuple)
{
    ostringstream tupleTrace;
    uint tupleSize = inputTuple.size();

    tupleTrace << "Key [";
    
    for (uint i = 0; i < tupleSize - 3 ; i ++) {
        tupleTrace 
            << printBuffer(inputTuple[i].pData, inputTuple[i].cbData);
        if (i < tupleSize - 4) {
            tupleTrace << "|";
        }
    }
    
    tupleTrace << "] RID [" 
               << opaqueToInt(*(LcsRid *)inputTuple[tupleSize - 3].pData)
               << "] ";

    if (isSingleton(inputTuple)) {
        tupleTrace <<"Singleton";
    }
    else {
        PBuffer segDesc = (PBuffer)inputTuple[tupleSize - 2].pData;
        /*
         * segments are stored backward.
         */
        PBuffer seg     = (PBuffer)inputTuple[tupleSize - 1].pData
                           + inputTuple[tupleSize - 1].cbData;
        
        if (segDesc) {
            tupleTrace << "Compressed Bitmap: SegDesc "
                       << inputTuple[tupleSize - 2].cbData
                       << " bytes, Seg "
                       << inputTuple[tupleSize - 1].cbData
                       << " bytes.\n";

            /*
             * Compressed bitmap(with both segment descriptors and segments)
             */
            PBuffer segDescEnd = segDesc + inputTuple[tupleSize - 2].cbData;

            tupleTrace << dumpSeg(segDesc, segDescEnd, seg);
        } else {
            tupleTrace << "Single Bitmap: Seg "
                       << inputTuple[tupleSize - 1].cbData
                       << " bytes\n";
            /*
             * An entry with a single bitmap segement.
             */
            tupleTrace << dumpBitmap(seg, inputTuple[tupleSize - 1].cbData);
        }
    }
    
    tupleTrace << "\n";

    return tupleTrace.str();
}

string LbmEntry::toRIDString(TupleData const &inputTuple)
{
    ostringstream tupleTrace;
    ostringstream keyTrace;
    uint tupleSize = inputTuple.size();
    LcsRid inputStartRID = *((LcsRid *)inputTuple[tupleSize - 3].pData);

    keyTrace << "Key [";
    for (uint i = 0; i < tupleSize - 3 ; i ++) {
        keyTrace 
            << printBuffer(inputTuple[i].pData, inputTuple[i].cbData);
        if (i < tupleSize - 4) {
            keyTrace << "|";
        }
    }
    keyTrace << "] RID [";
    
    if (isSingleton(inputTuple)) {
        tupleTrace << keyTrace.str()
                   << opaqueToInt(inputStartRID)
                   << "]\n";
    }
    else {
        PBuffer segDesc = (PBuffer)inputTuple[tupleSize - 2].pData;
        /*
         * segments are stored backward.
         */
        PBuffer seg     = (PBuffer)inputTuple[tupleSize - 1].pData
                           + inputTuple[tupleSize - 1].cbData;
        
        tupleTrace << "\n";

        if (segDesc) {
            /*
             * Compressed bitmap(with both segment descriptors and segments)
             */
            PBuffer segDescEnd = segDesc + inputTuple[tupleSize - 2].cbData;

            tupleTrace <<
                dumpSegRID(segDesc,
                           segDescEnd,
                           seg,
                           keyTrace.str(),
                           inputStartRID);
        } else {
            /*
             * An entry with a single bitmap segement.
             */
            tupleTrace <<
                dumpBitmapRID(seg, 
                              inputTuple[tupleSize - 1].cbData,
                              keyTrace.str(),
                              inputStartRID);
        }
    }
    
    return tupleTrace.str();
}

string LbmEntry::toString()
{
    ostringstream tupleTrace;
    uint tupleSize = entryTuple.size();

    tupleTrace << "Key [";
    
    for (uint i = 0; i < tupleSize - 3 ; i ++) {
        tupleTrace 
            << printBuffer(entryTuple[i].pData, entryTuple[i].cbData);
        if (i < tupleSize - 4) {
            tupleTrace << "|";
        }
    }
    
    tupleTrace << "] RID [" 
               << opaqueToInt(*(LcsRid *)entryTuple[tupleSize - 3].pData)
               << "] ";

    if (isSingleton()) {
        tupleTrace <<"Singleton";
    }
    else {
        PBuffer pSegDesc = pSegDescStart;
        /*
         * segments are stored backward.
         */
        PBuffer pSeg     = pSegStart;
        
        if (pSegDesc) {
            tupleTrace << "Compressed Bitmap: SegDesc "
                       << pSegDescEnd - pSegDescStart
                       << " bytes, Seg "
                       << pSegStart - pSegEnd
                       << " bytes.\n";

            /*
             * Compressed bitmap(with both segment descriptors and segments)
             */
            tupleTrace << dumpSeg(pSegDesc, pSegDescEnd, pSeg);
        } else {
            tupleTrace << "Single Bitmap: Seg "
                       << pSegStart - pSegEnd
                       << " bytes\n";
            /*
             * An entry with a single bitmap segement.
             */
            tupleTrace << dumpBitmap(pSeg, pSegStart - pSegEnd);
        }
    }
    
    tupleTrace << "\n";

    return tupleTrace.str();
}

void LbmEntry::getSizeBounds(
    TupleDescriptor const &indexTupleDesc, uint pageSize,
    uint &minEntrySize, uint &maxEntrySize)
{
    uint tupleSize = indexTupleDesc.size();
    TupleStorageByteLength segDescFieldMaxLength = 
        indexTupleDesc[tupleSize - 2].cbStorage;
    TupleStorageByteLength segFieldMaxLength = 
        indexTupleDesc[tupleSize - 1].cbStorage;
    
    // The length of the last two variable size fields are set to be the
    // maximum of the combined size, so that the buffer can fit the most number
    // of bytes for both fields. For exmaple, if the combined size should be
    // less then 512 bytes, then either field can have a size between 0 and
    // 512, however, the LbmEntry manages the buffer so that the combined size
    // of the two fields is not more than 512.
    assert(segDescFieldMaxLength == segFieldMaxLength);

    // The minimum size taken by the bitmap index tuple(in unmarshal format) is
    // then sizeof(keys) + size(RID), plus 2 bytes to encode at least one
    // segment(=1) and one segment desc byte(=1) with extended length
    // descriptor bytes(=3)
    minEntrySize = 
        indexTupleDesc.getMaxByteCount()
        - segDescFieldMaxLength 
        - segFieldMaxLength 
        + 5;

    
    // the sum total of the two bitmap columns needs to be less than the
    // max size of a single column. The maximum size taken by the bitmap index
    // tuple(in unmarshal format) is then sizeof(keys) + size(RID) + sizeof(one
    // variable length column).
    maxEntrySize = 
        indexTupleDesc.getMaxByteCount()
        - segDescFieldMaxLength;

    // Adjust max size based on page size. Try to fit a least minEntryPerPage
    // in a page.
    uint maxEntrySizeForPage = pageSize/LbmMinEntryPerPage;

    if (maxEntrySizeForPage < minEntrySize) {
        maxEntrySize = minEntrySize;
    } else {
        maxEntrySize = min(maxEntrySizeForPage, maxEntrySize);
    }
}

void LbmEntry::generateRIDs(
    TupleData const &inputTuple, std::vector<LcsRid> &ridValues)
{
    uint tupleSize = inputTuple.size();
    LcsRid inputStartRID = *((LcsRid *)inputTuple[tupleSize - 3].pData);

    if (isSingleton(inputTuple)) {
        ridValues.push_back(inputStartRID);
    } else {
        PBuffer segDesc = (PBuffer) inputTuple[tupleSize - 2].pData;
        /*
         * segments are stored backward.
         */
        PBuffer seg =
            (PBuffer) inputTuple[tupleSize - 1].pData
                + inputTuple[tupleSize - 1].cbData;
        if (segDesc) {
            /*
             * Compressed bitmap(with both segment descriptors and segments)
             */
            PBuffer segDescEnd = segDesc + inputTuple[tupleSize - 2].cbData;

            generateSegRIDs(segDesc, segDescEnd, seg, ridValues, inputStartRID);
        } else {
            /*
             * An entry with a single bitmap segement.
             */
            generateBitmapRIDs(
                seg, inputTuple[tupleSize - 1].cbData, ridValues,
                inputStartRID);
        }
    }
}

void LbmEntry::generateSegRIDs(
    PBuffer segDesc, PBuffer segDescEnd, PBuffer seg,
    std::vector<LcsRid> &ridValues, LcsRid srid)
{
    uint segBytes;
    uint zeroBytes;
    uint8_t bitmapByte;
    uint byteLength;
    uint i;

    while (segDesc < segDescEnd) {
        readSegDescAndAdvance(segDesc, segBytes, zeroBytes);

        for (i = 0; i < segBytes; i++) {
            seg --;
            bitmapByte = *(uint8_t *)seg;
            for(byteLength = 8; byteLength > 0; byteLength --) {
                if (bitmapByte % 2) {
                    ridValues.push_back(srid);
                }
                bitmapByte = bitmapByte >> 1;
                srid ++;
            }
        }

        /*
         * Skip the zeros, but need to update srid.
         */
        srid += zeroBytes * LbmOneByteSize;
    }
}

void LbmEntry::generateBitmapRIDs(
    PBuffer seg, uint segBytes, std::vector<LcsRid> &ridValues, LcsRid srid)
{
    uint byteLength;
    uint8_t bitmapByte;

    for (uint i = 0; i < segBytes; i++) {
        seg --;
        bitmapByte = *(uint8_t *)seg;
        for(byteLength = 8; byteLength > 0; byteLength --) {
            if (bitmapByte % 2) {
                ridValues.push_back(srid);
            }
            bitmapByte = bitmapByte >> 1;
            srid ++;
        }
    }
}

uint LbmEntry::getScratchBufferSize(uint bitmapColSize) 
{
    // size of scratch buffer should be the max size of one of
    // the bitmap columns plus space for the rid plus space for an
    // optional zero extension
    return bitmapColSize + sizeof(LcsRid) + LbmZeroLengthExtended;
}

uint LbmEntry::getMaxBitmapSize(uint bitmapColSize)
{
    // reserve a portion of the bitmap for building a segment
    // directory; this is actually slightly more than necessary, but
    // it's not too much of a loss
    assert (bitmapColSize % LbmMaxSegSize == 0);
    return bitmapColSize - (bitmapColSize / LbmMaxSegSize);
}

FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/lucidera/bitmap/LbmEntry.cpp#5 $");

// End LbmEntry.cpp

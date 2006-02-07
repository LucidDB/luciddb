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

#ifndef Fennel_LbmEntry_Included
#define Fennel_LbmEntry_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"
#include <algorithm>

using namespace std;

FENNEL_BEGIN_NAMESPACE

/**
 * Class implementing bitmap index entries.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class LbmEntry : public LbmSegment
{
    /**
     * Scratch buffer to store bitmap segments and descriptors.
     */
    PBuffer scratchBuffer;

    /**
     * Scratch buffer size. Needs to be at least the sizeof(index key columns) +
     * sizeof(LcsRid).
     */
    uint scratchBufferSize;

    /**
     * LbmEntry tuple, same format as output tuple from
     * LbmConstructorExecStream.
     * entryTuple always an associated buffer in the beginner of scratchBuffer;
     * entryTuple is not of type TupleDataWithBuffer since it needs an external
     * buffer and the tupleAccessor needs to be visible.
     */
    TupleData entryTuple;

    /**
     * startRID( if singleton, startRID == RID column in entryTuple)
     */
    LcsRid currSegByteStartRID;

    /**
     * size(key) + size(LcsRid) + size(segDesc) + size(seg).
     * For a LbmEntry in construction, it will be the current storage size.
     * For a LbmEntry from an existing tuple, it will be the storage size of
     * the tuple. 
     */
    uint currentEntrySize;

    /**
     * Increment forward from pSegDescStart.
     */ 
    PBuffer currSegDescByte;
    uint currSegDescLength;

    /**
     * Decrement backward from pBitmapSegStart.
     */ 
    PBuffer currSegByte;
    uint currSegLength;

    /**
     * Test if an entry is singleton.
     *
     * @return true if this LbmEntry is a singleton. All LbmEntries start out
     * as a singleton.
     */
    bool isSingleton() const;

    /**
     * Test if a tuple contains a single bitmap.
     *
     * @return true if the tuple contains a single bitmap. 
     */
    bool isSingleBitmap() const;

    /**
     * Test if a tuple contains a single bitmap.
     *
     * @param inputTuple tuple in which a LbmEntry is stored.
     *
     * @return true if the tuple contains a single bitmap. 
     */
    bool isSingleBitmap(TupleData const &inputTuple) const;


    /**
     * Set rid in the current segment byte.
     *
     * @param rid the rid to set the bit for.
     */
    void setRIDCurrentSegByte(LcsRid rid);

    /**
     * Set rid in an adjacent segment byte.
     *
     * @param rid the rid to set the bit for.
     *
     * @return true is there is space for the adjacent byte.
     */
    bool setRIDAdjacentSegByte(LcsRid rid);

    /**
     * Set rid in a new segmetn byte.
     *
     * @param rid the rid to set the bit for.
     *
     * @return true is there is space for the new segment byte(and the new
     * segment descriptor byte).
     */
    bool setRIDNewSegment(LcsRid rid);
    
    /**
     * Store value in a byte array. 
     * The least significant bytes in the value is stored
     * at the first location in the array.
     *
     * @param value
     * @param array a byte array
     * @param arraySize size of the array(number of bytes)
     *
     * @return number of bytes used to store the value; 0 if the value requires
     * more than arraySize bytes to store.
     */
    uint value2ByteArray(uint value, PBuffer array, uint arraySize);

    /**
     * Write out the current segment descriptor with no zero bits following the
     * current segment. This is ONLY called when the next RID is known to be in
     * the adjacent byte.
     */
    void completeCurrentDesc();

    /**
     * Write out the current segment descriptor.
     *
     * @param rid the new rid to be inserted into the next segment(or the next
     * LbmEntry if there is not enough space).
     *
     * @return if the LbmEntry can encode the zero bits following the last 
     * segment and the rid to be inserted.
     */
    bool completeCurrentDesc(LcsRid rid);

    /*
     * Get the last segment descriptor and calculate the number of rows
     * contained in the current entry, by going through
     * the segement descriptors from the start to the end.
     *
     * @param[out] lastLengthDescBytes the number of additional bytes required
     * to store the length of zero bytes
     *
     * @param[out] lastZeroBytes number of zero bytes if lastLengthDescBytes is
     * non-zero.
     *
     * @return number of rows contained in this entry.
     */
    uint getRowCount(uint &lastLengthDescBytes, uint &lastZeroBytes);

    /**
     * Grow entry to encode zeros until rid(to be exact, the byte that encodes 
     * rid).
     *
     * @param rid the new rid that this entry will try to include
     *
     * @return true if there is enough room to grow the current entry to rid;
     * false otherwise.
     */
    bool growEntry(LcsRid rid);

    /**
     * This function is called when the last byte of the current entry overlaps
     * with the first byte of the input entry tuple. Combine these two
     * byte-long bitmaps into the last byte of the current entry, and remove
     * the first bitmap byte from the input entry.
     *
     * @param[in|out] the input entry tuple
     *
     * @return true if the adjusted current entry includes the entire input
     * entry. In that case no merging is required later.
     */
    bool adjustEntry(TupleData &inputTuple);

    /**
     ** STATIC MEMBERS AND METHODS
     **/

    static const uint LbmMinEntryPerPage = 8;

    /**
     * Test if a tuple is singleton.
     *
     * @param inputTuple tuple in which a LbmEntry is stored.
     *
     * @return true if the tuple is a singleton. 
     */
    static bool isSingleton(TupleData const &inputTuple);

    /**
     * Print the inputTuple as a bitmap index entry. Print out the bitmaps.
     */
    static string toBitmapString(TupleData const &inputTuple);

    /**
     * Print a bitmap segment.
     */
    static string dumpSeg(PBuffer segDesc, PBuffer segDescEnd, PBuffer seg);

    /**
     * Print a single bitmap segment.
     */
    static string dumpBitmap(PBuffer seg, uint segBytes);

    /**
     * Print all "size" bytes starting form "ptr".
     */
    static string printBuffer(PConstBuffer ptr, uint size);

    /**
     * Print all bits in a byte.
     */
    static string printByte(uint8_t byte);

    /**
     * Print the inputTuple as a bitmap index entry. Print out the RIDs.
     */
    static string toRIDString(TupleData const &inputTuple);

    /**
     * Print a bitmap segment as RIDs.
     */
    static string dumpSegRID(PBuffer segDesc, PBuffer segDescEnd, PBuffer seg,
                             string prefix, LcsRid srid);

    /**
     * Print a single bitmap segment as RIDs.
     */
    static string dumpBitmapRID(PBuffer seg, uint segBytes, 
                                string prefix, LcsRid srid);

public:

    /**
     * Construct a new entry.
     */
    explicit LbmEntry();

    ~LbmEntry() {}

    /**
     * Initialize the LbmEntry: associate buffer with this LbmEntry and setup
     * the entryTuple.
     *
     * @param scratchBufferSizeInit
     * @param scratchBufferSizeInitSize
     * @param tupleDesc 
     */
    void init(PBuffer scratchBufferInit, uint scratchBufferSizeInit,
        TupleDescriptor const &tupleDesc);

    /**
     * Initialize an existing LbmEntry from a new entryTuple.
     * When called by the Generator, the entryTuple expects the index key
     * fields are all set and the RID field is set; The
     * SegmentDesc and Segment fields are NULL. The entry started out as
     * singleton entry with startRID member field set to the RID field in
     * entryTuple. When writing out an entryTuple with bitmaps, the RID field
     * in entryTuple will be replaced by startRID member value.
     * When called by the Splicer, the SegmentDesc and Segment fields are not
     * NULL if the entryTuple contains bitmaps(only NULL if singletons).
     *
     */
    void setEntryTuple(TupleData const &entryTuple);

    /**
     * Set the bit corresponding to the RID.
     *
     * @param rid RID of the row which has matching keys.
     *
     * @return true if successful; false if current LbmEntry is full. Caller
     * need to then call generate() to output the current LbmEntry into a
     * caller-provided buffer; re-initialize the LbmEntry to encode the next
     * set of input rows, and call setRID() again for the failed RID.
     *
     *
     */
    bool setRID(LcsRid rid);
    
    /**
     * @return -1 if this.entryTuple <  inputEntry.entryTuple,
     *          0 if this.entryTuple == inputEntry.entryTuple,
     *          1 if this.entryTuple > inputEntry.entryTuple.
     * 
     */
    int compareEntry(TupleData const &inputTuple, 
        TupleDescriptor const &tupleDesc) const;

    /**
     * Merged the current entry with input. The merged entry becomes the
     * current. Also needs to handle the singleton case.
     *
     * @param[in|out] inputTuple
     * @return false if merged entry can not fit into the maximum entry size.  
     *
     */
    bool mergeEntry(TupleData &inputTuple);

    /**
     * Produce the bitmap entry tuple constructed so far.
     *
     * @return a pointer to the bitmap entry tuple.
     */
    TupleData const &produceEntryTuple();

    /**
     * Print the current entry.
     */
    string toString();

    /**
     ** STATIC METHODS
     **/

    /**
     * Print the inputTuple as a bitmap index entry.
     *
     * @param inputTuple the index entry tuple to print.
     * @param printRID true if want to print out the RIDs rather than bitmaps.
     */
    static string toString(TupleData const &inputTuple, bool printRID=false);

    /**
     * Return the min and the max entry size, based on index tuple descriptor
     * , page size and preferred minimum entries per page.
     *
     * @param[in] indexTupleDesc the tuple descriptor for the index entries.
     * @param[in] pageSize the index leaf node size.
     * @param[out] minEntrySize minimum entry size.
     * @param[out] maxEntrySize maximum entry size.
     */
    static void getSizeBounds(
        TupleDescriptor const &indexTupleDesc, uint pageSize,
        uint &minEntrySize, uint &maxEntrySize);
};

FENNEL_END_NAMESPACE

#endif

// End LbmEntry.h

/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Copyright (C) 2005-2009 The Eigenbase Project
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
class FENNEL_LBM_EXPORT LbmEntry
    : public LbmSegment
{
    /**
     * Scratch buffer to store bitmap segments and descriptors.
     */
    PBuffer scratchBuffer;

    /**
     * Secondary scratch buffer; only need if mergeEntry() will be called
     */
    PBuffer mergeScratchBuffer;

    /**
     * Scratch buffer size. Needs to be at least the sizeof(index key columns) +
     * sizeof(LcsRid).
     */
    uint scratchBufferSize;

    uint scratchBufferUsableSize;

    /**
     * LbmEntry tuple, same format as output tuple from
     * LbmConstructorExecStream.
     * entryTuple always has an associated buffer at the beginning of
     * scratchBuffer;
     * entryTuple is not of type TupleDataWithBuffer since it needs an external
     * buffer and the tupleAccessor needs to be visible.
     */
    TupleData entryTuple;

    /**
     * Descriptor of the key portion + RID of the bitmap entry
     */
    TupleDescriptor keyDesc;

    /**
     * startRID( if singleton, startRID == RID column in entryTuple)
     */
    LcsRid currSegByteStartRID;

    /**
     * size(current key) + size(LcsRid) + size(segDesc) + size(seg).
     * For a LbmEntry in construction, it will be the current storage size.
     * For a LbmEntry from an existing tuple, it will be the storage size of
     * the tuple.
     */
    uint currentEntrySize;

    /**
     * size(current key) + size(SRID)
     */
    uint keySize;

    /**
     * Size of the bitmap segment field
     */
    uint bitmapSegSize;

    /**
     * Maximum possible size of a single bitmap segment, as computed by
     * getMaxBitmapSize
     */
    uint maxSegSize;

    /**
     * Increment forward from pSegDescStart.
     */
    PBuffer currSegDescByte;

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
    inline bool isSingleton() const;

    /**
     * Test if a tuple contains a single bitmap.
     *
     * @return true if the tuple contains a single bitmap.
     */
    inline bool isSingleBitmap() const;

    /**
     * Set rid in the specified segment byte.
     *
     * @param pSegByte the specified segment byte.
     * @param rid the rid to set the bit for.
     */
    inline void setRIDSegByte(PBuffer pSegByte, LcsRid rid);

    /**
     * Set rid in the current segment byte.
     *
     * @param rid the rid to set the bit for.
     */
    inline void setRIDCurrentSegByte(LcsRid rid);

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
     * reset segment related member fields.
     */
    inline void resetSegment();

    /**
     * Open a new segment for write. This is called when building or growing an
     * entry.
     *
     * @param rid the rid to set in the new segment.
     */
    bool openNewSegment(LcsRid rid);

    /**
     * Open the last segment for write. This is called on existing entries.
     */
    void openLastSegment();

    /**
     * Write out the current segment descriptor with no zero bits following the
     * current segment. This is ONLY called when the next RID is known to be in
     * the adjacent byte.
     */
    void closeCurrentSegment();

    /**
     * Write out the current segment descriptor.
     *
     * @param rid the new rid to be inserted into the next segment(or the next
     * LbmEntry if there is not enough space).
     *
     * @return if the LbmEntry can encode the zero bits following the last
     * segment and the rid to be inserted.
     */
    bool closeCurrentSegment(LcsRid rid);

    /**
     * Is the segment open for write?
     * @return true if segment is open for write
     */
    inline bool isSegmentOpen();

    /*
     * Get the last segment descriptor and calculate the number of rows
     * contained in the current entry, by going through
     * the segement descriptors from the start to the end.
     *
     * @param [out] lastSegDescByte the last SegDesc byte in this entry
     *
     * @param [out] lastZeroRIDs number of RIDs from rows not having this
     *  key value.
     *
     * @return number of rows encoded by this entry.
     */
    uint getRowCount(PBuffer &lastSegDescByte, uint &lastZeroRIDs);

    static uint getCompressedRowCount(
        PBuffer pDescStart, PBuffer pDescEnd,
        PBuffer &lastSegDescByte, uint &lastZeroRIDs);

    /**
     * Grow entry to encode zero RIDs before rid(or more precisely, the byte
     * that encodes rid).
     *
     * @param [in] rid the new rid that this entry will try to include
     *
     * @param [in] reserveSpace the number of bytes to reserve in the scratch
     * buffer
     *
     * @return true if there is enough room to grow the current entry to rid;
     * false otherwise.
     */
    bool growEntry(LcsRid rid, uint reserveSpace);

    /**
     * This function is called when the last byte of the current entry overlaps
     * with the first byte of the input entry tuple. Combine these two
     * byte-long bitmaps into the last byte of the current entry, and remove
     * the first bitmap byte from the input entry.
     *
     * @param [in, out] inputTuple the input entry tuple
     *
     * @return true if the adjusted current entry includes the entire input
     * entry. In that case no merging is required later.
     */
    bool adjustEntry(TupleData &inputTuple);

    /**
     * Add segment descriptors to a single bitmap entry, if the resulting
     * entry can have reservedSpace free.
     *
     * @param [in] reservedSpace amount of space in reserve
     * @param [in] bitmapLength bitmap segment length to add descriptors for.
     *
     * @return true if the new descriptors can fit.
     */
    bool addSegDesc(uint reservedSpace, uint bitmapLength);

    /**
     * Morph a singleton entry into a compressed bitmap entry.
     */
    bool singleton2Bitmap();

    /**
     * Splices a singleton input into the current entry.
     *
     * @param [in, out] inputTuple the input entry singleton
     *
     * @return true if there is enough space in the current entry to splice
     * in the singleton input entry; false otherwise
     */
    bool spliceSingleton(TupleData &inputTuple);

    /**
     * Copies the current entry into a secondary merge scratch buffer.
     *
     * @param newEntry new entry tuple corresponding to the copied entry
     * @param startRid startrid in the new entry tuple
     * @param segStart start of byte segment that should be copied
     * @param segDescStart start of the segment descriptor that should be
     * copied
     */
    void copyToMergeBuffer(
        TupleData &newEntry, LcsRid startRid, PBuffer segStart,
        PBuffer segDescStart);

    /**
     * Creates a new segment in the middle of two existing segments, the new
     * segment corresponding to a singleton input.  I.e., replaces a byte
     * that's currently a "zero byte" with a byte containing a singleton rid.
     *
     * @param [in, out] inputTuple the input entry singleton
     * @param prevSrid starting rid of the segment that the new segment will be
     * inserted after
     * @param prevSegDesc pointer to the segment descriptor corresponding to the
     * segment that the new segment will be inserted after
     * @param nextSegDesc pointer to the segment descriptor corresponding to the
     * segment that will follow the new segment
     * @param prevSeg pointer to the segment that the new segment will be
     * inserted after
     * @param prevSegBytes number of bytes in the segment that the new segment
     * will be inserted after
     * @param prevZeroBytes number of trailing zeros in the segment that the new
     * segment will be inserted after
     *
     * @return true if there is enough space in the current entry to
     * accomodate the new byte; false otherwise
     */
    bool addNewSegment(
        TupleData &inputTuple, LcsRid prevSrid, PBuffer prevSegDesc,
        PBuffer nextSegDesc, PBuffer prevSeg, uint prevSegBytes,
        uint prevZeroBytes);

    /**
     * Creates a new segment in between two segments containing a singleton
     * rid.
     *
     * @param [in, out] inputTuple the input entry singleton
     * @param prevSrid starting rid of the segment that the new segment will be
     * inserted after
     * @param prevSegDesc pointer to the segment descriptor corresponding to the
     * segment that the new segment will be inserted after
     * @param nextSegDesc pointer to the segment descriptor corresponding to the
     * segment that will follow the new segment
     * @param prevSeg pointer to the segment that the new segment will be
     * inserted after
     * @param prevSegBytes number of bytes in the segment that the new segment
     * will be inserted after
     * @param prevZeroBytes number of trailing zeros in the segment that the new
     * segment will be inserted after
     *
     * @return true if there is enough space in the current entry to
     * accomodate the new byte; false otherwise
     */
    bool addNewMiddleSegment(
        TupleData &inputTuple, LcsRid prevSrid, PBuffer prevSegDesc,
        PBuffer nextSegDesc, PBuffer prevSeg, uint prevSegBytes,
        uint prevZeroBytes);

    /**
     * Creates a new segment adjacent to an existing segment, the new
     * segment corresponding to a singleton input.  I.e., replaces a byte
     * that's currently a "zero byte" with a byte containing a singleton rid.
     * If possible, combines adjacents segments into larger segments.
     *
     * @param [in, out] inputTuple the input entry singleton
     * @param prevSrid starting rid of the segment that the new segment will be
     * inserted after
     * @param prevSegDesc pointer to the segment descriptor corresponding to the
     * segment that the new segment will be inserted after
     * @param nextSegDesc pointer to the segment descriptor corresponding to the
     * segment that will follow the new segment
     * @param prevSeg pointer to the segment that the new segment will be
     * inserted after
     * @param prevSegBytes number of bytes in the segment that the new segment
     * will be inserted after
     * @param prevZeroBytes number of trailing zeros in the segment that the new
     * segment will be inserted after
     *
     * @return true if there is enough space in the current entry to
     * accomodate the new byte; false otherwise
     */
    bool addNewAdjacentSegment(
        TupleData &inputTuple, LcsRid prevSrid, PBuffer prevSegDesc,
        PBuffer nextSegDesc, PBuffer prevSeg, uint prevSegBytes,
        uint prevZeroBytes);

    /**
     * Sets the zero length information in segment descriptor
     *
     * @param nZeroBytes number of zero bytes
     * @param pLenDesc pointer to the length descriptor byte
     * @param lengthBytes returns the number of length bytes if the length
     * cannot solely be encoded in the descriptor
     *
     * @return true if length descriptor can be encoded; false otherwise
     */
    bool setZeroLength(
        uint nZeroBytes, PBuffer pLenDesc, uint &lengthBytes);

    /**
     * Adds a new byte segment corresponding to a single rid, in the middle of
     * the current segment bytes.  In doing so, shifts over the current
     * segments that will follow the new one, in order to make room for the new
     * segment.
     *
     * @param nextSegDesc segment descriptor of the segment that will follow
     * the new one to be added
     * @param newSeg pointer to the byte corresponding to the new segment
     * @param newRid the single rid value that will be set in the new segment
     * @param remainingSegLen length of the segments that follow the new one
     * we're adding
     */
    void addNewRid(
        PBuffer nextSegDesc, PBuffer newSeg, LcsRid newRid,
        uint remainingSegLen);

    /**
     * Splits the current entry in half to free up space for the input
     * singleton, and merges the input singleton into the appropriate half.
     * The second half is copied into the input tuple.
     *
     * @param [in, out] inputTuple input singleton to be merged in
     */
    void splitEntry(TupleData &inputTuple);

    /**
     * Merges a singleton input into an entry that has just been split off from
     * the current entry
     *
     * @param inputTuple [in|out] input singleton
     * @param splitEntry the split off entry
     */
    void mergeIntoSplitEntry(
        TupleData &inputTuple, TupleData &splitEntry);

    /**
     * Determines whether a segment contains a specified rid
     *
     * @param rid the rid being searched for
     * @param startRid starting rid of the segment being searched
     * @param pSeg pointer to the start of the segment; since the segment is
     * stored backwards, the pointer actually points to one byte past the
     * first logical byte in the segment
     * @param nSegBytes number of bytes in the segment being searched
     *
     * @return 0 if the segment contains the rid; -1 if it does not; 1 if the
     * rid is not within the range of rid values covered by the segment
     */
    int segmentContainsRid(
        LcsRid rid,
        LcsRid startRid,
        PBuffer pSeg,
        uint nSegBytes);

    /**
     * Determines the amount of space required to merge an input tuple
     *
     * @param [in] inputTuple the input tuple
     *
     * @return the amount of space required for the merge
     */
    uint getMergeSpaceRequired(TupleData const &inputTuple);

    /**
     * Verifies that the current size of the constructed entry does not
     * exceed the scratch buffer size.  Throws an exception if it does.
     */
    void validateEntrySize();

    /**
     ** STATIC MEMBERS AND METHODS
     **/

    static const uint LbmMinEntryPerPage = 8;

    static const uint LbmSmallSingleBitmap = 256;

    /**
     * Test if a tuple contains a single bitmap.
     *
     * @param inputTuple tuple in which a LbmEntry is stored.
     *
     * @return true if the tuple contains a single bitmap.
     */
    inline static bool isSingleBitmap(TupleData const &inputTuple);

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
     * Print a TupleDatum.
     */
    static string printDatum(
        TupleDatum const &tupleDatum,
        bool reverseByteOrder);

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
    static string dumpSegRID(
        PBuffer segDesc, PBuffer segDescEnd, PBuffer seg,
        string prefix, LcsRid srid);

    /**
     * Print a single bitmap segment as RIDs.
     */
    static string dumpBitmapRID(
        PBuffer seg, uint segBytes,
        string prefix, LcsRid srid);

    /**
     * Generate a vector of the RIDs contained in a bitmap segment
     */
    static void generateSegRIDs(
        PBuffer segDesc, PBuffer segDescEnd, PBuffer seg,
        vector<LcsRid> &ridValues, LcsRid srid);

    /**
     * Generate a vector of RIDs contained in a single bitmap segment
     */
    static void generateBitmapRIDs(
        PBuffer seg, uint segBytes, vector<LcsRid> &ridValues,
        LcsRid srid);

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
     * @param scratchBufferInit scratch buffer used to construct entry
     * @param mergeScratchBufferInit secondary scratch buffer; only needs to
     * be passed in if mergeEntry() will be called to splice rids in the middle
     * of existing entries
     * @param scratchBufferSizeInit size of the scratch buffers
     * @param tupleDesc descriptor of the entry
     */
    void init(
        PBuffer scratchBufferInit, PBuffer mergeScratchBufferInit,
        uint scratchBufferSizeInit, TupleDescriptor const &tupleDesc);

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
    int compareEntry(
        TupleData const &inputTuple,
        TupleDescriptor const &tupleDesc) const;

    /**
     * Merge the current entry with input. The merged entry becomes the
     * current. The rids represented by the input must be larger than the
     * rids in the current entry, unless the input is a singleton.
     *
     * @param [in, out] inputTuple the input tuple
     *
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
     * Get the number of rows encoded by this entry
     *
     * @return number of rows encoded by this entry.
     */
    uint getRowCount();

    /**
     * Get the number of rows encoded by a bitmap tuple.
     *
     * @param [in] inputTuple the input bitmap tuple to get row count for
     *
     * @return number of rows encoded by this entry.
     */
    static uint getRowCount(TupleData const &inputTuple);

    /**
     * Determines if the bitmap entry contains a specified rid value
     *
     * @param rid rid value
     *
     * @return true if the entry contains the rid
     */
    bool containsRid(LcsRid rid);

    /**
     * Determines if a specified rid is within the range of the current
     * bitmap entry being constructed.
     *
     * @param rid the rid
     *
     * @return true if within current rid range
     */
    bool inRange(LcsRid rid);

    /**
     ** STATIC METHODS
     **/

    /**
     * Print the inputTuple as a bitmap index entry.
     *
     * @param [in] inputTuple the index entry tuple to print.
     * @param [in] printRID true if want to print out the RIDs rather than
     * bitmaps
     */
    static string toString(TupleData const &inputTuple, bool printRID = false);

    /**
     * Return the min and the max entry size, based on index tuple descriptor
     * , page size and preferred minimum entries per page.
     *
     * @param [in] indexTupleDesc the tuple descriptor for the index entries.
     * @param [in] pageSize the index leaf node size.
     * @param [out] minEntrySize minimum entry size.
     * @param [out] maxEntrySize maximum entry size.
     */
    static void getSizeBounds(
        TupleDescriptor const &indexTupleDesc, uint pageSize,
        uint &minEntrySize, uint &maxEntrySize);

    /**
     * Generates a vector of RIDs corresponding to the rid values represented
     * by a bitmap entry
     *
     * @param inputTuple tupledata corresponding to bitmap entry
     * @param ridValues returns vector of rid values
     */
    static void generateRIDs(
        TupleData const &inputTuple, vector<LcsRid> &ridValues);

    /**
     * Returns the ideal scratch buffer size to provide when initializing
     * an instance of LbmEntry. Providing a buffer of the appropriate size
     * ensures that generated tuple data will fit into the bounds of the
     * required tuple type.
     *
     * @param bitmapColSize size of a bitmap column in the generated tuple
     */
    static uint getScratchBufferSize(uint bitmapColSize);

    /**
     * Returns the maximum size of a bitmap segment for a given column size.
     * A portion of the column size is reserved for generating a segment
     * directory for the bitmap segment.
     *
     * @param bitmapColSize size of a bitmap column in the generated tuple
     */
    static uint getMaxBitmapSize(uint bitmapColSize);

    /**
     * Test if a tuple is singleton.
     *
     * @param inputTuple tuple in which a LbmEntry is stored.
     *
     * @return true if the tuple is a singleton.
     */
    inline static bool isSingleton(TupleData const &inputTuple);

    /**
     * Gets the startRID of a bitmap tuple.
     *
     * <p>Note: gcc 4.0.3 fails if this method is named getStartRID
     *
     * @param tuple tuple in which an LbmEntry is stored.
     *
     * @return startRID of the specified tuple
     */
    inline static LcsRid getStartRid(
        TupleData const &tuple);
};


/**************************************************
  Definitions of inline methods for class LbmEntry
 **************************************************/

inline bool LbmEntry::isSingleton(TupleData const &inputTuple)
{
    return (inputTuple[inputTuple.size() - 2].isNull() &&
        inputTuple[inputTuple.size() - 1].isNull());
}

inline LcsRid LbmEntry::getStartRid(
    TupleData const &tuple)
{
    return *reinterpret_cast<LcsRid const *> (tuple[tuple.size()-3].pData);
}

inline bool LbmEntry::isSingleton() const
{
    return (pSegStart == pSegEnd);
}


inline bool LbmEntry::isSingleBitmap(TupleData const &inputTuple)
{
    return (inputTuple[inputTuple.size() - 2].isNull() &&
        !inputTuple[inputTuple.size() - 1].isNull());
}


inline bool LbmEntry::isSingleBitmap() const
{
    return (pSegDescStart == NULL);
}

inline void LbmEntry::setRIDSegByte(PBuffer pSegByte, LcsRid rid)
{
    assert(pSegByte);
    *pSegByte |= (uint8_t)(1 << (opaqueToInt(rid) % LbmOneByteSize));
}

inline void LbmEntry::setRIDCurrentSegByte(LcsRid rid)
{
    setRIDSegByte(currSegByte, rid);
}

inline bool LbmEntry::isSegmentOpen()
{
    return (currSegDescByte != NULL);
}

inline void LbmEntry::resetSegment()
{
    currSegByte = NULL;
    currSegLength = 0;
    currSegByteStartRID = (LcsRid)0;
    currSegDescByte = NULL;
}

FENNEL_END_NAMESPACE

#endif

// End LbmEntry.h

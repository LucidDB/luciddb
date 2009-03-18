/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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

#ifndef Fennel_LbmRidReader_Included
#define Fennel_LbmRidReader_Included

#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"
#include "fennel/lucidera/bitmap/LbmSegmentReader.h"
#include "fennel/lucidera/bitmap/LbmTupleReader.h"

FENNEL_BEGIN_NAMESPACE

class LbmSingleTupleReader;

/**
 * LbmRidReaderBase provides an interace for reading RIDs from bit segments.
 * It utilizes LbmSegmentReader to read byte segments and then advances within
 * the byte to find set bits, returning the RIDs corresponding to those bits.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmRidReaderBase : public LbmSegment
{
protected:
    /**
     * Segment reader
     */
    LbmSegmentReader segmentReader;

    /**
     * True if initial read has been completed
     */
    bool firstReadDone;

    /**
     * Unread bits from current byte
     */
    uint curByte;

    /**
     * Position in current bit segment
     */
    LcsRid curRid;

    /**
     * True if need to move to the next bit
     */
    bool moveNext;

    /**
     * Next rid value that should be read
     */
    LcsRid nextRid;

    /**
     * Resets state variables so next call to rid reader will read a new
     * tuple from the input stream
     */
    void resetState();

    /**
     * Scans forward in the current byte segment until you hit a set bit.  If
     * the current byte segment is exhausted, new segments are read in.
     *
     * @return EXECRC_YIELD if rid available for reading
     */
    ExecStreamResult searchForNextRid();

    /**
     * Common initialization method, called by all other init methods
     */
    void initCommon();

public:
    /**
     * Advances input to the next rowid >= rid where rowid corresponds to a
     * set bit in a bitmap segment
     *
     * @param rid desired rid
     *
     * @return EXECRC_YIELD if successfully advanced to a rid
     */
    ExecStreamResult advanceToRid(LcsRid rid);

    /**
     * Retrieves rid and sets up caller to advance forward to the next set
     * bit in the input for the next call
     *
     * @param rid rid value to be retrieved
     *
     * @return EXECRC_YIELD if successfully read a rid
     */
    ExecStreamResult readRidAndAdvance(LcsRid &rid);
};

/**
 * LbmRidReader provides an interface for reading RIDs from an input stream
 */
class LbmRidReader : public LbmRidReaderBase
{
public:
    /**
     * Initializes reader to start reading rids corresponding to bit segments
     * from a specified input stream
     *
     * @param pInAccessorInit input stream accessor
     *
     * @param bitmapSegTuple tuple data for reading segments
     */
    void init(
        SharedExecStreamBufAccessor &pInAccessorInit,
        TupleData &bitmapSegTuple);
};

/**
 * LbmIterableRidReader provides an iterator interface to a rid reader.
 *
 * <p>Note that this class does not support the buffer underflow state,
 * EXECRC_BUF_UNDERFLOW. An assertion error will be thrown if the segment
 * reader is initialized with a tuple reader that returns this state.
 */
class LbmIterableRidReader : protected LbmRidReaderBase
{
protected:
    /**
     * True if a rid has been read, but not consumed
     */
    bool buffered;

    /**
     * If a rid has been buffered, this field contains its value
     */
    LcsRid bufferedRid;

    /**
     * Common initialization method, called by all other init methods
     */
    void initCommon();

    /**
     * Searches for the next rid and buffers it
     *
     * @return true if there was another input rid
     */
    inline bool searchForNextRid();

public:
    /**
     * Determines whether there are more rids to be read
     *
     * @return true if there was another input rid
     */
    inline bool hasNext();

    /**
     * Peeks at the current rid value without consuming it. An assertion
     * error is thrown if there are no more rids to be read.
     *
     * @return the first unread rid value
     */
    inline LcsRid peek();

    /**
     * Advances past the current rid value. As assertion error is thrown
     * if there are no more rids to be read.
     */
    inline void advance();

    /**
     * Gets the current rid value and advances to the next one. An assertion
     * error is thrown if there are not more rids to be read.
     *
     * @return the current rid value, before the reader is advanced
     */
    inline LcsRid getNext();
};

/**
 * LbmTupleRidReader is a class for reading rids from bitmap tuples
 */
class LbmTupleRidReader : public LbmIterableRidReader
{
    /**
     * Typed pointer to internal tuple reader
     */
    LbmSingleTupleReader *pReader;

    /**
     * Shared pointer to internal tuple reader
     */
    SharedLbmTupleReader pSharedReader;

public:
    /**
     * Initializes reader to start reading rids corresponding to bit segments
     * from a specified input tuple
     *
     * @param bitmapSegTuple tuple data for reading segments
     */
    void init(TupleData &bitmapSegTuple);
};

/**
 * LbmBTreeRidReader is a class for reading RIDs from a deletion index
 */
class LbmDeletionIndexReader
{
    /**
     * Deletion index btree reader
     */
    SharedBTreeReader btreeReader;

    /**
     * Pointer to tuple data containing a btree bitmap segment
     */
    TupleData *pBitmapSegTuple;

    /**
     * TupleData for searching btree
     */
    TupleData searchEntry;

    /**
     * Whether a tuple is currently being searched
     */
    bool currTuple;

    /**
     * Reads rids from a tuple sequentially
     */
    LbmTupleRidReader ridReader;

    /**
     * The last rid read
     */
    LcsRid btreeRid;

    /**
     * True if the deletion index is empty
     */
    bool emptyIndex;

    /**
     * True if it is not known whether the deletion index is empty
     */
    bool emptyIndexUnknown;

    /**
     * Reinitializes the internal rid reader. Should be called whenever
     * a new tuple is read or to restart a read on the current tuple.
     */
    void initRidReader();

public:
    ~LbmDeletionIndexReader();

    /**
     * Initializes reader to search for RIDs stored in a btree
     * specified BTreeReader
     *
     * @param btreeReader input btree reader
     *
     * @param bitmapSegTuple tuple data for reading segments
     */
    void init(
        SharedBTreeReader &btreeReader,
        TupleData &bitmapSegTuple);

    /**
     * Releases any locks held
     */
    void endSearch();

    /**
     * Determines whether the deletion index is empty
     */
    bool isEmpty();

    /**
     * Searches for a RID in the btree
     *
     * @param rid the RID to search for
     *
     * @return true if the RID was found, false otherwise
     */
    bool searchForRid(LcsRid rid);
};

/**************************************************************
  Definitions of inline methods for class LbmIterableRidReader
***************************************************************/

inline bool LbmIterableRidReader::searchForNextRid()
{
    ExecStreamResult rc = readRidAndAdvance(bufferedRid);
    switch (rc) {
    case EXECRC_YIELD:
        buffered = true;
        break;
    case EXECRC_EOS:
        buffered = false;
        break;
    default:
        permAssert(false);
    }
    return buffered;
}

inline bool LbmIterableRidReader::hasNext()
{
    if (buffered) {
        return true;
    }
    return searchForNextRid();
}

inline LcsRid LbmIterableRidReader::peek()
{
    bool valid = hasNext();
    assert(valid);
    return bufferedRid;
}

inline void LbmIterableRidReader::advance()
{
    bool valid = hasNext();
    assert(valid);
    buffered = false;
}

inline LcsRid LbmIterableRidReader::getNext()
{
    LcsRid next = peek();
    advance();
    return next;
}

FENNEL_END_NAMESPACE

#endif

// End LbmRidReader.h

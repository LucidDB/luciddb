/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2009 LucidEra, Inc.
// Copyright (C) 2006-2009 The Eigenbase Project
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

#ifndef Fennel_LbmSegmentReaderBase_Included
#define Fennel_LbmSegmentReaderBase_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"

#include <boost/dynamic_bitset.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSegmentReaderBase provides the base class for reading from bitmap
 * segments.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmSegmentReaderBase : public LbmSegment
{
protected:
    /**
     * Input tuple reader
     */
    SharedLbmTupleReader pTupleReader;

    /**
     * Length of the current byte segment
     */
    uint byteSegLen;

    /**
     * Byte offset representing current position in segment
     */
    LbmByteNumber byteSegOffset;

    /**
     * Pointer to tuple data containing input bitmap segment
     */
    TupleData *pBitmapSegTuple;

    /**
     * Index of the bitmap columns
     */
    uint iSrid;
    uint iSegmentDesc;
    uint iSegments;

    /**
     * Number of trailing zero bytes in the current segment
     */
    uint zeroBytes;

    /**
     * Used to construct singleton bitmap
     */
    uint8_t singleton;

    /**
     * Detects when a new tuple is read
     */
    bool tupleChange;

    /**
     * Initializes reader to start reading bit segments from a specified
     * input stream, optionally keeping track of the bits read.
     *
     * @param pInAccessorInit input stream accessor
     *
     * @param bitmapSegTupleInit tuple data for reading segments
     *
     * @param setBitmapInit if true, keep track of bits read
     *
     * @param pBitmapInit pointer to the bitmap to be set if the setBitmapInit
     * parameter is true
     */
    void init(
        SharedExecStreamBufAccessor &pInAccessorInit,
        TupleData &bitmapSegTupleInit,
        bool setBitmapInit,
        boost::dynamic_bitset<> *pBitmapInit);

    /**
     * Initializes reader to start reading bit segments from a specified
     * input stream
     *
     * @param pInAccessorInit input stream accessor
     *
     * @param bitmapSegTupleInit tuple data for reading segments
     */
    void init(
        SharedExecStreamBufAccessor &pInAccessorInit,
        TupleData &bitmapSegTupleInit);

    /**
     * Initializes reader to start reading bit segments from a specified
     * tuple reader
     *
     * @param pTupleReaderInit input tuple reader
     *
     * @param bitmapSegTupleInit tuple data for reading segments
     */
    void init(
        SharedLbmTupleReader &pTupleReaderInit,
        TupleData &bitmapSegTupleInit);

    /**
     * Initializes reader to start reading bit segments from a specified
     * tuple reader, optionally keeping track of the bits read.
     *
     * @param pTupleReaderInit input tuple reader
     *
     * @param bitmapSegTupleInit tuple data for reading segments
     *
     * @param setBitmapInit if true, keep track of bits read
     *
     * @param pBitmapInit pointer to the bitmap to be set if the setBitmapInit
     * parameter is true
     */
    void init(
        SharedLbmTupleReader &pTupleReaderInit,
        TupleData &bitmapSegTupleInit,
        bool setBitmapInit,
        boost::dynamic_bitset<> *pBitmapInit);

    /**
     * Reads a bitmap segment tuple from the input stream and extracts the
     * starting rid, the descriptor, and bitmap fields
     *
     * @return EXECRC_YIELD if read was successful
     */
    ExecStreamResult readBitmapSegTuple();

    /**
     * Advances byte segment offset and segment descriptor pointers to
     * the next segment in a bitmap segment
     */
    void advanceSegment();

private:
    /**
     * If true, keep track of bits read
     */
    bool setBitmap;

    /**
     * Pointer to a bitmap that keeps track of the bits read.  Whether bits
     * are set in the bitmap is determined by the setBitmap flag.
     */
    boost::dynamic_bitset<> *pBitmap;

    /**
     * If the reader is set to keep track of bits read, then this field is
     * the maximum rid read since it was initialized.  Otherwise, it is
     * always 0.
     */
    LcsRid maxRidSet;

    /**
     * Sets bits in a bitmap for all bits read from the segment, provided
     * the reader has been set to keep track of bits.
     *
     * @param startRid starting rid of the segment
     * @param segStart first byte of the segment; note that the segment is
     * stored backwards
     * @param segLen length of the segment
     */
    void setBitsRead(LcsRid startRid, PBuffer segStart, uint segLen);

public:
    /**
     * Reports whether a new tuple was read. Initially, this attribute is
     * false. It is updated to true whenever a new tuple is read. The
     * atribute is manually restored to false by calling
     * resetChangeListener(). Otherwise, it will be stuck at true.
     */
    bool getTupleChange();

    /**
     * Resets the tuple change attribute to false.
     *
     * @see getTupleChange().
     */
    void resetChangeListener();

    /**
     * Retrieves the maximum rid that has been read by this reader since it
     * was initialized, provided the reader has been set to keep track of
     * rids read.
     *
     * @return max rid read; 0 if the reader is not keeping track of rids read
     */
    LcsRid getMaxRidSet();
};

FENNEL_END_NAMESPACE

#endif

// End LbmSegmentReaderBase.h

/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2006 Dynamo BI Corporation
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

#ifndef Fennel_LbmSegmentWriter_Included
#define Fennel_LbmSegmentWriter_Included

#include "fennel/lbm/LbmEntry.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSegmentWriter constructs a bitmap segment by utilizing LbmEntry to
 * write out individual byte segments.  A bitmap segment is a bitmap entry
 * with no leading index key values.
 */
class FENNEL_LBM_EXPORT LbmSegmentWriter
    : public boost::noncopyable
{
    /**
     * Bitmap entry object used to construct bitmap segments
     */
    LbmEntry segmentEntry;

    /**
     * Tuple data used to pass byte segments to LbmEntry
     */
    TupleData bitmapTuple;

    /**
     * True if no byte segments have been written yet
     */
    bool firstWrite;

    /**
     * True if zeros should be removed from the bitmap segments.
     */
    bool removeZeros;

public:
    /**
     * Initializes a new segment writer object
     *
     * @param scratchBufferInit scratch buffer for constructing bitmap segments
     *
     * @param scratchBufferSizeInit size of scratch buffer
     *
     * @param bitmapTupleDesc descriptor of the tuple that will represent
     * the constructed bitmap segment
     *
     * @param removeZeros true if zeros should be removed before
     * writing out segments; should only be true in cases like intersection
     * where it is possible to have zeros; whereas, union should
     * not
     */
    void init(
        PBuffer scratchBufferInit, uint scratchBufferSizeInit,
        TupleDescriptor const &bitmapTupleDesc, bool removeZeros);

    /**
     * Resets a segment writer object to start writing out a new bitmap
     * segment
     */
    void reset();

    /**
     * Returns whether the segment writer has added any segments
     */
    bool isEmpty();

    /**
     * Adds a new byte segment to the bitmap segment under construction.
     * Removes zeros if specified during intialization.
     *
     * @param startRid starting rid of segment
     *
     * @param pByteSeg pointer to the last byte segment in a segment that is
     * stored backwards
     *
     * @param len length of the byte segment
     *
     * @return false if not enough space to add the new segment, in which
     * case startRid, pByteSeg, and len represent the remaining portions of
     * the segment that have not been written out yet
     */
    bool addSegment(LcsRid &startRid, PBuffer &pByteSeg, uint &len);

    /**
     * Produces the current segment constructed thus far in tupledata format
     *
     * @return a pointer to the bitmap segment
     */
    TupleData const &produceSegmentTuple();
};

FENNEL_END_NAMESPACE

#endif

// End LbmSegmentWriter.h

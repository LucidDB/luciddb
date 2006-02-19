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

#ifndef Fennel_LbmSegmentWriter_Included
#define Fennel_LbmSegmentWriter_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/bitmap/LbmEntry.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSegmentWriter constructs a bitmap segment by utilizing LbmEntry to
 * write out individual byte segments.  A bitmap segment is a bitmap entry
 * with no leading index key values.
 */
class LbmSegmentWriter
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

public:
    /**
     * Initializes a new segment writer object
     *
     * @param scratchBufferInit scratch buffer for constructing bitmap segments
     *
     * @param scratchBufferInitSize size of scratch buffer
     *
     * @param bitmapTupleDesc descriptor of the tuple that will represent
     * the constructed bitmap segment
     */
    void init(
        PBuffer scratchBufferInit, uint scratchBufferSizeInit,
        TupleDescriptor const &bitmapTupleDesc);

    /**
     * Resets a segment writer object to start writing out a new bitmap
     * segment
     */
    void reset();

    /**
     * Adds a new byte segment to the bitmap segment under construction
     *
     * @param startRid starting rid of segment
     *
     * @param pByteSeg pointer to the byte segment
     *
     * @param len length of the byte segment
     *
     * @return false if not enough space to add the new segment
     */
    bool addSegment(LcsRid startRid, PBuffer pByteSeg, uint len);

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

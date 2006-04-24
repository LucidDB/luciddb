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

#ifndef Fennel_LbmRidReader_Included
#define Fennel_LbmRidReader_Included

#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/lucidera/bitmap/LbmSegmentReader.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmRidReader provides the interace necessary to read RIDs from bit segments.
 * It utilizes LbmSegmentReader to read byte segments and then advances within
 * the byte to find set bits, returning the RIDs corresponding to those bits.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmRidReader : public LbmSegment
{
    /**
     * Input stream accessor
     */
    SharedExecStreamBufAccessor pInAccessor;

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


FENNEL_END_NAMESPACE

#endif

// End LbmRidReader.h

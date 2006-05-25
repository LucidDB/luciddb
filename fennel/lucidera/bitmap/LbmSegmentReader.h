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

#ifndef Fennel_LbmSegmentReader_Included
#define Fennel_LbmSegmentReader_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/lucidera/bitmap/LbmSegmentReaderBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSegmentReader provides the interace necessary to read bit segments.
 * Segments are positioned to a specified byte number (or rid that is
 * converted to a byte).  Only segments that contain set bit values are
 * actually read, so the position specified is actually a lower bound.
 * Once positioned, the current byte can be read.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmSegmentReader : public LbmSegmentReaderBase
{
    /**
     * True if initial read has been done
     */
    bool firstReadDone;

    /**
     * Reads a bitmap segment from the input stream
     *
     * @returns EXECRC_YIELD if successfully read a segment
     */
    ExecStreamResult readSegment();

public:
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
     * Advances within a segment to at least the specified rid, reading
     * in new segments (as needed) to satisfy the request.
     *
     * @param rid desired rid
     *
     * @returns EXECRC_YIELD if successfully read a segment
     */
    ExecStreamResult advanceToRid(LcsRid rid);

    /**
     * Advances within a bit segment to at least the specified byte number,
     * reading in new segments (as needed) to satisfy the request.
     * Reading byte number "x" is equivalent to retrieving rid >= "x * 8".
     *
     * @param byteNum desired byte number
     *
     * @returns EXECRC_YIELD if successfully read a bitmap segment
     */
    ExecStreamResult advanceToByte(LbmByteNumber byteNum);

    /**
     * Reads the current byte segment, based on current position 
     *
     * @param startRid returns rid value corresponding to the start
     * of the current byte segment
     *
     * @param byteSeg returns current byte segment; note that the segment read
     * is stored backwards, so the caller needs to read from right to left
     * starting at byteSeg
     *
     * @param len returns length of current byte segment
     */
    void readCurrentByteSegment(LcsRid &startRid, PBuffer &byteSeg, uint &len);
};

FENNEL_END_NAMESPACE

#endif

// End LbmSegmentReader.h

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

#ifndef Fennel_LbmSeqSegmentReader_Included
#define Fennel_LbmSeqSegmentReader_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/exec/ExecStreamDefs.h"
#include "fennel/lucidera/bitmap/LbmSegmentReaderBase.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LbmSeqSegmentReader provides the interace necessary to read byte segments
 * sequentially. Only segments that contain set bit values are returned to
 * the caller.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class LbmSeqSegmentReader : public LbmSegmentReaderBase
{
public:
    /**
     * Initializes reader to start reading byte segments from a specified
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
     * Reads the current byte segment, based on current position and advances
     * forward to the next segment
     *
     * @param byteNum returns byte number corresponding to the start
     * of the current byte segment
     *
     * @param byteSeg returns current byte segment; note that the segment read
     * is stored backwards, so the caller needs to read from right to left
     * starting at byteSeg
     *
     * @param len returns length of current byte segment
     */
    ExecStreamResult readSegmentAndAdvance(
        LbmByteNumber &byteNum, PBuffer &byteSeg, uint &len);

    /**
     * Returns the start rid of the current tuple
     */
    LcsRid getSrid();
};

FENNEL_END_NAMESPACE

#endif

// End LbmSeqSegmentReader.h

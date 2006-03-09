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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lucidera/bitmap/LbmSegmentReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSegmentReader::init(
    SharedExecStreamBufAccessor &pInAccessorInit,
    TupleData &bitmapSegTuple)
{
    LbmSegmentReaderBase::init(pInAccessorInit, bitmapSegTuple);
    firstReadDone = false;
}

ExecStreamResult LbmSegmentReader::readSegment()
{
    ExecStreamResult rc = readBitmapSegTuple();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    if (pSegDescStart) {
        // in the case where the segment contains a descriptor,
        // set some initial values to make the first call to advanceToByte()
        // read the descriptor and point to the first bitmap in the segment
        byteSegLen = 0;
        return advanceToByte(byteSegOffset);
    }

    return EXECRC_YIELD;
}

ExecStreamResult LbmSegmentReader::advanceToByte(LcsRid byteNum)
{
    // read byte segments until find a suitable one
    while (byteSegOffset + byteSegLen <= byteNum) {

        // if current segment is exhausted, read another
        if (pSegDescStart >= pSegDescEnd) {
            ExecStreamResult rc = readSegment();
            if (rc != EXECRC_YIELD) {
                return rc;
            }
            firstReadDone = true;
            continue;
        }

        // advance to the next segment
        advanceSegment();
    }

    // Found a suitable segment, or were on a suitable one to begin
    // with.  Move to correct position within segment.
    if (byteNum > byteSegOffset) {
        uint delta = opaqueToInt(byteNum - byteSegOffset);
        byteSegLen -= delta;
        pSegStart -= delta;
        byteSegOffset += delta;
    }
    
    return EXECRC_YIELD;
}

ExecStreamResult LbmSegmentReader::advanceToRid(LcsRid rid)
{
    return advanceToByte(rid / LbmOneByteSize);
}

void LbmSegmentReader::readCurrentByteSegment(
    LcsRid &startRid, PBuffer &byteSeg, uint &len)
{
    assert(firstReadDone);
    startRid = LcsRid(byteSegOffset * LbmOneByteSize);
    byteSeg = pSegStart;
    len = byteSegLen;
    // assumes advanceToByte() has been called to move to a segment
    // that has actual bits set, and intermediate zeros have been removed
    assert(*byteSeg != 0);
    assert(len > 0);
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSegmentReader.cpp

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

#include "fennel/lucidera/bitmap/LbmSegmentWriter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSegmentWriter::init(
    PBuffer scratchBufferInit, uint scratchBufferSizeInit,
    TupleDescriptor const &bitmapTupleDesc)
{
    segmentEntry.init(
        scratchBufferInit, scratchBufferSizeInit, bitmapTupleDesc);
    bitmapTuple.compute(bitmapTupleDesc);
    reset();
}

void LbmSegmentWriter::reset()
{
    firstWrite = true;
}

bool LbmSegmentWriter::addSegment(LcsRid startRid, PBuffer pByteSeg, uint len)
{
    uint8_t segDescByte;

    bitmapTuple[0].pData = (PConstBuffer) &startRid;
    // if the length can't be encoded in a segment descriptor, then
    // we have to treat this bitmap as a single bitmap
    if (LbmSegment::setSegLength(segDescByte, len)) {
        bitmapTuple[1].pData = &segDescByte;
        bitmapTuple[1].cbData = 1;
    } else {
        bitmapTuple[1].pData = NULL;
        bitmapTuple[1].cbData = 0;
    }
    bitmapTuple[2].pData = pByteSeg;
    bitmapTuple[2].cbData = len;

    if (firstWrite) {
        segmentEntry.setEntryTuple(bitmapTuple);
        firstWrite = false;
    } else {
        if (!segmentEntry.mergeEntry(bitmapTuple))
            return false;
    }

    return true;
}

TupleData const &LbmSegmentWriter::produceSegmentTuple()
{
    return segmentEntry.produceEntryTuple();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSegmentWriter.cpp

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
#include "fennel/lucidera/bitmap/LbmSegmentWriter.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSegmentWriter::init(
    PBuffer scratchBufferInit, uint scratchBufferSizeInit,
    TupleDescriptor const &bitmapTupleDesc,
    bool removeZerosInit)
{
    segmentEntry.init(
        scratchBufferInit, NULL, scratchBufferSizeInit, bitmapTupleDesc);
    bitmapTuple.compute(bitmapTupleDesc);
    removeZeros = removeZerosInit;
    reset();
}

void LbmSegmentWriter::reset()
{
    firstWrite = true;
}

bool LbmSegmentWriter::isEmpty() 
{
    return firstWrite;
}

bool LbmSegmentWriter::addSegment(
    LcsRid &startRid, PBuffer &pByteSeg, uint &len)
{
    uint8_t segDescByte;

    if (removeZeros) {
        // remove trailing zeros; note that they're trailing because the
        // segments in pByteSeg are backwards
        while (len > 0 && *pByteSeg == 0) {
            pByteSeg++;
            len--;
        }

        // remove leading zeros
        PBuffer bufPtr = pByteSeg + len - 1;
        while (len > 0 && *bufPtr == 0) {
            bufPtr--;
            len--;
            startRid += LbmSegment::LbmOneByteSize;
        }
    }

    while (len > 0) {
        // determine if there are any intermediate zeros
        uint subLen;
        if (!removeZeros) {
            subLen = len;
        } else {
            for (subLen = 0; subLen < len; subLen++) {
                if (pByteSeg[len - 1 - subLen] == 0) {
                    break;
                }
            }
        }

        // write out the first set of segments up to the first intermediate
        // zero, if there is one; otherwise, we just end up writing out
        // all of the segments passed in
        
        bitmapTuple[0].pData = (PConstBuffer) &startRid;
        // if the length can't be encoded in a segment descriptor, then
        // we have to treat this bitmap as a single bitmap
        if (LbmSegment::setSegLength(segDescByte, subLen)) {
            bitmapTuple[1].pData = &segDescByte;
            bitmapTuple[1].cbData = 1;
        } else {
            bitmapTuple[1].pData = NULL;
            bitmapTuple[1].cbData = 0;
        }
        bitmapTuple[2].pData = pByteSeg + len - subLen;
        bitmapTuple[2].cbData = subLen;

        if (firstWrite) {
            segmentEntry.setEntryTuple(bitmapTuple);
            firstWrite = false;
        } else {
            if (!segmentEntry.mergeEntry(bitmapTuple)) {
                return false;
            }
        }

        if (subLen == len) {
            break;
        }

        // figure out how many more intermediate zeros there are
        while (pByteSeg[len - 1 - subLen] == 0) {
            subLen++;
        }

        // adjust the next segment forward, past the intermediate zeros
        startRid += subLen * LbmSegment::LbmOneByteSize;
        len -= subLen;
    }

    return true;
}

TupleData const &LbmSegmentWriter::produceSegmentTuple()
{
    return segmentEntry.produceEntryTuple();
}

FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/lucidera/bitmap/LbmSegmentWriter.cpp#5 $");

// End LbmSegmentWriter.cpp

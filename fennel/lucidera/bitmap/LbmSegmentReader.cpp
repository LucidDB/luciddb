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
    pInAccessor = pInAccessorInit;
    pBitmapSegTuple = &bitmapSegTuple;
    byteSegLen = 0;
    byteSegOffset = 0;
    pSegStart = NULL;
    pSegDescStart = NULL;
    pSegDescEnd = NULL;
    firstReadDone = false;
    zeroBytes = 0;
}

ExecStreamResult LbmSegmentReader::readSegment()
{
    if (pInAccessor->getState() == EXECBUF_EOS) {
        return EXECRC_EOS;
    }

    // consume the previous input if there was one
    if (pInAccessor->isTupleConsumptionPending()) {
        pInAccessor->consumeTuple();
    }
    if (!pInAccessor->demandData()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    // read a new segment and set fields corresponding to the segment --
    // startRid, descriptor, and segment pointer

    pInAccessor->unmarshalTuple(*pBitmapSegTuple);
    startRID = *reinterpret_cast<LcsRid const *> ((*pBitmapSegTuple)[0].pData);

    uint segDescLen = (*pBitmapSegTuple)[1].cbData;
    pSegDescStart = (PBuffer) (*pBitmapSegTuple)[1].pData;
    // descriptor can be NULL
    if (pSegDescStart != NULL) {
        pSegDescEnd = pSegDescStart + segDescLen;
    } else {
        pSegDescEnd = NULL;
    }

    uint segLen;
    if ((*pBitmapSegTuple)[2].pData) {
        // note that bit segment is stored backwards
        segLen = (*pBitmapSegTuple)[2].cbData;
        pSegStart = (PBuffer) ((*pBitmapSegTuple)[2].pData + segLen - 1);
    } else {
        // singletons do not have a corresponding bitmap, so create one
        segLen = 1;
        pSegStart = &singleton;
        singleton = (uint8_t)(1 << (opaqueToInt(startRID) % LbmOneByteSize));
    }

    if (pSegDescStart) {
        // set some initial values to make the first call to advanceToByte()
        // read the descriptor and point to the first bitmap in the segment
        byteSegOffset = opaqueToInt(startRID) / LbmOneByteSize;
        byteSegLen = 0;
        return advanceToByte(byteSegOffset);
    }

    // single segment containing a single bitmap
    byteSegOffset = opaqueToInt(startRID) / LbmOneByteSize;
    byteSegLen = segLen;
    return EXECRC_YIELD;
}

ExecStreamResult LbmSegmentReader::advanceToByte(uint byteNum)
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
    
        // first, advance byte segment offset and segment pointer by the
        // length of remaining part of the previous segment and the trailing
        // zero bytes; in the case where
        // we have already advanced into the segment, byteSegLen has also
        // already been decremented accordingly
        byteSegOffset += byteSegLen + zeroBytes;
        pSegStart -= byteSegLen;

        // then, read the segment descriptor to determine where the
        // segment starts and its length; also advance the segment descriptor
        // to the next descriptor
        readSegDescAndAdvance(pSegDescStart, byteSegLen, zeroBytes);
    }

    // Found a suitable segment, or were on a suitable one to begin
    // with.  Move to correct position within segment.
    if (byteNum > byteSegOffset) {
        uint delta = byteNum - byteSegOffset;
        byteSegLen -= delta;
        pSegStart -= delta;
        byteSegOffset += delta;
    }
    
    return EXECRC_YIELD;
}

ExecStreamResult LbmSegmentReader::advanceToRid(LcsRid rid)
{
    return advanceToByte(opaqueToInt(rid) / LbmOneByteSize);
}

void LbmSegmentReader::readCurrentByteSegment(
    LcsRid &startRid, PBuffer &byteSeg, uint &len)
{
    assert(firstReadDone);
    startRid = LcsRid(byteSegOffset * LbmOneByteSize);
    byteSeg = pSegStart;
    len = byteSegLen;
    // assumes advanceToByte() has been called to move to a segment
    // that has actual bits set
    assert(*byteSeg != 0);
    assert(len > 0);
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSegmentReader.cpp

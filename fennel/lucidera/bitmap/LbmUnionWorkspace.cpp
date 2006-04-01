/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmUnionWorkspace.h"

#include "boost/format.hpp"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmUnionWorkspace::init(SharedByteBuffer pBuffer, uint maxSegmentSize)
{
    mergeArea.init(pBuffer);
    this->maxSegmentSize = maxSegmentSize;
    reset();
}

void LbmUnionWorkspace::reset()
{
    mergeArea.reset();
    segment.reset();
    limited = true;
    productionLimitByte = (LbmByteNumber) 0;
}

void LbmUnionWorkspace::advanceToSrid(LcsRid requestedSrid)
{
    advanceToByteNum(ridToByteNumber(requestedSrid));
}

void LbmUnionWorkspace::advanceToByteNum(LbmByteNumber requestedByteNum)
{
    if (opaqueToInt(requestedByteNum) > mergeArea.getStart()) {
        mergeArea.advance(opaqueToInt(requestedByteNum));
    }
}

void LbmUnionWorkspace::advancePastSegment()
{
    LbmByteNumber endByteNum = segment.byteNum + segment.len;
    advanceToByteNum(endByteNum);
}

void LbmUnionWorkspace::setProductionLimit(LcsRid productionLimitRid)
{
    productionLimitByte = ridToByteNumber(productionLimitRid);
    limited = true;
}

void LbmUnionWorkspace::removeLimit()
{
    limited = false;
}

bool LbmUnionWorkspace::isEmpty() const
{
    LbmByteNumberPrimitive i;
    for (i = mergeArea.getStart(); i < mergeArea.getEnd(); i++) {
        if (mergeArea.getByte(i) != 0) {
            return false;
        }
    }
    return true;
}

bool LbmUnionWorkspace::canProduce()
{
    return (! getSegment().isNull());
}

const LbmByteSegment &LbmUnionWorkspace::getSegment()
{
    // limit for the beginning of a segment; we don't begin a segment
    // unless it has had time to mature (grow to maximum size)
    LbmByteNumberPrimitive startLimit;

    // limit for reading; we can read to the production limit, but not
    // past the end
    LbmByteNumberPrimitive readLimit;

    // if production limit is past end of current data, then it can all
    // be written out due to the gap in data
    LbmByteNumberPrimitive productionLimit = opaqueToInt(productionLimitByte);
    if ( (!limited) || (productionLimit > mergeArea.getEnd()) )
    {
        startLimit = readLimit = mergeArea.getEnd();
    } else {
        readLimit = productionLimit;
        startLimit = (productionLimit > maxSegmentSize)
            ? (productionLimit - maxSegmentSize) : 0;
    }

    // begin with a null segment
    segment.reset();

    // skip past whitespace
    LbmByteNumberPrimitive i = mergeArea.getStart();
    while (i < readLimit && mergeArea.getByte(i) == 0) {
        i++;
    }
    mergeArea.advance(i);
    LbmByteNumberPrimitive start = i;

    if (start > startLimit) {
        return segment;
    }

    // find length of segment (only use the contiguous part)
    uint len = 0;
    while(i < readLimit && mergeArea.getByte(i) != 0) {
        i++;
        len++;
        if (len == maxSegmentSize) {
            break;
        }
    }
    uint contigLen;
    PBuffer mem = mergeArea.getMem(start, contigLen);
    len = std::min(len, contigLen);

    if (len > 0) {
        segment.byteNum = LbmByteNumber(start);
        segment.byteSeg = mem;
        segment.len = len;
    }
    return segment;
}

bool LbmUnionWorkspace::addSegment(const LbmByteSegment &segmentIn)
{
    LbmByteSegment segment = segmentIn;

    // return false if segment cannot fit into merge area
    if (opaqueToInt(segment.getEnd()) > mergeArea.getLimit()) {
        return false;
    }

    // return true if merge area is already advanced beyond segment
    if (opaqueToInt(segment.getEnd()) < mergeArea.getStart()) {
        return true;
    }

    segment.advanceToByteNum(LbmByteNumber(mergeArea.getStart()));
    if (! segment.isNull()) {
        LbmByteNumberPrimitive next = opaqueToInt(segment.byteNum);
        LbmByteNumberPrimitive last = next + segment.len;
        PBuffer read = segment.byteSeg;
        while (next < last) {
            mergeArea.mergeByte(next++, *read--);
        }
    }
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmUnionWorkspace.cpp

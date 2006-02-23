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
    productionLimitByte = (LcsRid) 0;
}

void LbmUnionWorkspace::advanceToSrid(LcsRid requestedSrid)
{
    advanceToByteNum(getByteNumber(requestedSrid));
}

void LbmUnionWorkspace::advanceToByteNum(LcsRid requestedByteNum)
{
    if (requestedByteNum > mergeArea.getStart()) {
        mergeArea.advance(requestedByteNum);
    }
}

void LbmUnionWorkspace::advancePastSegment()
{
    LcsRid endByteNum = segment.byteNum + segment.len;
    advanceToByteNum(endByteNum);
}

void LbmUnionWorkspace::setProductionLimit(LcsRid productionLimitRid)
{
    productionLimitByte = getByteNumber(productionLimitRid);
    limited = true;
}

void LbmUnionWorkspace::removeLimit()
{
    limited = false;
}

bool LbmUnionWorkspace::isEmpty() const
{
    for (LcsRid i = mergeArea.getStart(); i < mergeArea.getEnd(); i++) {
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
    // update segment to current starting position
    segment.advanceToByteNum(mergeArea.getStart());

    // if we already a valid segment, return the segment
    if (! segment.isNull()) {
        return segment;
    }

    // read to the production limit, but not past the end
    LcsRid readLimit = productionLimitByte;
    if (readLimit > mergeArea.getEnd()) {
        readLimit = mergeArea.getEnd();
    }
    if (! limited) {
        readLimit = mergeArea.getEnd();
    }

    // begin with a null segment
    segment.reset();

    // skip past whitespace
    LcsRid i = mergeArea.getStart();
    while (i < readLimit && mergeArea.getByte(i) == 0) {
        i++;
    }
    mergeArea.advance(i);
    LcsRid start = i;

    // find length of segment (only use the contiguous part)
    uint len = 0;
    while(i < readLimit && mergeArea.getByte(i) != 0) {
        i++;
        len++;
        if (len == maxSegmentSize) {
            break;
        }
    }
    len = mergeArea.getContiguousMemSize(start, len);

    if (len > 0) {
        segment.byteNum = start;
        segment.byteSeg = mergeArea.getMem(start, len);
        segment.len = len;
    }
    return segment;
}

bool LbmUnionWorkspace::addSegment(const LbmByteSegment &segmentIn)
{
    LbmByteSegment segment = segmentIn;

    // return false if segment cannot fit into merge area
    if (segment.getEnd() > mergeArea.getLimit()) {
        return false;
    }

    // return true if merge area is already advanced beyond segment
    if (segment.getEnd() < mergeArea.getStart()) {
        return true;
    }

    segment.advanceToByteNum(mergeArea.getStart());
    if (! segment.isNull()) {
        mergeArea.mergeMem(segment.byteNum, segment.byteSeg, segment.len);
    }
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmUnionWorkspace.cpp

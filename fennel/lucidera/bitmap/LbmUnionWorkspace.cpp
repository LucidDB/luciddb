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

void LbmUnionWorkspace::init(
    PBuffer buffer, uint bufferSize, uint maxSegmentSize)
{
    int mergeAreaSize = bufferSize - maxSegmentSize;
    mergeArea.init(buffer, mergeAreaSize);

    segmentArea = buffer + mergeAreaSize;
    this->maxSegmentSize = maxSegmentSize;

    reset();
}

uint LbmUnionWorkspace::getRidLimit()
{
    return mergeArea.getCapacity() - maxSegmentSize;
}

void LbmUnionWorkspace::reset()
{
    mergeArea.reset();
    segment.reset();
    limited = false;
    highestByte = (LcsRid) 0;
}

void LbmUnionWorkspace::advance(LcsRid requestedSrid)
{
    mergeArea.setMin(getByteNumber(requestedSrid));
}

void LbmUnionWorkspace::advanceSegment()
{
    // get the index to the byte past the end of the segment
    LcsRid segmentEndByte = segment.byteNum + segment.len;
    mergeArea.setMin(segmentEndByte);
}

void LbmUnionWorkspace::setLimit(LcsRid productionLimitRid)
{
    limited = true;
    LcsRid invalidRid = productionLimitRid + 1;
    productionLimitByte = getByteNumber(invalidRid) - 1;
}

void LbmUnionWorkspace::removeLimit()
{
    limited = false;
}

bool LbmUnionWorkspace::isEmpty() const
{
    for (LcsRid i = mergeArea.getStartByte(); i < highestByte; i++) {
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
    if (! segment.isNull() && segment.byteNum >= mergeArea.getStartByte()) {
        // we already a valid segment
        return segment;
    }
    // nullify segment
    segment.reset();

    // skip past whitespace
    LcsRid i = mergeArea.getStartByte();
    while (i < productionLimitByte && mergeArea.getByte(i) == 0) {
        i++;
    }
    mergeArea.setMin(i);

    // find length of segment
    uint len = 0;
    while(i < productionLimitByte && mergeArea.getByte(i) != 0) {
        i++;
        len++;
        if (len == maxSegmentSize) {
            break;
        }
    }
    if (mergeArea.getByte(i) != 0 && len != maxSegmentSize) {
        // we only have a partial segment, do not return it yet
        return segment;
    }

    // copy to segment area
    LcsRid byteNum = mergeArea.getStartByte();
    for (uint j = 0; j < len; j++) {
        segmentArea[j] = mergeArea.getByte(byteNum+j);
    }
    segment.byteNum = byteNum;
    segment.byteSeg = segmentArea;
    segment.len = len;
    return segment;
}

bool LbmUnionWorkspace::addSegment(const LbmByteSegment &segment)
{
    return mergeArea.mergeByteSegment(
        segment.byteNum, segment.byteSeg, segment.len);
}

FENNEL_END_CPPFILE("$Id$");

// End LbmUnionWorkspace.cpp

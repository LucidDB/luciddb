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

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"

FENNEL_BEGIN_CPPFILE("$Id$");

uint LbmSegment::byteArray2Value(PBuffer array, uint arraySize)
{
    uint value = 0;
    while (arraySize > 0) {
        value = value * (uint)(1 << LbmOneByteSize) + array[arraySize - 1];
        arraySize --;
    }
    return value;
}


uint LbmSegment::value2ByteArray(uint value, PBuffer array, uint arraySize)
{
    assert(value != 0);

    uint size = 0;

    while (value > 0 && size < arraySize) {
        array[size] = (uint8_t)(value & 0xff);
        value = value >> LbmOneByteSize;
        size ++;
    }
    /*
     * If value is non-zero, it means that the value cannot be encoded
     * within an array of arraySize. Return 0 in that case.
     */
    if (value > 0) {
        size = 0;
    }

    return size;
}

uint LbmSegment::computeSpaceForZeroBytes(uint nZeroBytes)
{
    if (nZeroBytes <= LbmZeroLengthCompact) {
        return 0;
    }

    uint size = 0;
    while (nZeroBytes > 0) {
        nZeroBytes = nZeroBytes >> LbmOneByteSize;
        size++;
    }

    return size;
}

void LbmSegment::readSegDescAndAdvance(
    PBuffer &pSegDesc, uint &bmSegLen, uint &zeroBytes)
{
    // should only be called in the case where the bit segment has
    // a descriptor
    assert(pSegDesc != NULL);
    bmSegLen = (*pSegDesc >> LbmHalfByteSize) + 1;
    uint zeroLen = (*pSegDesc & LbmZeroLengthMask);

    // advance past the initial length byte
    pSegDesc++;

    if (zeroLen <= LbmZeroLengthCompact) {
        zeroBytes = zeroLen;
    } else {
        zeroBytes =
            byteArray2Value(pSegDesc, zeroLen - LbmZeroLengthCompact);
        pSegDesc += zeroLen - LbmZeroLengthCompact;
    }
}

uint LbmSegment::computeSegDescLength(PBuffer segDesc)
{
    uint segDescLength = 0;

    while (segDesc < pSegDescEnd) {
        uint segBytes;
        uint zeroBytes;
        readSegDescAndAdvance(segDesc, segBytes, zeroBytes);
        segDescLength += computeSpaceForZeroBytes(zeroBytes) + 1;
    }

    return segDescLength;
}

uint LbmSegment::computeSegLength(PBuffer segDesc)
{
    uint segLength = 0;

    while (segDesc < pSegDescEnd) {
        uint segBytes;
        uint zeroBytes;
        readSegDescAndAdvance(segDesc, segBytes, zeroBytes);
        segLength += segBytes;
    }

    return segLength;
}

uint LbmSegment::countSegments()
{
    uint count = 0;

    PBuffer segDesc = pSegDescStart;
    while (segDesc < pSegDescEnd) {
        uint segBytes;
        uint zeroBytes;
        readSegDescAndAdvance(segDesc, segBytes, zeroBytes);
        count++;
    }

    return count;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSegment.cpp

/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/lbm/LbmSegment.h"

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

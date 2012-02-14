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
#include "fennel/lbm/LbmSegmentWriter.h"

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

FENNEL_END_CPPFILE("$Id$");

// End LbmSegmentWriter.cpp

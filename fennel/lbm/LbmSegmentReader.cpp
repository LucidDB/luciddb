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
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/lbm/LbmSegmentReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSegmentReader::init(
    SharedExecStreamBufAccessor &pInAccessorInit,
    TupleData &bitmapSegTuple)
{
    init(pInAccessorInit, bitmapSegTuple, false, NULL);
}

void LbmSegmentReader::init(
    SharedExecStreamBufAccessor &pInAccessorInit,
    TupleData &bitmapSegTuple,
    bool setBitmapInit,
    boost::dynamic_bitset<> *pBitmapInit)
{
    LbmSegmentReaderBase::init(
        pInAccessorInit,
        bitmapSegTuple,
        setBitmapInit,
        pBitmapInit);
    initCommon();
}

void LbmSegmentReader::init(
    SharedLbmTupleReader &pTupleReaderInit,
    TupleData &bitmapSegTuple)
{
    init(pTupleReaderInit, bitmapSegTuple, false, NULL);
}

void LbmSegmentReader::init(
    SharedLbmTupleReader &pTupleReaderInit,
    TupleData &bitmapSegTuple,
    bool setBitmapInit,
    boost::dynamic_bitset<> *pBitmapInit)
{
    LbmSegmentReaderBase::init(
        pTupleReaderInit,
        bitmapSegTuple,
        setBitmapInit,
        pBitmapInit);
    initCommon();
}

void LbmSegmentReader::initCommon()
{
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

ExecStreamResult LbmSegmentReader::advanceToByte(LbmByteNumber byteNum)
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
    return advanceToByte(ridToByteNumber(rid));
}

void LbmSegmentReader::readCurrentByteSegment(
    LcsRid &startRid, PBuffer &byteSeg, uint &len)
{
    assert(firstReadDone);
    startRid = byteNumberToRid(byteSegOffset);
    byteSeg = pSegStart;
    len = byteSegLen;
    // assumes advanceToByte() has been called to move to a segment
    // that has actual bits set, and intermediate zeros have been removed
    assert(*byteSeg != 0);
    assert(len > 0);
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSegmentReader.cpp

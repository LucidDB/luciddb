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
#include "fennel/lbm/LbmSeqSegmentReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmSeqSegmentReader::init(
    SharedExecStreamBufAccessor &pInAccessorInit,
    TupleData &bitmapSegTuple)
{
    LbmSegmentReaderBase::init(pInAccessorInit, bitmapSegTuple);
}

ExecStreamResult LbmSeqSegmentReader::readSegmentAndAdvance(
    LbmByteNumber &byteNum, PBuffer &byteSeg, uint &len)
{
    if (pSegDescStart >= pSegDescEnd) {
        // read a new bitmap segment tuple from the input stream
        ExecStreamResult rc = readBitmapSegTuple();
        if (rc != EXECRC_YIELD) {
            return rc;
        }

        if (!pSegDescStart) {
            // single bitmap case
            byteNum = byteSegOffset;
            byteSeg = pSegStart;
            len = byteSegLen;
            return EXECRC_YIELD;
        } else {
            // set byteSegLen to 0 to force advanceSegment()
            // to read the initial segment
            byteSegLen = 0;
        }
    }

    // advance to the next segment and set the return values
    advanceSegment();
    byteNum = byteSegOffset;
    byteSeg = pSegStart;
    len = byteSegLen;

    return EXECRC_YIELD;
}

LcsRid LbmSeqSegmentReader::getSrid()
{
    return startRID;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmSeqSegmentReader.cpp

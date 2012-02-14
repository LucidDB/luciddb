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
#include "fennel/lbm/LbmIntersectExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmIntersectExecStream::prepare(LbmIntersectExecStreamParams const &params)
{
    LbmBitOpExecStream::prepare(params);
}

void LbmIntersectExecStream::open(bool restart)
{
    LbmBitOpExecStream::open(restart);
    iInput = 0;
    nMatches = 0;
    minLen = 0;
}

ExecStreamResult LbmIntersectExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (producePending) {
        ExecStreamResult rc = producePendingOutput(iInput);
        if (rc != EXECRC_YIELD) {
            return rc;
        }
        nMatches = 0;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        while (nMatches < nInputs) {
            // get the first segment from the input with at least a starting
            // rid of startRid
            LcsRid currRid;
            PBuffer currByteSeg;
            uint currLen;

            ExecStreamResult rc = readInput(
                iInput, currRid, currByteSeg, currLen);
            if (rc != EXECRC_YIELD) {
                return rc;
            }

            // if the starting rid of this current segment that has just
            // been read matches the desired starting rid, indicate that
            // we have a match; otherwise, reset the starting rid to the
            // new, larger rid value and start all over again, looking
            // for matches from all other input streams
            assert(currRid >= startRid);
            if (currRid > startRid) {
                startRid = currRid;
                writeStartRidParamValue();
                nMatches = 1;
                minLen = currLen;
            } else {
                // the shortest segment indicates where the overlapping ends
                if (nMatches == 0 || currLen < minLen) {
                    minLen = currLen;
                }
                nMatches++;
            }

            // now try reading segments from the other streams
            iInput = (iInput + 1) % nInputs;
        }

        // intersect the overlapping segments
        if (!intersectSegments(minLen)) {
            return EXECRC_BUF_OVERFLOW;
        }

        nMatches = 0;
    }

    return EXECRC_QUANTUM_EXPIRED;
}

bool LbmIntersectExecStream::intersectSegments(uint len)
{
    LcsRid currRid;
    PBuffer currByteSeg;
    uint currLen;

    // initialize temporary buffer with all 1's
    for (uint i = 0; i < len; i++) {
        pByteSegBuf[i] = 0xff;
    }

    // retrieve each current segment and perform the AND operation
    for (uint i = 0; i < nInputs; i++) {
        segmentReaders[i].readCurrentByteSegment(
            currRid, currByteSeg, currLen);
        if (i == 0) {
            addRid = currRid;
        } else {
            permAssert(addRid == currRid);
        }
        // byte segments are stored in reverse order, so currByteSeg points
        // to the end of the buffer whereas pByteSegBuf points to the
        // beginning but needs to be filled in backwards
        for (int j = 0; j < len; j++) {
            pByteSegBuf[len - j - 1] &= currByteSeg[-j];
        }
    }

    // the next set of segments to read will start past the end of
    // the overlapping segments just read
    startRid = addRid + len * LbmSegment::LbmOneByteSize;
    writeStartRidParamValue();

    // add the AND'd segment to the segment under construction;
    // if the segment is full and the output buffer fills up writing
    // out the segment, return
    assert(len > 0);
    addLen = len;
    addByteSeg = pByteSegBuf;
    if (!addSegments()) {
        return false;
    }

    return true;
}

void LbmIntersectExecStream::closeImpl()
{
    LbmBitOpExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmIntersectExecStream.cpp

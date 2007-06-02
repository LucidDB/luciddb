/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2007 LucidEra, Inc.
// Copyright (C) 2006-2007 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmIntersectExecStream.h"

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
                pDynamicParamManager->writeParam(
                    startRidParamId, startRidDatum);
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
            iInput = ++iInput % nInputs;
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
    pDynamicParamManager->writeParam(startRidParamId, startRidDatum);

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

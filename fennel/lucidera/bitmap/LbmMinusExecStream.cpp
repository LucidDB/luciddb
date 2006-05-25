/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2006-2006 LucidEra, Inc.
// Copyright (C) 2006-2006 The Eigenbase Project
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
#include "fennel/lucidera/bitmap/LbmMinusExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmMinusExecStream::LbmMinusExecStream() : LbmBitOpExecStream()
{
}

void LbmMinusExecStream::prepare(LbmMinusExecStreamParams const &params)
{
    LbmBitOpExecStream::prepare(params);
}

void LbmMinusExecStream::open(bool restart)
{
    LbmBitOpExecStream::open(restart);
    childrenDone = false;
    needToRead = true;
    minChildRid = LcsRid(0);
    // since the children need to read till EOS, don't set a rowLimit
    rowLimit = 0;
}

ExecStreamResult LbmMinusExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc;

    if (producePending) {
        rc = producePendingOutput(0);
        if (rc != EXECRC_YIELD) {
            return rc;
        }
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {

        // read a segment from the anchor if we've finished processing the
        // previous segment
        if (needToRead) {
            rc = readInput(0, baseRid, baseByteSeg, baseLen);
            if (rc != EXECRC_YIELD) {
                return rc;
            }
            memcpy(pByteSegBuf, baseByteSeg - baseLen + 1, baseLen);
            needToRead = false;
            // reset the startrid to the rid just read in and write the
            // dynamic parameter so the children can skip forward to that
            // rid
            startRid = baseRid;
            pDynamicParamManager->writeParam(startRidParamId, startRidDatum);
            iInput = 1;
        }

        // minus the children input, if they haven't all reached EOS
        if (!childrenDone) {
           
            rc = advanceChildren(baseRid);
            if (rc != EXECRC_YIELD) {
                return rc;
            }

            rc = minusSegments(baseRid, baseByteSeg, baseLen);
            if (rc != EXECRC_YIELD && rc != EXECRC_EOS) {
                return rc;
            }
        }

        // bump up the startrid past the segment just read and
        // write out the anchor segment
        needToRead = true;
        startRid = baseRid + baseLen * LbmSegment::LbmOneByteSize;
        addRid = baseRid;
        addByteSeg = pByteSegBuf;
        addLen = baseLen;
        if (!addSegments()) {
            return EXECRC_BUF_OVERFLOW;
        }

        // loop back to read the next anchor segment
    }

    return EXECRC_QUANTUM_EXPIRED;
}

ExecStreamResult LbmMinusExecStream::advanceChildren(LcsRid baseRid)
{
    // no need to advance children if they're all positioned past the anchor
    if (minChildRid > baseRid) {
        return EXECRC_YIELD;
    }

    // advance the children input, resuming at the one where we last left off
    for (; iInput < nInputs; iInput++) {
        ExecStreamResult rc = segmentReaders[iInput].advanceToRid(baseRid);
        if (rc == EXECRC_EOS) {
            continue;
        }
        if (rc != EXECRC_YIELD) {
            return rc;
        }
    }

    return EXECRC_YIELD;
}

ExecStreamResult LbmMinusExecStream::minusSegments(
    LcsRid baseRid, PBuffer baseByteSeg, uint baseLen)
{
    while (true) {

        // find the child with the minimum startrid and read its current
        // segment
        int minInput;
        ExecStreamResult rc = findMinInput(minInput);
        if (rc == EXECRC_EOS) {
            return rc;
        }

        LcsRid currRid;
        PBuffer currByteSeg;
        uint currLen;
        segmentReaders[minInput].readCurrentByteSegment(
            currRid, currByteSeg, currLen);

        // if the non-anchor inputs are not within the range of the
        // anchor's current rid range, ignore the current segment and
        // get a new one
        uint offset =
            opaqueToInt(currRid - baseRid) / LbmSegment::LbmOneByteSize;
        if (offset >= baseLen) {
            break;
        }

        // only read from the children input the amount that will
        // match the anchor's segment
        currLen = std::min(currLen, baseLen - offset);

        // minus from the anchor -- note that segments are stored 
        // backwards
        PBuffer out = pByteSegBuf + baseLen - 1 - offset; 
        uint len = currLen;
        while (len--) {
            *out-- &= ~(*currByteSeg--);
        }

        // advance the child by the amount read in; note that we don't return
        // if this child has reached EOS, as there may still be other children
        // that aren't in the EOS state
        rc = segmentReaders[minInput].advanceToRid(
            currRid + currLen * LbmSegment::LbmOneByteSize);
        if (rc != EXECRC_YIELD && rc != EXECRC_EOS) {
            return rc;
        }
    }

    return EXECRC_YIELD;
}

ExecStreamResult LbmMinusExecStream::findMinInput(int &minInput)
{
    minInput = -1;

    for (uint i = 1; i < nInputs; i++) {
        if (inAccessors[i]->getState() == EXECBUF_EOS) {
            continue;
        }

        LcsRid currRid;
        PBuffer currByteSeg;
        uint currLen;
        segmentReaders[i].readCurrentByteSegment(
            currRid, currByteSeg, currLen);

        if (minInput == -1 || currRid < minChildRid) {
            minInput = i;
            minChildRid = currRid;
        }
    }

    if (minInput == -1) {
        childrenDone = true;
        return EXECRC_EOS;
    } else {
        return EXECRC_YIELD;
    }
}

void LbmMinusExecStream::closeImpl()
{
    LbmBitOpExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmMinusExecStream.cpp

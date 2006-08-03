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
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/lucidera/bitmap/LbmMinusExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id:");

LbmMinusExecStream::LbmMinusExecStream() : LbmBitOpExecStream()
{
}

void LbmMinusExecStream::prepare(LbmMinusExecStreamParams const &params)
{
    LbmBitOpExecStream::prepare(params);

    if (nFields) {
        // compute an output tuple based on the minuend
        prefixedBitmapTuple.compute(inAccessors[0]->getTupleDesc());

        // prevTuple contains only the prefix fields, it has it's own
        // storage to track the previous tuple when the current tuple
        // pointers have moved forward
        TupleDescriptor prevTupleDesc;
        TupleDescriptor const &inputDesc = inAccessors[0]->getTupleDesc();
        for (int i = 0; i < nFields; i ++) {
            prevTupleDesc.push_back(inputDesc[i]);
        }
        prevTuple.computeAndAllocate(prevTupleDesc);
    }
}

void LbmMinusExecStream::open(bool restart)
{
    LbmBitOpExecStream::open(restart);
    childrenDone = false;
    needToRead = true;
    minChildRid = LcsRid(0);
    advancePending = false;
    // since the children need to read till EOS, don't set a rowLimit
    rowLimit = 0;
    state = FIRST_MINUS;
    copyPrefixPending = false;
    prevTupleValid = false;
    minuendReader.init(inAccessors[0], bitmapSegTuples[0]);
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

    if (copyPrefixPending) {
        copyPrefix();
        copyPrefixPending = false;
        needToRead = false;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {

        // read a segment from the anchor if we've finished processing the
        // previous segment
        if (needToRead) {
            rc = readMinuendInputAndRestart(baseRid, baseByteSeg, baseLen);
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
        if ((state != EMPTY_INPUT) && !childrenDone) {
           
            if (advancePending) {
                rc = advanceChild(advanceChildInputNo, advanceChildRid);
                if (rc != EXECRC_YIELD && rc != EXECRC_EOS) {
                    return rc;
                }
                advancePending = false;
            } else {
                rc = advanceChildren(baseRid);
                if (rc != EXECRC_YIELD) {
                    return rc;
                }
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

ExecStreamResult LbmMinusExecStream::readMinuendInputAndRestart(
    LcsRid &currRid, PBuffer &currByteSeg, uint &currLen)
{
    ExecStreamResult rc = EXECRC_YIELD;

    // If there are no keys, read as the minuend as an ordered input
    if (!nFields) {
        rc = readInput(0, currRid, currByteSeg, currLen);
        return rc;
    }

    // Otherwise, read the minuend as a random sequence of segments
    rc = readMinuendInput(currRid, currByteSeg, currLen);

    // When a minuend's bitmap tuple changes, the RIDs may be unordered.
    // Restart the subtrahends so all of their data can be minused
    // from the next minuend input.
    //
    // Also flush the segment writer's current tuple. If it cannot be
    // written, then we can't copy the next prefix yet, because the old
    // values will be used to construct the pending output tuple.
    if (prevTupleValid) {
        if (minuendReader.getTupleChange()) {
            minuendReader.resetChangeListener();
            int keyComp = comparePrefixes();
            // NOTE: if prefix key is a partial subset of index key then
            // rids may restart though the prefix key has not changed
            if (keyComp != 0 || currRid < startRid) {
                restartSubtrahends();
                if (!flush()) {
                    copyPrefixPending = true;
                    return EXECRC_BUF_OVERFLOW;
                }
                copyPrefix();
            }
        }
    } else {
        prevTupleValid = true;
        copyPrefix();
        minuendReader.resetChangeListener();
    }
    return rc;
}

ExecStreamResult LbmMinusExecStream::readMinuendInput(
    LcsRid &currRid, PBuffer &currByteSeg, uint &currLen)
{
    LbmByteNumber byteNumber;
    ExecStreamResult rc = minuendReader.readSegmentAndAdvance(
        byteNumber, currByteSeg, currLen);
    currRid = byteNumberToRid(byteNumber);
    if (rc == EXECRC_EOS) {
        // write out the last pending segment
        if (! flush()) {
            return EXECRC_BUF_OVERFLOW;
        }
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    } else if (rc != EXECRC_YIELD) {
        return rc;
    }

    // segment read should never be larger than space available
    // for segments
    assert(currLen <= bitmapBufSize);

    return EXECRC_YIELD;    
}

int LbmMinusExecStream::comparePrefixes()
{
    int ret =
        (inAccessors[0]->getTupleDesc()).compareTuplesKey(
            prevTuple,
            bitmapSegTuples[0],
            nFields);    
    return ret;
}

void LbmMinusExecStream::restartSubtrahends()
{
    childrenDone = false;
    minChildRid = LcsRid(0);
    advancePending = false;
    for (uint i = 1; i < nInputs; i++) {
        pGraph->getStreamInput(getStreamId(), i)->open(true);
        segmentReaders[i].init(inAccessors[i], bitmapSegTuples[i]);
    }
}

void LbmMinusExecStream::copyPrefix()
{
    /*
      Need to make sure pointers are allocated before memcpy.
      resetBuffer restores the pointers to the associated buffer.
    */
    prevTuple.resetBuffer();
    
    for (int i = 0; i < nFields; i ++) {
        prevTuple[i].memCopyFrom(bitmapSegTuples[0][i]);
    }
}

ExecStreamResult LbmMinusExecStream::advanceChild(int inputNo, LcsRid rid)
{
    ExecStreamResult rc = segmentReaders[inputNo].advanceToRid(rid);
    return rc;
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
            advancePending = true;
            advanceChildRid = currRid + currLen * LbmSegment::LbmOneByteSize;
            advanceChildInputNo = minInput;
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
        if (state == FIRST_MINUS) {
            state = EMPTY_INPUT;
        }
        return EXECRC_EOS;
    } else {
        if (state == FIRST_MINUS) {
            state = NONEMPTY_INPUT;
        }
        return EXECRC_YIELD;
    }
}

bool LbmMinusExecStream::produceTuple(TupleData bitmapTuple)
{
    // If the minuend contained prefix fields, they are prepended to
    // the output: ([optional prefix fields], bitmap)
    if (nFields) {
        for (uint i = 0; i < nFields; i++) {
            prefixedBitmapTuple[i].copyFrom(prevTuple[i]);
        }
        assert (prefixedBitmapTuple.size() == nFields + bitmapTuple.size());
        for (uint i = 0; i < 3; i++) {
            prefixedBitmapTuple[nFields+i].copyFrom(bitmapTuple[i]);
        }
        return pOutAccessor->produceTuple(prefixedBitmapTuple);
    }
    return pOutAccessor->produceTuple(bitmapTuple);
}

void LbmMinusExecStream::closeImpl()
{
    LbmBitOpExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmMinusExecStream.cpp

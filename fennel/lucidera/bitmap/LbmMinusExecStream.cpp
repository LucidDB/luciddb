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
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/lucidera/bitmap/LbmMinusExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id:");

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
    subtrahendBitmap.resize(0);
}

void LbmMinusExecStream::open(bool restart)
{
    LbmBitOpExecStream::open(restart);
    subtrahendsDone = false;
    needToRead = true;
    minSubtrahendRid = LcsRid(0);
    maxSubtrahendRid = LcsRid(0);
    baseRid = LcsRid(0);
    advancePending = false;
    // since the subtrahends need to read till EOS, don't set a rowLimit
    rowLimit = 0;
    inputType = UNKNOWN_INPUT;
    copyPrefixPending = false;
    prevTupleValid = false;
    minuendReader.init(inAccessors[0], bitmapSegTuples[0]);

    if (nFields > 0) {
        subtrahendBitmap.resize(SUBTRAHEND_BITMAP_SIZE);
    }
    restartSubtrahends();
}

ExecStreamResult LbmMinusExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc;

    // On the first execution, check whether any subtrahend has data
    if (inputType == UNKNOWN_INPUT) {
        rc = advanceSubtrahends(LcsRid(0));
        if (rc != EXECRC_YIELD) {
            return rc;
        }
        int dummy;
        rc = findMinInput(dummy);
        if (rc == EXECRC_EOS) {
            inputType = EMPTY_INPUT;
            if (nFields == 0) {
                subtrahendsDone = true;
            }
        } else {
            inputType = NONEMPTY_INPUT;
        }
    }

    if (producePending) {
        rc = producePendingOutput(0);
        if (rc != EXECRC_YIELD) {
            return rc;
        }
    }

    bool skipMinus = false;
    if (copyPrefixPending) {
        copyPrefix();
        copyPrefixPending = false;
        needToRead = false;
        // Since we bypassed the restart check when this current minuend
        // was read, we need to do the check here
        skipMinus = checkNeedForRestart();
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        // read a segment from the minuend if we've finished processing the
        // previous segment
        if (needToRead) {
            rc = readMinuendInputAndFlush(baseRid, baseByteSeg, baseLen);
            if (rc != EXECRC_YIELD) {
                return rc;
            }

            // See if we need to restart the subtrahends
            skipMinus = checkNeedForRestart();
        }

        // Minus the subtrahends if they haven't all reached EOS in the
        // case where there are no keys.  In the case where there are keys,
        // the bitmap determines whether we can skip the minus.
        if ((nFields == 0 && !subtrahendsDone) || !skipMinus) {
            if (advancePending) {
                rc =
                    advanceSingleSubtrahend(
                        advanceSubtrahendInputNo,
                        advanceSubtrahendRid);
                if (rc != EXECRC_YIELD && rc != EXECRC_EOS) {
                    return rc;
                }
                advancePending = false;
            } else {
                rc = advanceSubtrahends(baseRid);
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
        // write out the minuend segment
        needToRead = true;
        startRid = baseRid + baseLen * LbmSegment::LbmOneByteSize;
        addRid = baseRid;
        addByteSeg = pByteSegBuf;
        addLen = baseLen;
        if (!addSegments()) {
            return EXECRC_BUF_OVERFLOW;
        }

        // loop back to read the next minuend segment
    }

    return EXECRC_QUANTUM_EXPIRED;
}

ExecStreamResult LbmMinusExecStream::readMinuendInputAndFlush(
    LcsRid &currRid, PBuffer &currByteSeg, uint &currLen)
{
    ExecStreamResult rc;
    bool unordered = false;

    // If there are no keys, read as the minuend as an ordered input.
    // Otherwise, read the minuend as a random sequence of segments.
    if (nFields == 0) {
        rc = readInput(0, currRid, currByteSeg, currLen);
    } else {
        rc = readMinuendInput(currRid, currByteSeg, currLen);
        if (currRid < startRid) {
            unordered = true;
        }
    }
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    // Store the segment just read
    memcpy(pByteSegBuf, baseByteSeg - baseLen + 1, baseLen);
    needToRead = false;
    // reset the startrid to the rid just read in and write the
    // dynamic parameter so the subtrahends can skip forward to that
    // rid
    startRid = baseRid;
    writeStartRidParamValue();
    iInput = 1;

    // If there are no keys (the usual case), we never need to restart inputs
    if (nFields == 0) {
        return rc;
    }

    // If there are keys, then data is expected to come from an index. RIDs
    // may be ordered for each key, but are not ordered for the entire stream.
    // In fact, when minus keys are only a subset of an index's keys, then
    // RIDs may restart at any time.
    // (Ex: RIDs in index [K1, K2] are ordered for each pair [k1, k2].
    // However, a minus based on [K1] will be completely out of order.)
    //
    // Due to the lack of ordering, we may need to restart subtrahends
    // whenever the minuend is out of order so all of the subtrahend data
    // can be minused from the next minuend input.  That is handled outside
    // of this method because restarts don't always need to be done
    // immediately after a new key is read.
    //
    // We also flush the segment writer's current tuple. If it cannot be
    // written, then we can't copy the next prefix yet, because the old
    // values will be used to construct the pending output tuple.
    if (prevTupleValid) {
        if (minuendReader.getTupleChange()) {
            minuendReader.resetChangeListener();
            int keyComp = comparePrefixes();
            if (keyComp != 0 || unordered) {
                needSubtrahendRestart = true;
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
    minSubtrahendRid = LcsRid(0);
    advancePending = false;
    for (uint i = 1; i < nInputs; i++) {
        pGraph->getStreamInput(getStreamId(), i)->open(true);
        segmentReaders[i].init(
            inAccessors[i],
            bitmapSegTuples[i],
            (!subtrahendsDone && nFields > 0),
            &subtrahendBitmap);
    }
    iInput = 1;
    needSubtrahendRestart = false;
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

ExecStreamResult LbmMinusExecStream::advanceSingleSubtrahend(
    int inputNo,
    LcsRid rid)
{
    ExecStreamResult rc = segmentReaders[inputNo].advanceToRid(rid);
    return rc;
}

ExecStreamResult LbmMinusExecStream::advanceSubtrahends(LcsRid baseRid)
{
    // no need to advance subtrahends if they're all positioned past the
    // minuend
    if (minSubtrahendRid > baseRid) {
        return EXECRC_YIELD;
    }

    // advance the subtrahends, resuming at the one where we last left off
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

bool LbmMinusExecStream::checkNeedForRestart()
{
    // Return value indicates whether the minus can be skipped, not whether
    // a restart was done.

    if (nFields == 0) {
        return false;
    } else if (inputType == EMPTY_INPUT) {
        // If the input is empty, we can always bypass the minus
        return true;
    } else if (needSubtrahendRestart) {
        bool skipMinus = canSkipMinus();
        // If there are potentially overlapping rids and the subtrahend is
        // positioned past the current minuend, we need to restart
        if (!skipMinus && minSubtrahendRid > startRid) {
            restartSubtrahends();
        }
        return skipMinus;
    } else {
        return canSkipMinus();
    }
}

bool LbmMinusExecStream::canSkipMinus()
{
    LcsRid rid = baseRid;
    LcsRid endRid = baseRid + baseLen * LbmSegment::LbmOneByteSize - 1;

    // Determine whether the rids in the current minuend segment are
    // "covered" by the bitmap in its current state
    if (subtrahendsDone) {
        // If the first rid we're interested in extends past the max rid read
        // from all subtrahends, then there are no rids to subtract off.
        if (rid > maxSubtrahendRid) {
            return true;
        }
    } else {
        // If the last rid we're interested in extends past the max rid read
        // from any subtrahend, then we can't use the bitmap to determine if
        // we can skip the minus.
        for (uint i = 1; i < nInputs; i++) {
            if (endRid > segmentReaders[i].getMaxRidSet()) {
                return false;
            }
        }
    }

    PBuffer seg = baseByteSeg;
    for (uint i = 0; i < baseLen; i++) {
        uint8_t byte = *((uint8_t *) seg);
        for (uint j = 0; j < LbmSegment::LbmOneByteSize; j++) {
            if (byte & 1) {
                // once we find a match, no need to look any further
                if (subtrahendBitmap.test(
                    opaqueToInt(rid % SUBTRAHEND_BITMAP_SIZE)))
                {
                    return false;
                }
            }
            byte = byte >> 1;
            rid++;
        }
        seg--;
    }
    return true;
}

ExecStreamResult LbmMinusExecStream::minusSegments(
    LcsRid baseRid, PBuffer baseByteSeg, uint baseLen)
{
    while (true) {
        // find the subtrahend with the minimum startrid and read its current
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

        // if the subtrahends are not within the range of the minuend's
        // current rid range, ignore the current segment and get a new one
        uint offset =
            opaqueToInt(currRid - baseRid) / LbmSegment::LbmOneByteSize;
        if (offset >= baseLen) {
            break;
        }

        // only read from the subtrahends the amount that will match the
        // minuend's segment
        currLen = std::min(currLen, baseLen - offset);

        // minus from the minuend -- note that segments are stored
        // backwards
        PBuffer out = pByteSegBuf + baseLen - 1 - offset;
        uint len = currLen;
        while (len--) {
            *out-- &= ~(*currByteSeg--);
        }

        // advance the subtrahend by the amount read in; note that we don't
        // return if this subtrahend has reached EOS, as there may still be
        // other subtrahends that aren't in the EOS state
        rc = segmentReaders[minInput].advanceToRid(
                currRid + currLen * LbmSegment::LbmOneByteSize);
        if (rc != EXECRC_YIELD && rc != EXECRC_EOS) {
            advancePending = true;
            advanceSubtrahendRid =
                currRid + currLen * LbmSegment::LbmOneByteSize;
            advanceSubtrahendInputNo = minInput;
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

        if (minInput == -1 || currRid < minSubtrahendRid) {
            minInput = i;
            minSubtrahendRid = currRid;
        }
    }

    if (minInput == -1) {
        // Note that once we've made one pass over the subtrahends, by setting
        // subtrahendsDone, we'll avoid resetting the bits on subsequent passes.
        // Position minSubtrahendRid past the max subtrahend rid since the
        // subtrahends are no longer positioned at that minimum rid.
        subtrahendsDone = true;
        for (uint i = 1; i < nInputs; i++) {
            LcsRid rid = segmentReaders[i].getMaxRidSet();
            if (rid > maxSubtrahendRid) {
                maxSubtrahendRid = rid;
            }
        }
        minSubtrahendRid = maxSubtrahendRid + 1;
        return EXECRC_EOS;
    } else {
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
            prefixedBitmapTuple[nFields + i].copyFrom(bitmapTuple[i]);
        }
        return pOutAccessor->produceTuple(prefixedBitmapTuple);
    }
    return pOutAccessor->produceTuple(bitmapTuple);
}

void LbmMinusExecStream::closeImpl()
{
    subtrahendBitmap.resize(0);
    LbmBitOpExecStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmMinusExecStream.cpp

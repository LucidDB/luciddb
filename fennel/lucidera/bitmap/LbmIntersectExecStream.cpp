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
#include "fennel/lucidera/bitmap/LbmIntersectExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmIntersectExecStream::LbmIntersectExecStream()
{
    dynParamsCreated = false;
}

void LbmIntersectExecStream::prepare(LbmIntersectExecStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);

    // set dynanmic parameter ids
    rowLimitParamId = params.rowLimitParamId;
    startRidParamId = params.startRidParamId;
    assert(opaqueToInt(rowLimitParamId) > 0);
    assert(opaqueToInt(startRidParamId) > 0);

    // setup tupledatums for writing dynamic parameter values
    rowLimitDatum.pData = (PConstBuffer) &rowLimit;
    rowLimitDatum.cbData = sizeof(rowLimit);
    startRidDatum.pData = (PConstBuffer) &startRid;
    startRidDatum.cbData = sizeof(startRid);

    // initialize segment readers for reading bitmaps from input stream
    nInputs = inAccessors.size();
    segmentReaders.reset(new LbmSegmentReader[nInputs]);
    bitmapSegTuples.reset(new TupleData[nInputs]);
    for (uint i = 0; i < nInputs; i++) {
        assert(
            inAccessors[i]->getTupleDesc() == inAccessors[0]->getTupleDesc());
        bitmapSegTuples[i].compute(inAccessors[i]->getTupleDesc());
    }

    assert(inAccessors[0]->getTupleDesc() == pOutAccessor->getTupleDesc());
}

void LbmIntersectExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);
    if (!restart) {
        // allocate output buffer and a temporary buffer for "AND'ing"
        // together segments; the output buffer size is based on the size
        // required for building an LbmEntry, while the temporary buffer
        // should not be larger than an LbmEntry supports
        uint bitmapColSize = pOutAccessor->getTupleDesc()[1].cbStorage;
        uint outputBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
        outputBuf.reset(new FixedBuffer[outputBufSize]);
        segmentWriter.init(
            outputBuf.get(), outputBufSize, pOutAccessor->getTupleDesc(), true);
        bitmapBufSize = LbmEntry::getMaxBitmapSize(bitmapColSize);
        byteSegBuf.reset(new FixedBuffer[bitmapBufSize]); 
        pByteSegBuf = byteSegBuf.get();

        // create dynamic parameters
        pDynamicParamManager->createParam(
            rowLimitParamId, pOutAccessor->getTupleDesc()[0]);
        pDynamicParamManager->createParam(
            startRidParamId, pOutAccessor->getTupleDesc()[0]);
        dynParamsCreated = true;
    } else {
        segmentWriter.reset();
    }
    iInput = 0;
    nMatches = 0;
    startRid = LcsRid(0);
    minLen = 0;
    rowLimit = 1;
    producePending = false;
    pDynamicParamManager->writeParam(rowLimitParamId, rowLimitDatum);
    pDynamicParamManager->writeParam(startRidParamId, startRidDatum);
    for (uint i = 0; i < nInputs; i++) {
        segmentReaders[i].init(inAccessors[i], bitmapSegTuples[i]);
    }
}

ExecStreamResult LbmIntersectExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (producePending) {
        if (!pOutAccessor->produceTuple(outputTuple)) {
            return EXECRC_BUF_OVERFLOW;
        }
        // in the middle of adding segments when buffer overflow occurred;
        // go back and add the remaining segments
        if (!segmentWriter.isEmpty()) {
            segmentWriter.reset();
            if (!addSegments()) {
                return EXECRC_BUF_OVERFLOW;
            }
        }
        producePending = false;
        nMatches = 0;
        if (inAccessors[iInput]->getState() == EXECBUF_EOS) {
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        }
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {

        while (nMatches < nInputs) {

            // get the first segment from the input with at least a starting
            // rid of startRid
            ExecStreamResult rc = segmentReaders[iInput].advanceToRid(startRid);

            if (rc == EXECRC_EOS) {
                // write out the last pending segment
                if (!segmentWriter.isEmpty()) {
                    outputTuple = segmentWriter.produceSegmentTuple();
                    segmentWriter.reset();
                    if (!pOutAccessor->produceTuple(outputTuple)) {
                        producePending = true;
                        return EXECRC_BUF_OVERFLOW;
                    }
                }
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            } else if (rc != EXECRC_YIELD) {
                return rc;
            }

            LcsRid currRid;
            PBuffer currByteSeg;
            uint currLen;

            segmentReaders[iInput].readCurrentByteSegment(
                currRid, currByteSeg, currLen);
            // segment read should never be larger than space available
            // for segments
            assert(currLen <= bitmapBufSize);

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

bool LbmIntersectExecStream::addSegments()
{
    while (addLen > 0) {

        if (segmentWriter.addSegment(addRid, addByteSeg, addLen)) {
            break;
        }

        outputTuple = segmentWriter.produceSegmentTuple();
        if (!pOutAccessor->produceTuple(outputTuple)) {
            producePending = true;
            return false;
        }

        // loop back and start creating a new segment for the remainder of
        // the segments that wouldn't fit
        segmentWriter.reset();
    }

    return true;
}

void LbmIntersectExecStream::closeImpl()
{
    ConfluenceExecStream::closeImpl();
    if (dynParamsCreated) {
        pDynamicParamManager->deleteParam(rowLimitParamId);
        pDynamicParamManager->deleteParam(startRidParamId);
    }
    outputBuf.reset();
    byteSegBuf.reset();
}

FENNEL_END_CPPFILE("$Id$");

// End LbmIntersectExecStream.cpp

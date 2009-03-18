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
#include "fennel/lucidera/bitmap/LbmBitOpExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void LbmBitOpExecStream::prepare(LbmBitOpExecStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);

    // set dynanmic parameter ids
    rowLimitParamId = params.rowLimitParamId;
    startRidParamId = params.startRidParamId;

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
        bitmapSegTuples[i].compute(inAccessors[i]->getTupleDesc());
    }

    nFields = inAccessors[0]->getTupleDesc().size() - 3;
}

void LbmBitOpExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    uint nKeys = pOutAccessor->getTupleDesc().size() - 3;

    if (!restart) {
        // create dynamic parameters with the same type as the first bitmap
        // field, a RID, if this is the first time open is called
        if (opaqueToInt(rowLimitParamId) > 0) {
            pDynamicParamManager->createParam(
                rowLimitParamId, pOutAccessor->getTupleDesc()[nKeys]);
        }
        if (opaqueToInt(startRidParamId) > 0) {
            pDynamicParamManager->createParam(
                startRidParamId, pOutAccessor->getTupleDesc()[nKeys]);
        }
    }

    // need to allocate buffers if this is the very first open, or if a
    // previous close freed up the buffers; note that we don't check "restart"
    // in case the stream was closed early, and then reopened in restart mode
    if (!outputBuf) {
        // allocate output buffer; the output buffer size is based on the size
        // required for building a LbmEntry
        uint bitmapColSize = pOutAccessor->getTupleDesc()[nKeys+1].cbStorage;
        uint outputBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
        outputBuf.reset(new FixedBuffer[outputBufSize]);

        // initialize the writer to produce bitmap tuples; the second input
        // should be a bitmap input
        segmentWriter.init(
            outputBuf.get(), outputBufSize,
            inAccessors[1]->getTupleDesc(), true);

        // allocate a temporary buffer for the bit operation; the temporary
        // buffer should not be larger than what a LbmEntry supports
        bitmapBufSize = LbmEntry::getMaxBitmapSize(bitmapColSize);
        byteSegBuf.reset(new FixedBuffer[bitmapBufSize]);
        pByteSegBuf = byteSegBuf.get();

    } else {
        segmentWriter.reset();
    }

    startRid = LcsRid(0);
    rowLimit = 1;
    producePending = false;
    writeStartRidParamValue();
    if (opaqueToInt(rowLimitParamId) > 0) {
        pDynamicParamManager->writeParam(rowLimitParamId, rowLimitDatum);
    }
    for (uint i = 0; i < nInputs; i++) {
        segmentReaders[i].init(inAccessors[i], bitmapSegTuples[i]);
    }
}

ExecStreamResult LbmBitOpExecStream::producePendingOutput(uint iInput)
{
    if (!produceTuple(outputTuple)) {
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
    if (inAccessors[iInput]->getState() == EXECBUF_EOS) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    return EXECRC_YIELD;
}

ExecStreamResult LbmBitOpExecStream::readInput(
    uint iInput, LcsRid &currRid, PBuffer &currByteSeg, uint &currLen)
{
    ExecStreamResult rc = segmentReaders[iInput].advanceToRid(startRid);

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

    segmentReaders[iInput].readCurrentByteSegment(
        currRid, currByteSeg, currLen);
    // segment read should never be larger than space available
    // for segments
    assert(currLen <= bitmapBufSize);

    return EXECRC_YIELD;
}

bool LbmBitOpExecStream::flush()
{
    assert (!producePending);

    if (!segmentWriter.isEmpty()) {
        outputTuple = segmentWriter.produceSegmentTuple();
        segmentWriter.reset();
        if (!produceTuple(outputTuple)) {
            producePending = true;
        }
    }
    return !producePending;
 }

bool LbmBitOpExecStream::addSegments()
{
    while (addLen > 0) {

        if (segmentWriter.addSegment(addRid, addByteSeg, addLen)) {
            break;
        }

        outputTuple = segmentWriter.produceSegmentTuple();
        if (!produceTuple(outputTuple)) {
            producePending = true;
            return false;
        }

        // loop back and start creating a new segment for the remainder of
        // the segments that wouldn't fit
        segmentWriter.reset();
    }

    return true;
}

bool LbmBitOpExecStream::produceTuple(TupleData bitmapTuple)
{
    assert(pOutAccessor->getTupleDesc().size() == bitmapTuple.size());
    return pOutAccessor->produceTuple(bitmapTuple);
}

void LbmBitOpExecStream::closeImpl()
{
    ConfluenceExecStream::closeImpl();
    outputBuf.reset();
    byteSegBuf.reset();
}

void LbmBitOpExecStream::writeStartRidParamValue()
{
    if (opaqueToInt(startRidParamId) > 0) {
        pDynamicParamManager->writeParam(startRidParamId, startRidDatum);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LbmBitOpExecStream.cpp

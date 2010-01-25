/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2010-2010 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
#include "fennel/lbm/LbmChopperExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LbmChopperExecStream::LbmChopperExecStream()
{
    ridLimitParamId = DynamicParamId(0);
}

void LbmChopperExecStream::prepare(LbmChopperExecStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);

    // set dynamic parameter ids
    ridLimitParamId = params.ridLimitParamId;
    assert(opaqueToInt(ridLimitParamId) > 0);

    // initialize reader
    inputTuple.compute(inAccessors[0]->getTupleDesc());

    // output buffer will come from scratch segment
    SegmentAccessor scratchAccessor = params.scratchAccessor;
    writerPageLock.accessSegment(scratchAccessor);
    pageSize = scratchAccessor.pSegment->getUsablePageSize();
}

void LbmChopperExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConfluenceExecStream::getResourceRequirements(minQuantity, optQuantity);

    // one page for the writer
    minQuantity.nCachePages += 1;
    optQuantity.nCachePages += 1;
}

void LbmChopperExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    if (!restart) {
        uint bitmapColSize = pOutAccessor->getTupleDesc()[1].cbStorage;
        uint writerBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
        writerPageLock.allocatePage();
        PBuffer writerBuf = writerPageLock.getPage().getWritableData();
        segmentWriter.init(
            writerBuf, writerBufSize, pOutAccessor->getTupleDesc(), false);
    } else {
        segmentWriter.reset();
    }

    state = LBM_STATE_READ;
    writePending = false;
    producePending = false;
    segmentReader.init(inAccessors[0], inputTuple);
}

ExecStreamResult LbmChopperExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ridLimit = *reinterpret_cast<RecordNum const *>(
        pDynamicParamManager->getParam(ridLimitParamId).getDatum().pData);

    uint nTuples = 0;
    ExecStreamResult status;
    while (nTuples < quantum.nTuplesMax) {
        switch (state) {
        case LBM_STATE_READ:
            status = readSegment();
            if (status == EXECRC_EOS) {
                // flush any remaining data as last tuple(s)
                if (! segmentWriter.isEmpty()) {
                    producePending = true;
                    state = LBM_STATE_PRODUCE;
                    continue;
                }
                state = LBM_STATE_DONE;
                continue;
            }
            if (status != EXECRC_YIELD) {
                return status;
            }
            state = LBM_STATE_WRITE;
            continue;
        case LBM_STATE_WRITE:
            if (! writeSegment()) {
                producePending = true;
                state = LBM_STATE_PRODUCE;
                continue;
            }
            nTuples++;
            state = LBM_STATE_READ;
            continue;
        case LBM_STATE_PRODUCE:
            if (! produceTuple()) {
                return EXECRC_BUF_OVERFLOW;
            }
            state = writePending ? LBM_STATE_WRITE : LBM_STATE_READ;
            continue;
        case LBM_STATE_DONE:
            pOutAccessor->markEOS();
            return EXECRC_EOS;
        default:
            assert(false);
        }
    }
    return EXECRC_QUANTUM_EXPIRED;
}

void LbmChopperExecStream::closeImpl()
{
    ConfluenceExecStream::closeImpl();
}

ExecStreamResult LbmChopperExecStream::readSegment()
{
    if (writePending) {
        return EXECRC_YIELD;
    }
    ExecStreamResult status = segmentReader.readSegmentAndAdvance(
        inputSegment.byteNum, inputSegment.byteSeg, inputSegment.len);
    if (status == EXECRC_YIELD) {
        writePending = true;
    }
    return status;
}

bool LbmChopperExecStream::writeSegment()
{
    assert(writePending = true);
    LcsRid startRid = inputSegment.getSrid();
    LcsRid endRid = inputSegment.getEndRid();
    assert(opaqueToInt(endRid - startRid) <= ridLimit);

    // if appending to previous segments, ensure that the current segment
    // follows the previous segment, and that it would not exceed the
    // rid limit for the tuple being written
    bool firstWrite = segmentWriter.isEmpty();
    if (! firstWrite) {
        if (startRid < currentEndRid) {
            return false;
        }
        if (opaqueToInt(endRid - currentSrid) > ridLimit) {
            return false;
        }
    }

    // try to add segment to writer
    PBuffer byteSeg = inputSegment.byteSeg - (inputSegment.len - 1);
    if (segmentWriter.addSegment(
            startRid,
            byteSeg,
            inputSegment.len))
    {
        writePending = false;
        if (firstWrite) {
            currentSrid = startRid;
        }
        currentEndRid = endRid;
        return true;
    }
    return false;
}

bool LbmChopperExecStream::produceTuple()
{
    assert(producePending);

    TupleData outputTuple = segmentWriter.produceSegmentTuple();
    if (pOutAccessor->produceTuple(outputTuple)) {
        segmentWriter.reset();
        producePending = false;
        return true;
    }
    return false;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmChopperExecStream.cpp

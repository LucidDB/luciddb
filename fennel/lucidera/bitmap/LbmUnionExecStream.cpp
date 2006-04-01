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
#include "fennel/lucidera/bitmap/LbmUnionExecStream.h"

#include <math.h>

FENNEL_BEGIN_CPPFILE("$Id$");

LbmUnionExecStream::LbmUnionExecStream()
{
    dynParamsCreated = false;
}

void LbmUnionExecStream::prepare(LbmUnionExecStreamParams const &params)
{
    ConfluenceExecStream::prepare(params);
    maxRid = params.maxRid;
    
    // set dynanmic parameter ids
    ridLimitParamId = params.ridLimitParamId;
    assert(opaqueToInt(ridLimitParamId) > 0);

    // optional parameters
    startRidParamId = params.startRidParamId;
    segmentLimitParamId = params.segmentLimitParamId;

    // setup tupledatums for writing dynamic parameter values
    ridLimitDatum.pData = (PConstBuffer) &ridLimit;
    ridLimitDatum.cbData = sizeof(ridLimit);

    assert(inAccessors[0]->getTupleDesc() == pOutAccessor->getTupleDesc());

    // initialize reader
    inputTuple.compute(inAccessors[0]->getTupleDesc());

    // output buffer will come from scratch segment
    scratchAccessor = params.scratchAccessor;
    workspacePageLock.accessSegment(scratchAccessor);
    writerPageLock.accessSegment(scratchAccessor);
    pageSize = scratchAccessor.pSegment->getUsablePageSize();
}

void LbmUnionExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConfluenceExecStream::getResourceRequirements(minQuantity, optQuantity);

    // at least 2 scratch pages for constructing output bitmap segments
    //   - 1 for workspace
    //   - 1 for writer
    minQuantity.nCachePages += 2;
    optQuantity.nCachePages += computeOptWorkspacePages(maxRid) + 1;

    // cheat for now, until we get the chopper going
    minQuantity = optQuantity;
}

void LbmUnionExecStream::setResourceAllocation(
    ExecStreamResourceQuantity &quantity)
{
    ConfluenceExecStream::setResourceAllocation(quantity);

    // TODO: can we just grab all the remaining pages like this?
    nWorkspacePages = quantity.nCachePages - 1;
    ridLimit = computeRidLimit(nWorkspacePages);
}

void LbmUnionExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);

    if (!restart) {
        uint bitmapColSize = pOutAccessor->getTupleDesc()[1].cbStorage;
        uint writerBufSize = LbmEntry::getScratchBufferSize(bitmapColSize);
        writerPageLock.allocatePage();
        PBuffer writerBuf = writerPageLock.getPage().getWritableData();
        segmentWriter.init(
            writerBuf, writerBufSize, pOutAccessor->getTupleDesc(), false);
        // still have plenty of space for merging
        reverseArea = writerBuf + writerBufSize;
        reverseAreaSize =
            scratchAccessor.pSegment->getUsablePageSize() - writerBufSize;
        
        // allocate byte buffer for merging segments
        boost::shared_array<PBuffer> ppBuffers(new PBuffer[nWorkspacePages]);
        assert(ppBuffers != NULL);
        for (uint i = 0; i < nWorkspacePages; i++) {
            workspacePageLock.allocatePage();
            ppBuffers[i] = workspacePageLock.getPage().getWritableData();
            workspacePageLock.unlock();
        }
        ByteBuffer *pBuffer = new ByteBuffer();
        pBuffer->init(ppBuffers, nWorkspacePages, pageSize);
        SharedByteBuffer pWorkspaceBuffer(pBuffer);
        uint maxSegmentSize = LbmEntry::getMaxBitmapSize(bitmapColSize);
        workspace.init(pWorkspaceBuffer, maxSegmentSize);

        // create dynamic parameters
        pDynamicParamManager->createParam(
            ridLimitParamId, pOutAccessor->getTupleDesc()[0]);
        dynParamsCreated = true;
        pDynamicParamManager->writeParam(ridLimitParamId, ridLimitDatum);
    } else {
        workspace.reset();
        segmentWriter.reset();
    }

    writePending = false;
    producePending = false;
    isDone = false;
    segmentReader.init(inAccessors[0], inputTuple);
}

ExecStreamResult LbmUnionExecStream::execute(
    ExecStreamQuantum const &quantum)
{
     if (isDone) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
     }

     if (isConsumerSridSet()) {
         // avoid RIDs not required by the downstream consumer
         requestedSrid = (LcsRid) *reinterpret_cast<RecordNum const *>(
             pDynamicParamManager->getParam(startRidParamId).getDatum().pData);
         workspace.advanceToSrid(requestedSrid);
     }
     if (isSegmentLimitSet()) {
         segmentsRemaining = *reinterpret_cast<uint const *>(
             pDynamicParamManager->getParam(segmentLimitParamId)
             .getDatum().pData);
     }

     for (uint i = 0; i < quantum.nTuplesMax; i++) {
        while (! producePending) {
            // yield control if segment limit is reached
            if (isSegmentLimitSet() && segmentsRemaining == 0) {
                return EXECRC_QUANTUM_EXPIRED;
            }

            ExecStreamResult status = readSegment();
            if (status == EXECRC_EOS) {
                // flush any remaining data as last tuple(s)
                isDone = workspace.isEmpty() && segmentWriter.isEmpty();
                if (! isDone) {
                    transferLast();
                    producePending = true;
                    break;
                }
                return EXECRC_EOS;
            }
            if (status != EXECRC_YIELD) {
                return status;
            }
            if (! writeSegment()) {
                producePending = (! segmentWriter.isEmpty());
            }
        }

        if (! produceTuple()) {
            return EXECRC_BUF_OVERFLOW;
        }
        producePending = false;
    }
    return EXECRC_QUANTUM_EXPIRED;
}

void LbmUnionExecStream::closeImpl()
{
    ConfluenceExecStream::closeImpl();
    if (dynParamsCreated) {
        pDynamicParamManager->deleteParam(ridLimitParamId);
    }

    // FIXME: deallocate pages
}

uint LbmUnionExecStream::computeOptWorkspacePages(LcsRid maxRid) 
{
    // TODO: come up with a better estimate once we have statistics
    return 2;
}

uint LbmUnionExecStream::computeRidLimit(uint nWorkspacePages)
{
    // save a quarter page for building segments
    // based upon the idea that the largest segment could be
    // 1/8 of a page along with 1/8 of a page for "growing" a
    // segment before writing it out (not true as of 2006-03-08)
    uint bytes = (uint) ((nWorkspacePages - 0.25) * pageSize);
    return bytes * LbmSegment::LbmOneByteSize;
}

bool LbmUnionExecStream::isConsumerSridSet()
{
    return (opaqueToInt(startRidParamId) > 0);
}

bool LbmUnionExecStream::isSegmentLimitSet()
{
    return (opaqueToInt(segmentLimitParamId) > 0);
}

ExecStreamResult LbmUnionExecStream::readSegment()
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

bool LbmUnionExecStream::writeSegment()
{
    assert(writePending = true);

    // eagerly flush segments
    LcsRid currentSrid = segmentReader.getSrid();
    workspace.setProductionLimit(currentSrid);
    if (!transfer()) {
        return false;
    }
    if (workspace.isEmpty()) {
        workspace.advanceToSrid(currentSrid);
    }

    // flushing the workspace should make enough room for the next tuple
    assert(workspace.addSegment(inputSegment));
    writePending = false;
    return true;
}

void LbmUnionExecStream::transferLast()
{
    workspace.removeLimit();
    transfer();
}

bool LbmUnionExecStream::transfer()
{
    while (workspace.canProduce()) {
        if (isSegmentLimitSet() && segmentsRemaining == 0) {
            return false;
        }

        LbmByteSegment seg = workspace.getSegment();
        assert(seg.len < reverseAreaSize);
        PBuffer reverseStart = reverseArea + seg.len - 1;
        for (uint i = 0; i < seg.len; i++) {
            reverseStart[-i] = seg.byteSeg[i];
        }
        LcsRid startRid = seg.getSrid();
        if (! segmentWriter.addSegment(startRid, reverseArea, seg.len)) {
            return false;
        }
        workspace.advancePastSegment();

        if (isSegmentLimitSet()) {
            segmentsRemaining--;
        }
    }
    return true;
}

bool LbmUnionExecStream::produceTuple()
{
    assert(producePending);
    assert(! segmentWriter.isEmpty());

    outputTuple = segmentWriter.produceSegmentTuple();
    if (pOutAccessor->produceTuple(outputTuple)) {
        segmentWriter.reset();
        producePending = false;
        return true;
    }
    return false;
}

FENNEL_END_CPPFILE("$Id$");

// End LbmUnionExecStream.cpp

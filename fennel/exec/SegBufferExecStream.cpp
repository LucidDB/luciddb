/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SegBufferExecStream::prepare(SegBufferExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;

    multipass = params.multipass;
    firstPageId = NULL_PAGE_ID;
}

void SegBufferExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for I/O
    minQuantity.nCachePages += 1;
    
    optQuantity = minQuantity;
}

void SegBufferExecStream::open(bool restart)
{
    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);
    
    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    if (restart) {
        pOutAccessor->clear();
        if (multipass) {
            if (pByteInputStream) {
                // reread from beginning
                openBufferForRead(false);
            } else {
                // nothing was ever buffered, so treat this the
                // same as first open
                ConduitExecStream::open(restart);
            }
        } else {
            // for a single-pass buffer, a restart means forget any buffered
            // contents
            destroyBuffer();
            ConduitExecStream::open(restart);
        }
    } else {
        ConduitExecStream::open(restart);
    }
}

void SegBufferExecStream::closeImpl()
{
    destroyBuffer();
    ConduitExecStream::closeImpl();
}

void SegBufferExecStream::destroyBuffer()
{
    if (pByteOutputStream || (multipass && (firstPageId != NULL_PAGE_ID))) {
        // this is to make sure that buffer storage gets deallocated in all
        // cases
        openBufferForRead(true);
    }
    pByteInputStream.reset();
    firstPageId = NULL_PAGE_ID;
}

void SegBufferExecStream::openBufferForRead(bool destroy)
{
    cbLastRead = 0;
    if (firstPageId == NULL_PAGE_ID) {
        firstPageId = pByteOutputStream->getFirstPageId();
        pByteOutputStream.reset();
        pByteInputStream = SegInputStream::newSegInputStream(
            bufferSegmentAccessor,firstPageId);
        pByteInputStream->getSegPos(restartPos);
    } else {
        pByteInputStream->seekSegPos(restartPos);
    }
    
    if (destroy) {
        pByteInputStream->setDeallocate(true);
    }
}

ExecStreamResult SegBufferExecStream::execute(ExecStreamQuantum const &)
{
    if (!pByteInputStream) {
        if (!pByteOutputStream) {
            pByteOutputStream = SegOutputStream::newSegOutputStream(
                bufferSegmentAccessor);
        }
        switch(pInAccessor->getState()) {
        case EXECBUF_NONEMPTY:
        case EXECBUF_OVERFLOW:
            pByteOutputStream->consumeWritePointer(
                pInAccessor->getConsumptionAvailable());
            pByteOutputStream->hardPageBreak();
            pInAccessor->consumeData(pInAccessor->getConsumptionEnd());
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_UNDERFLOW:
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EMPTY:
            {
                uint cb;
                PBuffer pBuffer = pByteOutputStream->getWritePointer(1,&cb);
                pInAccessor->provideBufferForProduction(
                    pBuffer,
                    pBuffer + cb,
                    false);
            }
            return EXECRC_BUF_UNDERFLOW;
        case EXECBUF_EOS:
            closeProducers(getStreamId());
            openBufferForRead(!multipass);
            break;
        default:
            permAssert(false);
        }
    }
    
    switch(pOutAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
    case EXECBUF_EMPTY:
        break;
    case EXECBUF_EOS:
        return EXECRC_EOS;
    default:
        permAssert(false);
    }
    pByteInputStream->consumeReadPointer(cbLastRead);
    PConstBuffer pBuffer = pByteInputStream->getReadPointer(1,&cbLastRead);
    if (!pBuffer) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }
    pOutAccessor->provideBufferForConsumption(
        pBuffer,
        pBuffer + cbLastRead);
    return EXECRC_BUF_OVERFLOW;
}

void SegBufferExecStream::closeProducers(ExecStreamId streamId)
{
    ExecStreamGraphImpl &graphImpl =
        dynamic_cast<ExecStreamGraphImpl&>(getGraph());
    ExecStreamGraphImpl::GraphRep graphRep = graphImpl.getGraphRep();
    ExecStreamGraphImpl::InEdgeIterPair inEdges =
        boost::in_edges(streamId, graphRep);
    for (; inEdges.first != inEdges.second; ++(inEdges.first)) {
        ExecStreamGraphImpl::Edge edge = *(inEdges.first);
        // move streamId upstream
        streamId = boost::source(edge,graphRep);
        // close the producers of this stream before closing the stream
        // itself
        closeProducers(streamId);
        SharedExecStream pStream = graphImpl.getStreamFromVertex(streamId);
        pStream->close();
    }
}

ExecStreamBufProvision SegBufferExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision SegBufferExecStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End SegBufferExecStream.cpp

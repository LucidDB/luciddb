/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
// 
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
// 
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/SegBufferStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SegBufferStream::prepare(SegBufferStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;

    multipass = params.multipass;
    firstPageId = NULL_PAGE_ID;
}

void SegBufferStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for I/O
    minQuantity.nCachePages += 1;
    
    optQuantity = minQuantity;
}

void SegBufferStream::open(bool restart)
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

void SegBufferStream::closeImpl()
{
    destroyBuffer();
    ConduitExecStream::closeImpl();
}

void SegBufferStream::destroyBuffer()
{
    if (pByteOutputStream || (multipass && (firstPageId != NULL_PAGE_ID))) {
        // this is to make sure that buffer storage gets deallocated in all
        // cases
        openBufferForRead(true);
    }
    pByteInputStream.reset();
    firstPageId = NULL_PAGE_ID;
}

void SegBufferStream::openBufferForRead(bool destroy)
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

ExecStreamResult SegBufferStream::execute(ExecStreamQuantum const &)
{
    if (!pByteInputStream) {
        if (!pByteOutputStream) {
            pByteOutputStream = SegOutputStream::newSegOutputStream(
                bufferSegmentAccessor);
        }
        switch(pInAccessor->getState()) {
        case EXECBUF_NEED_CONSUMPTION:
            pByteOutputStream->consumeWritePointer(
                pInAccessor->getConsumptionAvailable());
            pByteOutputStream->hardPageBreak();
            pInAccessor->consumeData(pInAccessor->getConsumptionEnd());
            return EXECRC_NO_OUTPUT;
        case EXECBUF_NEED_PRODUCTION:
            return EXECRC_NEED_INPUT;
        case EXECBUF_IDLE:
            {
                uint cb;
                PBuffer pBuffer = pByteOutputStream->getWritePointer(1,&cb);
                pInAccessor->provideBufferForProduction(
                    pBuffer,
                    pBuffer + cb,
                    false);
            }
            return EXECRC_NEED_INPUT;
        case EXECBUF_EOS:
            openBufferForRead(!multipass);
            break;
        default:
            permAssert(false);
        }
    }
    switch(pOutAccessor->getState()) {
    case EXECBUF_NEED_CONSUMPTION:
        return EXECRC_NEED_OUTPUTBUF;
    case EXECBUF_NEED_PRODUCTION:
    case EXECBUF_IDLE:
        break;
    case EXECBUF_EOS:
        return EXECRC_EOS;
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
    return EXECRC_OUTPUT;
}

ExecStreamBufProvision SegBufferStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision SegBufferStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End SegBufferStream.cpp

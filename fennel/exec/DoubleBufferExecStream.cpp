/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 2004-2007 John V. Sichi
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
#include "fennel/exec/DoubleBufferExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void DoubleBufferExecStream::prepare(
    DoubleBufferExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    scratchAccessor = params.scratchAccessor;
    bufferLock1.accessSegment(scratchAccessor);
    bufferLock2.accessSegment(scratchAccessor);
}

void DoubleBufferExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);

    // two scratch pages
    minQuantity.nCachePages += 2;

    optQuantity = minQuantity;
}

void DoubleBufferExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);

    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    if (!bufferLock1.isLocked()) {
        bufferLock1.allocatePage();
    }

    if (!bufferLock2.isLocked()) {
        bufferLock2.allocatePage();
    }

    // until the first swap, only the back buffer is active
    pFrontBuffer = NULL;
    pBackBuffer = bufferLock1.getPage().getWritableData();

    pInAccessor->provideBufferForProduction(
        pBackBuffer,
        pBackBuffer + bufferLock1.getPage().getCache().getPageSize(),
        false);
}

ExecStreamResult DoubleBufferExecStream::execute(ExecStreamQuantum const &)
{
    if (pFrontBuffer) {
        // both front and back buffers are active
        switch (pOutAccessor->getState()) {
        case EXECBUF_NONEMPTY:
        case EXECBUF_OVERFLOW:
            // consumer isn't done with front buffer, so we can't swap yet
            return EXECRC_BUF_OVERFLOW;
        case EXECBUF_UNDERFLOW:
        case EXECBUF_EMPTY:
            // consumer has finished reading, so fall through to check
            // on producer
            break;
        case EXECBUF_EOS:
            assert(pInAccessor->getState() == EXECBUF_EOS);
            return EXECRC_EOS;
        }
    }
    switch (pInAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        // producer has given us data, so fall through to swap
        break;
    case EXECBUF_UNDERFLOW:
        // producer hasn't written yet
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EMPTY:
        pInAccessor->requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        if (pOutAccessor->getState() == EXECBUF_EOS) {
            return EXECRC_EOS;
        } else {
            // done with back buffer now, but we're not really EOS until
            // front buffer is consumed too
            pBackBuffer = NULL;
            pOutAccessor->markEOS();
            return EXECRC_BUF_OVERFLOW;
        }
    default:
        permAssert(false);
    }

    if (!pFrontBuffer) {
        // from here on both front buffer and back buffer are active
        pFrontBuffer = bufferLock2.getPage().getWritableData();
    }

    std::swap(pFrontBuffer, pBackBuffer);
    pOutAccessor->provideBufferForConsumption(
        pFrontBuffer,
        pInAccessor->getConsumptionEnd());
    pInAccessor->consumeData(pInAccessor->getConsumptionEnd());
    if (pInAccessor->getState() == EXECBUF_EOS) {
        pBackBuffer = NULL;
        pOutAccessor->markEOS();
        return EXECRC_BUF_OVERFLOW;
    }
    pInAccessor->provideBufferForProduction(
        pBackBuffer,
        pBackBuffer + bufferLock1.getPage().getCache().getPageSize(),
        false);
    return EXECRC_BUF_UNDERFLOW;
}

void DoubleBufferExecStream::closeImpl()
{
    pFrontBuffer = NULL;
    pBackBuffer = NULL;
    bufferLock1.unlock();
    bufferLock2.unlock();
    ConduitExecStream::closeImpl();
}

ExecStreamBufProvision DoubleBufferExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision DoubleBufferExecStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End DoubleBufferExecStream.cpp

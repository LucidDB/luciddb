/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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
#include "fennel/exec/ScratchBufferExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ScratchBufferExecStream::prepare(
    ScratchBufferExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

void ScratchBufferExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // one scratch page
    minQuantity.nCachePages += 1;

    optQuantity = minQuantity;
}

void ScratchBufferExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);

    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    if (!bufferLock.isLocked()) {
        bufferLock.allocatePage();
    }

    pInAccessor->provideBufferForProduction(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getWritableData()
        + bufferLock.getPage().getCache().getPageSize(),
        true);

    pLastConsumptionEnd = NULL;
}

ExecStreamResult ScratchBufferExecStream::execute(ExecStreamQuantum const &)
{
    switch (pOutAccessor->getState()) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
    case EXECBUF_EMPTY:
        if (pLastConsumptionEnd) {
            // Since our output buf is empty, the downstream consumer
            // must have consumed everything up to the last byte we
            // told it was available; pass that information on to our
            // upstream producer.
            pInAccessor->consumeData(pLastConsumptionEnd);
            pLastConsumptionEnd = NULL;
        }
        break;
    case EXECBUF_EOS:
        assert(pInAccessor->getState() == EXECBUF_EOS);
        return EXECRC_EOS;
    }
    switch (pInAccessor->getState()) {
    case EXECBUF_OVERFLOW:
    case EXECBUF_NONEMPTY:
        if (!pLastConsumptionEnd) {
            pLastConsumptionEnd = pInAccessor->getConsumptionEnd();
            pOutAccessor->provideBufferForConsumption(
                pInAccessor->getConsumptionStart(),
                pLastConsumptionEnd);
        }
        return EXECRC_BUF_OVERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EMPTY:
        pInAccessor->requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    default:
        permAssert(false);
    }
}

void ScratchBufferExecStream::closeImpl()
{
    bufferLock.unlock();
    ConduitExecStream::closeImpl();
}

ExecStreamBufProvision ScratchBufferExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision ScratchBufferExecStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End ScratchBufferExecStream.cpp

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
#include "fennel/exec/ScratchBufferStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ScratchBufferStream::prepare(ExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

void ScratchBufferStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity,optQuantity);

    // one scratch page
    minQuantity.nCachePages += 1;

    optQuantity = minQuantity;
}

void ScratchBufferStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);
    
    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    if (restart) {
        pOutAccessor->clear();
        pInAccessor->clear();
    } else {
        bufferLock.allocatePage();
    }

    pInAccessor->provideBufferForProduction(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getWritableData()
        + bufferLock.getPage().getCache().getPageSize(),
        true);
}

ExecStreamResult ScratchBufferStream::execute(ExecStreamQuantum const &)
{
    switch(pOutAccessor->getState()) {
    case EXECBUF_NEED_CONSUMPTION:
        return EXECRC_NEED_OUTPUTBUF;
    case EXECBUF_NEED_PRODUCTION:
    case EXECBUF_IDLE:
        break;
    case EXECBUF_EOS:
        assert(pInAccessor->getState() == EXECBUF_EOS);
        return EXECRC_EOS;
    }
    switch(pInAccessor->getState()) {
    case EXECBUF_NEED_CONSUMPTION:
        pOutAccessor->provideBufferForConsumption(
            pInAccessor->getConsumptionStart(),
            pInAccessor->getConsumptionEnd());
        // REVIEW jvs 27-Oct-2004:  This will break if we implement
        // double-buffering in ExecStreamBufAccessor.  It's not safe
        // to consume the data until our own consumer is done with it.
        pInAccessor->consumeData(pInAccessor->getConsumptionEnd());
        return EXECRC_OUTPUT;
    case EXECBUF_NEED_PRODUCTION:
        return EXECRC_NEED_INPUT;
    case EXECBUF_IDLE:
        pInAccessor->requestProduction();
        return EXECRC_NEED_INPUT;
    case EXECBUF_EOS:
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    default:
        permAssert(false);
    }
}

void ScratchBufferStream::closeImpl()
{
    bufferLock.unlock();
    ConduitExecStream::closeImpl();
}

ExecStreamBufProvision ScratchBufferStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

ExecStreamBufProvision ScratchBufferStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End ScratchBufferStream.cpp

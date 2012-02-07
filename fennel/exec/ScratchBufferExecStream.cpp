/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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

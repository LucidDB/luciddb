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
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

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

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
#include "fennel/exec/SegBufferWriterExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/exec/SegBufferWriter.h"
#include "fennel/tuple/StandardTypeDescriptor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SegBufferWriterExecStream::prepare(
    SegBufferWriterExecStreamParams const &params)
{
    DiffluenceExecStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;
    readerRefCountParamId = params.readerRefCountParamId;
    paramCreated = false;

    for (uint i = 0; i < outAccessors.size(); i++) {
        assert(outAccessors[i]->getTupleDesc().size() == 1);
    }
    outputTuple.compute(outAccessors[0]->getTupleDesc());
    outputTuple[0].pData = (PConstBuffer) &firstBufferPageId;
    outputBufSize =
        outAccessors[0]->getScratchTupleAccessor().getMaxByteCount();
    outputWritten.resize(outAccessors.size());
}

void SegBufferWriterExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    DiffluenceExecStream::getResourceRequirements(minQuantity, optQuantity);

    // set aside 1 page for I/O
    minQuantity.nCachePages += 1;
    optQuantity = minQuantity;
}

void SegBufferWriterExecStream::open(bool restart)
{
    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);

    std::fill(outputWritten.begin(), outputWritten.end(), false);
    nOutputsWritten = 0;

    if (!restart) {
        StandardTypeDescriptorFactory stdTypeFactory;
        TupleAttributeDescriptor attrDesc =
            TupleAttributeDescriptor(
                stdTypeFactory.newDataType(STANDARD_TYPE_UINT_64));
        pDynamicParamManager->createCounterParam(readerRefCountParamId);
        paramCreated = true;
        outputTupleBuffer.reset(new FixedBuffer[outputBufSize]);
        firstBufferPageId = NULL_PAGE_ID;
    }
    DiffluenceExecStream::open(restart);
}

void SegBufferWriterExecStream::closeImpl()
{
    assert(readReaderRefCount() == 0);
    paramCreated = false;
    pSegBufferWriter.reset();
    outputTupleBuffer.reset();
    DiffluenceExecStream::closeImpl();
}

bool SegBufferWriterExecStream::canEarlyClose()
{
    return (readReaderRefCount() == 0);
}

int64_t SegBufferWriterExecStream::readReaderRefCount()
{
    if (!paramCreated) {
        // If the stream was never opened, then the parameter will not have
        // been created.
        return 0;
    }

    int64_t refCount;
    TupleDatum refCountDatum;
    refCountDatum.pData = (PConstBuffer) &refCount;
    refCountDatum.cbData = 8;
    pDynamicParamManager->readParam(readerRefCountParamId, refCountDatum);
    return refCount;
}

ExecStreamResult SegBufferWriterExecStream::execute(ExecStreamQuantum const &)
{
    if (nOutputsWritten == outAccessors.size()) {
        for (uint i = 0; i < outAccessors.size(); i++) {
            outAccessors[i]->markEOS();
        }
        return EXECRC_EOS;
    }

    // Buffer the input
    if (firstBufferPageId == NULL_PAGE_ID) {
        if (!pSegBufferWriter) {
            pSegBufferWriter =
                SegBufferWriter::newSegBufferWriter(
                    pInAccessor,
                    bufferSegmentAccessor,
                    true);
        }
        ExecStreamResult rc = pSegBufferWriter->write();
        if (rc != EXECRC_EOS) {
            return rc;
        }
        // Close the upstream producers
        ExecStreamGraphImpl &graphImpl =
            dynamic_cast<ExecStreamGraphImpl&>(getGraph());
        graphImpl.closeProducers(getStreamId());
        firstBufferPageId = pSegBufferWriter->getFirstPageId();
    }

    // Once the input has been buffered, then pass along the first buffer
    // pageId to only those consumers that have explicitly requested data
    bool newOutput = false;
    for (uint i = 0; i < outAccessors.size(); i++) {
        switch (outAccessors[i]->getState()) {
        case EXECBUF_NONEMPTY:
        case EXECBUF_OVERFLOW:
        case EXECBUF_EMPTY:
        case EXECBUF_EOS:
            break;

        case EXECBUF_UNDERFLOW:
            // Underflow means the consumer has explicitly requested data
            {
                assert(!outputWritten[i]);
                TupleAccessor *outputTupleAccessor =
                    &outAccessors[i]->getScratchTupleAccessor();
                outputTupleAccessor->marshal(
                    outputTuple,
                    outputTupleBuffer.get());
                outAccessors[i]->provideBufferForConsumption(
                    outputTupleBuffer.get(),
                    outputTupleBuffer.get() + outputBufSize);
                outputWritten[i] = true;
                nOutputsWritten++;
                newOutput = true;
                break;
            }

        default:
            permAssert(false);
        }
    }

    // Verify that at least one output stream was written
    assert(newOutput);
    return EXECRC_BUF_OVERFLOW;
}

ExecStreamBufProvision SegBufferWriterExecStream::getInputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

ExecStreamBufProvision SegBufferWriterExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End SegBufferWriterExecStream.cpp

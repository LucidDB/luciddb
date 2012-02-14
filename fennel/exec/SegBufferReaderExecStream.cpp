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
#include "fennel/exec/SegBufferReaderExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/DynamicParam.h"
#include "fennel/exec/SegBufferReader.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SegBufferReaderExecStream::prepare(
    SegBufferReaderExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;
    readerRefCountParamId = params.readerRefCountParamId;
    paramIncremented = false;

    assert(pInAccessor->getTupleDesc().size() == 1);
    inputTuple.compute(pInAccessor->getTupleDesc());
}

void SegBufferReaderExecStream::getResourceRequirements(
    ExecStreamResourceQuantity &minQuantity,
    ExecStreamResourceQuantity &optQuantity)
{
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // set aside 1 page for I/O
    minQuantity.nCachePages += 1;
    optQuantity = minQuantity;
}

void SegBufferReaderExecStream::open(bool restart)
{
    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    if (!restart) {
        firstBufferPageId = NULL_PAGE_ID;
        pDynamicParamManager->incrementCounterParam(readerRefCountParamId);
        paramIncremented = true;
        ConduitExecStream::open(restart);
    } else {
        pOutAccessor->clear();
        if (pSegBufferReader) {
            // reread from beginning
            pSegBufferReader->open(false);
        } else {
            // the buffered data hasn't been read yet, so treat this the
            // same as first open
            ConduitExecStream::open(restart);
        }
    }
}

void SegBufferReaderExecStream::closeImpl()
{
    // If this stream was never opened, then it's possible that the parameter
    // does not exist.  So, only decrement if we know we've done an increment.
    if (paramIncremented) {
        pDynamicParamManager->decrementCounterParam(readerRefCountParamId);
        paramIncremented = false;
    }
    pSegBufferReader.reset();
    ConduitExecStream::closeImpl();
}

ExecStreamResult SegBufferReaderExecStream::execute(ExecStreamQuantum const &)
{
    // Retrieve the first pageId of the buffered data from the writer stream,
    // if it hasn't already been retrieved, and then setup a buffer reader.
    if (firstBufferPageId == NULL_PAGE_ID) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }
        pInAccessor->unmarshalTuple(inputTuple);
        firstBufferPageId =
            *reinterpret_cast<PageId const *> (inputTuple[0].pData);
        pInAccessor->consumeTuple();
        pSegBufferReader =
            SegBufferReader::newSegBufferReader(
                pOutAccessor,
                bufferSegmentAccessor,
                firstBufferPageId);
        pSegBufferReader->open(false);
    }

    // Read the buffered data
    return pSegBufferReader->read();
}

ExecStreamBufProvision SegBufferReaderExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$")

// End SegBufferReaderExecStream.cpp

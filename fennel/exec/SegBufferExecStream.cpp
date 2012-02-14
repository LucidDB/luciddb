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
#include "fennel/exec/SegBufferExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraphImpl.h"
#include "fennel/exec/SegBufferReader.h"
#include "fennel/exec/SegBufferWriter.h"

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
    ConduitExecStream::getResourceRequirements(minQuantity, optQuantity);

    // set aside 1 page for I/O
    minQuantity.nCachePages += 1;

    optQuantity = minQuantity;
}

void SegBufferExecStream::open(bool restart)
{
    assert(pInAccessor);
    assert(pInAccessor->getProvision() == BUFPROV_CONSUMER);

    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == BUFPROV_PRODUCER);

    // TODO jvs 1-June-2006:  generalize SegStreamAllocation to handle
    // the multipass usage requirements here

    if (restart) {
        pOutAccessor->clear();
        if (multipass) {
            if (pSegBufferReader) {
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
    if (pSegBufferWriter || (multipass && (firstPageId != NULL_PAGE_ID))) {
        // this is to make sure that buffer storage gets deallocated in all
        // cases
        openBufferForRead(true);
    }
    pSegBufferReader.reset();
    firstPageId = NULL_PAGE_ID;
}

void SegBufferExecStream::openBufferForRead(bool destroy)
{
    if (firstPageId == NULL_PAGE_ID) {
        firstPageId = pSegBufferWriter->getFirstPageId();
        pSegBufferWriter.reset();
        pSegBufferReader =
            SegBufferReader::newSegBufferReader(
                pOutAccessor,
                bufferSegmentAccessor,
                firstPageId);
    }
    pSegBufferReader->open(destroy);
}

ExecStreamResult SegBufferExecStream::execute(ExecStreamQuantum const &)
{
    if (!pSegBufferReader) {
        if (!pSegBufferWriter) {
            pSegBufferWriter =
                SegBufferWriter::newSegBufferWriter(
                    pInAccessor,
                    bufferSegmentAccessor,
                    false);
        }
        ExecStreamResult rc = pSegBufferWriter->write();
        if (rc != EXECRC_EOS) {
            return rc;
        }

        ExecStreamGraphImpl &graphImpl =
            dynamic_cast<ExecStreamGraphImpl&>(getGraph());
        graphImpl.closeProducers(getStreamId());
        openBufferForRead(!multipass);
    }

    return pSegBufferReader->read();
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

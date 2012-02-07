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
#include "fennel/exec/SegBufferReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/segment/SegInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegBufferReader SegBufferReader::newSegBufferReader(
    SharedExecStreamBufAccessor &pOutAccessor,
    SegmentAccessor const &bufferSegmentAccessor,
    PageId firstPageId)
{
    return SharedSegBufferReader(
        new SegBufferReader(pOutAccessor, bufferSegmentAccessor, firstPageId),
        ClosableObjectDestructor());
}

SegBufferReader::SegBufferReader(
    SharedExecStreamBufAccessor &pOutAccessorInit,
    SegmentAccessor const &bufferSegmentAccessorInit,
    PageId firstPageIdInit)
    : pOutAccessor(pOutAccessorInit),
        bufferSegmentAccessor(bufferSegmentAccessorInit),
        firstPageId(firstPageIdInit)
{
}

void SegBufferReader::open(bool destroy)
{
    cbLastRead = 0;
    // If previously opened, restart from the beginning
    if (pByteInputStream) {
        pByteInputStream->endPrefetch();
        pByteInputStream->seekSegPos(restartPos);
    } else {
        pByteInputStream =
            SegInputStream::newSegInputStream(
                bufferSegmentAccessor,
                firstPageId);
        pByteInputStream->getSegPos(restartPos);
    }
    if (destroy) {
        pByteInputStream->setDeallocate(true);
    }
    pByteInputStream->startPrefetch();
}

ExecStreamResult SegBufferReader::read()
{
    switch (pOutAccessor->getState()) {
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

void SegBufferReader::closeImpl()
{
    pByteInputStream.reset();
}

FENNEL_END_CPPFILE("$Id$");

// End SegBufferReader.cpp

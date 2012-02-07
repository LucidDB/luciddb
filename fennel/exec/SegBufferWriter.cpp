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
#include "fennel/exec/SegBufferWriter.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/segment/SegInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedSegBufferWriter SegBufferWriter::newSegBufferWriter(
    SharedExecStreamBufAccessor &pInAccessor,
    SegmentAccessor const &bufferSegmentAccessor,
    bool destroyOnClose)
{
    return SharedSegBufferWriter(
        new SegBufferWriter(pInAccessor, bufferSegmentAccessor, destroyOnClose),
        ClosableObjectDestructor());
}

SegBufferWriter::SegBufferWriter(
    SharedExecStreamBufAccessor &pInAccessorInit,
    SegmentAccessor const &bufferSegmentAccessorInit,
    bool destroyOnCloseInit)
    : pInAccessor(pInAccessorInit),
        bufferSegmentAccessor(bufferSegmentAccessorInit),
        destroyOnClose(destroyOnCloseInit)
{
    firstPageId = NULL_PAGE_ID;
}

ExecStreamResult SegBufferWriter::write()
{
    if (!pByteOutputStream) {
        pByteOutputStream =
            SegOutputStream::newSegOutputStream(bufferSegmentAccessor);
        firstPageId = pByteOutputStream->getFirstPageId();
    }

    ExecStreamBufState inState = pInAccessor->getState();
    switch (inState) {
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        pByteOutputStream->consumeWritePointer(
            pInAccessor->getConsumptionAvailable());
        pByteOutputStream->hardPageBreak();
        pInAccessor->consumeData(pInAccessor->getConsumptionEnd());
        if (pInAccessor->getState() == EXECBUF_EOS) {
            return EXECRC_BUF_UNDERFLOW;
        }
        // else fall through intentionally
    case EXECBUF_EMPTY:
        {
            uint cb;
            PBuffer pBuffer = pByteOutputStream->getWritePointer(1,&cb);
            pInAccessor->provideBufferForProduction(
                pBuffer,
                pBuffer + cb,
                false);
        }
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        pByteOutputStream.reset();
        return EXECRC_EOS;
    default:
        permAssert(false);
    }
}

PageId SegBufferWriter::getFirstPageId()
{
    return firstPageId;
}

void SegBufferWriter::closeImpl()
{
    pByteOutputStream.reset();
    if (destroyOnClose) {
        SharedSegInputStream pByteInputStream =
            SegInputStream::newSegInputStream(
                bufferSegmentAccessor,
                firstPageId);
        pByteInputStream->setDeallocate(true);
        pByteInputStream.reset();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End SegBufferWriter.cpp

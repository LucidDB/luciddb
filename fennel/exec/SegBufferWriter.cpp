/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

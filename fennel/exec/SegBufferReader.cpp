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

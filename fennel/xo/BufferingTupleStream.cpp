/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/xo/BufferingTupleStream.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BufferingTupleStream::prepare(BufferingTupleStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);
    bufferSegmentAccessor = params.scratchAccessor;

    // need to freeze a copy of this since we're going to close pInputStream
    // early
    tupleDesc = pInputStream->getOutputDesc();

    multipass = params.multipass;
    firstPageId = NULL_PAGE_ID;
}

void BufferingTupleStream::getResourceRequirements(
    ExecutionStreamResourceQuantity &minQuantity,
    ExecutionStreamResourceQuantity &optQuantity)
{
    ExecutionStream::getResourceRequirements(minQuantity,optQuantity);

    // one page for I/O
    minQuantity.nCachePages += 1;
    
    optQuantity = minQuantity;
}

void BufferingTupleStream::open(bool restart)
{
    if (restart) {
        if (multipass) {
            if (pByteInputStream) {
                // reread from beginning
                openBufferForRead(false);
            } else {
                // nothing was ever buffered, so treat this the
                // same as first open
                SingleInputTupleStream::open(restart);
            }
        } else {
            // for a single-pass buffer, a restart means forget any buffered
            // contents
            destroyBuffer();
            SingleInputTupleStream::open(restart);
        }
    } else {
        SingleInputTupleStream::open(restart);
    }
}

void BufferingTupleStream::closeImpl()
{
    destroyBuffer();
    SingleInputTupleStream::closeImpl();
}

void BufferingTupleStream::destroyBuffer()
{
    if (pByteOutputStream || (multipass && (firstPageId != NULL_PAGE_ID))) {
        // this is to make sure that buffer storage gets deallocated in all
        // cases
        openBufferForRead(true);
    }
    pByteInputStream.reset();
    firstPageId = NULL_PAGE_ID;
}

TupleDescriptor const &BufferingTupleStream::getOutputDesc() const
{
    return tupleDesc;
}

void BufferingTupleStream::openBufferForRead(bool destroy)
{
    if (firstPageId == NULL_PAGE_ID) {
        firstPageId = pByteOutputStream->getFirstPageId();
        pByteOutputStream.reset();
        pInputStream->close();
        pByteInputStream = SegInputStream::newSegInputStream(
            bufferSegmentAccessor,firstPageId);
        pByteInputStream->getSegPos(restartPos);
    } else {
        pByteInputStream->seekSegPos(restartPos);
    }
    
    if (destroy) {
        pByteInputStream->setDeallocate(true);
    }
}

ByteInputStream &BufferingTupleStream::getProducerResultStream()
{
    if (!pByteInputStream) {
        pByteOutputStream = SegOutputStream::newSegOutputStream(
            bufferSegmentAccessor);
        while (pInputStream->writeResultToConsumerBuffer(*pByteOutputStream)) {
            // nothing to do
        }
        openBufferForRead(!multipass);
    }
    return *pByteInputStream;
}

TupleStream::BufferProvision
BufferingTupleStream::getResultBufferProvision() const
{
    return PRODUCER_PROVISION;
}

TupleStream::BufferProvision
BufferingTupleStream::getInputBufferRequirement() const
{
    return CONSUMER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End BufferingTupleStream.cpp

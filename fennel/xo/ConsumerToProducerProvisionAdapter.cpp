/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/xo/ConsumerToProducerProvisionAdapter.h"
#include "fennel/common/ByteArrayOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ConsumerToProducerProvisionAdapter::prepare(
    TupleStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);
    scratchAccessor = params.scratchAccessor;
    bufferLock.accessSegment(scratchAccessor);
}

void ConsumerToProducerProvisionAdapter::getResourceRequirements(
    ExecutionStreamResourceQuantity &minQuantity,
    ExecutionStreamResourceQuantity &optQuantity)
{
    ExecutionStream::getResourceRequirements(minQuantity,optQuantity);

    // one scratch page
    minQuantity.nCachePages += 1;

    optQuantity = minQuantity;
}

void ConsumerToProducerProvisionAdapter::open(bool restart)
{
    SingleInputTupleStream::open(restart);

    if (restart) {
        pBufferOutputStream->clear();
        return;
    }
    
    bufferLock.allocatePage();
    pBufferOutputStream = ByteArrayOutputStream::newByteArrayOutputStream(
        bufferLock.getPage().getWritableData(),
        bufferLock.getPage().getCache().getPageSize());
}

ByteInputStream &ConsumerToProducerProvisionAdapter::getProducerResultStream()
{
    return *this;
}

void ConsumerToProducerProvisionAdapter::readNextBuffer()
{
    pBufferOutputStream->clear();
    if (pInputStream->writeResultToConsumerBuffer(*pBufferOutputStream)) {
        PBuffer pBuffer = bufferLock.getPage().getWritableData();
        PBuffer pBufferEnd = pBufferOutputStream->getWritePointer(0);
        // should have written at least one tuple
        assert(pBufferEnd > pBuffer);
        setBuffer(
            pBuffer,
            pBufferEnd - pBuffer);
    } else {
        nullifyBuffer();
    }
}

TupleStream::BufferProvision
ConsumerToProducerProvisionAdapter::getResultBufferProvision() const
{
    return PRODUCER_PROVISION;
}

TupleStream::BufferProvision
ConsumerToProducerProvisionAdapter::getInputBufferRequirement() const
{
    return CONSUMER_PROVISION;
}

void ConsumerToProducerProvisionAdapter::closeImpl()
{
    bufferLock.unlock();
    pBufferOutputStream.reset();
    SingleInputTupleStream::closeImpl();
}

ExecutionStream *ConsumerToProducerProvisionAdapter::getImpl()
{
    return pInputStream->getImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End ConsumerToProducerProvisionAdapter.cpp

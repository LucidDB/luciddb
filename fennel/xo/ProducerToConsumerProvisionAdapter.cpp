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
#include "fennel/xo/ProducerToConsumerProvisionAdapter.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ProducerToConsumerProvisionAdapter::prepare(
    TupleStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);
    tupleAccessor.compute(getOutputDesc());
}

bool ProducerToConsumerProvisionAdapter::writeResultToConsumerBuffer(
    ByteOutputStream &outputStream)
{
    ByteInputStream &inputStream = pInputStream->getProducerResultStream();
    uint cbAvailableIn,cbAvailableOut;
    PConstBuffer pSrc = inputStream.getReadPointer(1,&cbAvailableIn);
    if (!pSrc) {
        return false;
    }
    PBuffer pDst = outputStream.getWritePointer(1,&cbAvailableOut);
    
    if (cbAvailableOut < cbAvailableIn) {
        // oops, impedance mismatch:  have to figure out how many
        // complete tuples we can safely copy without overflow
        PConstBuffer pTuple = pSrc;
        PConstBuffer pTupleSafe = pTuple;
        PConstBuffer pEnd = pSrc + cbAvailableOut;
        for (;;) {
            uint cbTuple = tupleAccessor.getBufferByteCount(pTuple);
            pTuple += cbTuple;
            if (pTuple > pEnd) {
                // this tuple would put us over the limit
                break;
            }
            // this tuple will fit
            pTupleSafe = pTuple;
        }
        cbAvailableIn = pTupleSafe - pSrc;
        assert(cbAvailableIn);
    }
    
    memcpy(pDst,pSrc,cbAvailableIn);
    inputStream.consumeReadPointer(cbAvailableIn);
    outputStream.consumeWritePointer(cbAvailableIn);
    // we can't use whatever's left in output buffer, so tell consumer
    // to give us a fresh one next time
    outputStream.hardPageBreak();
    return true;
}

TupleStream::BufferProvision
ProducerToConsumerProvisionAdapter::getResultBufferProvision() const
{
    return CONSUMER_PROVISION;
}

TupleStream::BufferProvision
ProducerToConsumerProvisionAdapter::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

ExecutionStream *ProducerToConsumerProvisionAdapter::getImpl()
{
    return pInputStream->getImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End ProducerToConsumerProvisionAdapter.cpp

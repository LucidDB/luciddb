/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
            // TODO:  this could be optimized a little if we had
            // a tuple accessor method which could tell us the
            // length without even messing with the null indicators
            tupleAccessor.setCurrentTupleBuf(pTuple);
            uint cbTuple = tupleAccessor.getCurrentByteCount();
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

FENNEL_END_CPPFILE("$Id$");

// End ProducerToConsumerProvisionAdapter.cpp

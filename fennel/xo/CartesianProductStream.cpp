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
#include "fennel/xo/CartesianProductStream.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CartesianProductStream::prepare(CartesianProductStreamParams const &params)
{
    DoubleInputTupleStream::prepare(params);
    TupleDescriptor const &leftDesc = pFirstInputStream->getOutputDesc();
    TupleDescriptor const &rightDesc = pSecondInputStream->getOutputDesc();
    leftAccessor.compute(leftDesc);
    rightAccessor.compute(rightDesc);
    outputDesc.insert(outputDesc.end(),leftDesc.begin(),leftDesc.end());
    outputDesc.insert(outputDesc.end(),rightDesc.begin(),rightDesc.end());
    outputAccessor.compute(outputDesc);
    outputData.compute(outputDesc);
}

void CartesianProductStream::open(bool restart)
{
    DoubleInputTupleStream::open(restart);
    // these are used as state variables during fetch
    leftAccessor.resetCurrentTupleBuf();
    rightAccessor.resetCurrentTupleBuf();
}

TupleDescriptor const &CartesianProductStream::getOutputDesc() const
{
    return outputDesc;
}

bool CartesianProductStream::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    ByteInputStream &leftInputStream =
        pFirstInputStream->getProducerResultStream();
    
    uint cbBuffer;
    PBuffer pBuffer = resultOutputStream.getWritePointer(1,&cbBuffer);
    PBuffer pBufferEnd = pBuffer + cbBuffer;
    PBuffer pNextTuple = pBuffer;

    // TODO:  lots of small optimizations possible here
    
    for (;;) {
        if (!leftAccessor.getCurrentTupleBuf()) {
            PConstBuffer pTupleBuf = leftInputStream.getReadPointer(1);
            if (!pTupleBuf) {
                uint cb = pNextTuple - pBuffer;
                resultOutputStream.consumeWritePointer(cb);
                return (cb > 0);
            }
            leftAccessor.setCurrentTupleBuf(pTupleBuf);
            leftAccessor.unmarshal(outputData);
        }
        ByteInputStream &rightInputStream =
            pSecondInputStream->getProducerResultStream();
        for (;;) {
            if (!rightAccessor.getCurrentTupleBuf()) {
                PConstBuffer pTupleBuf = rightInputStream.getReadPointer(1);
                if (!pTupleBuf) {
                    leftInputStream.consumeReadPointer(
                        leftAccessor.getCurrentByteCount());
                    leftAccessor.resetCurrentTupleBuf();
                    // restart right input stream
                    pSecondInputStream->open(true);
                    // NOTE: break out of the inner for loop, which will take
                    // us back to the top of the outer for loop
                    break;
                }
                rightAccessor.setCurrentTupleBuf(pTupleBuf);
                rightAccessor.unmarshal(outputData,leftAccessor.size());
            }
            if (!outputAccessor.isBufferSufficient(
                    outputData,pBufferEnd - pNextTuple))
            {
                resultOutputStream.consumeWritePointer(pNextTuple - pBuffer);
                return true;
            }
            outputAccessor.marshal(outputData,pNextTuple);
            pNextTuple += outputAccessor.getCurrentByteCount();
            rightInputStream.consumeReadPointer(
                rightAccessor.getCurrentByteCount());
            rightAccessor.resetCurrentTupleBuf();
        }
    }
}

TupleStream::BufferProvision
CartesianProductStream::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

TupleStream::BufferProvision
CartesianProductStream::getResultBufferProvision() const
{
    return CONSUMER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End CartesianProductStream.cpp

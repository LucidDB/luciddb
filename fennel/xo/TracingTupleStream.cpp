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
#include "fennel/xo/TracingTupleStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void TracingTupleStream::prepare(TupleStreamParams const &params)
{
    SingleInputTupleStream::prepare(params);
    TupleDescriptor const &tupleDesc = getOutputDesc();
    tupleAccessor.compute(tupleDesc);
    tupleData.compute(tupleDesc);
}

void TracingTupleStream::readNextBuffer()
{
    ByteInputStream &inputResultStream =
        pInputStream->getProducerResultStream();
    inputResultStream.consumeReadPointer(getBytesConsumed());
    uint cbNextBuffer;
    PConstBuffer pNextBuffer = inputResultStream.getReadPointer(
        1,&cbNextBuffer);
    if (pNextBuffer) {
        setBuffer(pNextBuffer,cbNextBuffer);
        PConstBuffer pBufferEnd = pNextBuffer + cbNextBuffer;
        TupleDescriptor const &tupleDesc = getOutputDesc();
        while (pNextBuffer < pBufferEnd) {
            tupleAccessor.setCurrentTupleBuf(pNextBuffer);
            // while we're here, we might as well sanity-check the input
            assert(pNextBuffer + tupleAccessor.getCurrentByteCount()
                   <= pBufferEnd);
            tupleAccessor.unmarshal(tupleData);
            // TODO:  sanity-check individual data values?
            std::ostringstream oss;
            tuplePrinter.print(oss,tupleDesc,tupleData);
            trace(TRACE_FINE,oss.str());
            pNextBuffer += tupleAccessor.getCurrentByteCount();
        }
    } else {
        nullifyBuffer();
    }
}

ByteInputStream &TracingTupleStream::getProducerResultStream()
{
    return *this;
}

TupleStream::BufferProvision
TracingTupleStream::getResultBufferProvision() const
{
    return PRODUCER_PROVISION;
}

TupleStream::BufferProvision
TracingTupleStream::getInputBufferRequirement() const
{
    return PRODUCER_PROVISION;
}

void *TracingTupleStream::getImpl()
{
    return pInputStream->getImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End TracingTupleStream.cpp

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
#include "fennel/xo/MockTupleStream.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void MockTupleStream::prepare(MockTupleStreamParams const &params)
{
    TupleStream::prepare(params);
    assert(!pGraph->getInputCount(getStreamId()));
    outputTupleDesc = params.outputTupleDesc;
    assert(outputTupleDesc.size() == 1);
    assert(!outputTupleDesc[0].isNullable);
    assert(StandardTypeDescriptor::isIntegralNative(
               StandardTypeDescriptorOrdinal(
                   outputTupleDesc[0].pTypeDescriptor->getOrdinal())));
    nRowsMax = params.nRows;
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(outputTupleDesc);
    assert(tupleAccessor.isFixedWidth());
    cbTuple = tupleAccessor.getMaxByteCount();
}

TupleDescriptor const &MockTupleStream::getOutputDesc() const
{
    return outputTupleDesc;
}

void MockTupleStream::open(bool restart)
{
    // TODO: pass restart request on to Java!  Requires support up in Farrago
    // which is currently missing.  For now we use buffering to ensure that we
    // never get here.
    assert(!restart);

    TupleStream::open(restart);

    nRowsProduced = 0;
}

bool MockTupleStream::writeResultToConsumerBuffer(
    ByteOutputStream &outputStream)
{
    uint cbBatch = 0;
    uint cb;
    PBuffer pBuffer = outputStream.getWritePointer(1,&cb);
    while ((cb >= cbTuple) && (nRowsProduced < nRowsMax)) {
        cb -= cbTuple;
        cbBatch += cbTuple;
        ++nRowsProduced;
    }
    memset(pBuffer,0,cbBatch);
    if (cbBatch) {
        outputStream.consumeWritePointer(cbBatch);
        return true;
    } else {
        return false;
    }
}

void MockTupleStream::closeImpl()
{
    TupleStream::closeImpl();
}

TupleStream::BufferProvision
MockTupleStream::getResultBufferProvision() const
{
    return CONSUMER_PROVISION;
}

FENNEL_END_CPPFILE("$Id$");

// End MockTupleStream.cpp

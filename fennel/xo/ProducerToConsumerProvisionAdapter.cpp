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

bool ProducerToConsumerProvisionAdapter::writeResultToConsumerBuffer(
    ByteOutputStream &outputStream)
{
    // TODO:  Deal with impedance mismatch (producer may have a larger buffer
    // size than consumer).  In that case, have to fall back to copying
    // individual tuples.
    ByteInputStream &inputStream = pInputStream->getProducerResultStream();
    uint cb;
    PConstBuffer pSrc = inputStream.getReadPointer(1,&cb);
    if (!pSrc) {
        return false;
    }
    PBuffer pDst = outputStream.getWritePointer(cb);
    memcpy(pDst,pSrc,cb);
    inputStream.consumeReadPointer(cb);
    outputStream.consumeWritePointer(cb);
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

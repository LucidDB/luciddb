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
#include "fennel/xo/TupleStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStream::TupleStream()
{
    pGraph = NULL;
    id = MAXU;
    isOpen = false;
}

void TupleStream::open(bool restart)
{
    if (restart) {
        assert(isOpen);
    } else {
        // NOTE: this assertion is bad because in case of multiple inheritance,
        // open can be called twice.  So we rely on the corresponding assertion
        // in TupleStreamGraph instead, unless someone can come up with
        // something better.
#if 0
        assert(!isOpen);
#endif
        isOpen = true;
        needsClose = true;
    }
}

void TupleStream::closeImpl()
{
    isOpen = false;
}

TupleFormat TupleStream::getOutputFormat() const
{
    return TUPLE_FORMAT_STANDARD;
}

void TupleStream::prepare(TupleStreamParams const &)
{
}

ByteInputStream &TupleStream::getProducerResultStream()
{
    assert(false);
    throw;
}

bool TupleStream::writeResultToConsumerBuffer(
    ByteOutputStream &resultOutputStream)
{
    assert(false);
    throw;
}

TupleStream::BufferProvision TupleStream::getInputBufferRequirement() const
{
    return NO_PROVISION;
}

SharedTupleStream TupleStream::getStreamInput(uint ordinal)
{
    return pGraph->getStreamInput(getStreamId(),ordinal);
}

FENNEL_END_CPPFILE("$Id$");

// End TupleStream.cpp

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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
#include "fennel/xo/SingleInputTupleStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SingleInputTupleStream::prepare(TupleStreamParams const &params)
{
    TupleStream::prepare(params);
    assert(pGraph->getInputCount(getStreamId()) == 1);
    assert(getInputBufferRequirement() != NO_PROVISION);
    pInputStream = getTupleStreamInput(0);
}

TupleDescriptor const &SingleInputTupleStream::getOutputDesc() const
{
    return pInputStream->getOutputDesc();
}

void SingleInputTupleStream::open(bool restart)
{
    TupleStream::open(restart);
    if (restart) {
        pInputStream->open(true);
    } else {
        pInputStream = getTupleStreamInput(0);
    }
}

void SingleInputTupleStream::closeImpl()
{
    pInputStream.reset();
    TupleStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End SingleInputTupleStream.cpp

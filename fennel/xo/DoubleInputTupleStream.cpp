/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/xo/DoubleInputTupleStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void DoubleInputTupleStream::prepare(TupleStreamParams const &params)
{
    TupleStream::prepare(params);
    assert(pGraph->getInputCount(getStreamId()) == 2);
    assert(getInputBufferRequirement() != NO_PROVISION);
    pFirstInputStream = getTupleStreamInput(0);
    pSecondInputStream = getTupleStreamInput(1);
}

void DoubleInputTupleStream::open(bool restart)
{
    TupleStream::open(restart);
    if (restart) {
        pFirstInputStream->open(true);
        pSecondInputStream->open(true);
    } else {
        pFirstInputStream = getTupleStreamInput(0);
        pSecondInputStream = getTupleStreamInput(1);
    }
}

void DoubleInputTupleStream::closeImpl()
{
    pFirstInputStream.reset();
    pSecondInputStream.reset();
    TupleStream::closeImpl();
}

FENNEL_END_CPPFILE("$Id$");

// End DoubleInputTupleStream.cpp

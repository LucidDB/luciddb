/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
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
#include "fennel/xo/TupleStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleStream::TupleStream() : ExecutionStream()
{
}

SharedTupleStream TupleStream::getTupleStreamInput(uint ordinal)
{
    SharedExecutionStream stream = getStreamInput(ordinal);
    return boost::static_pointer_cast<TupleStream>(stream);
}

void TupleStream::prepare(TupleStreamParams const &params)
{
    ExecutionStream::prepare(params);
}

FENNEL_END_CPPFILE("$Id$");

// End TupleStream.cpp

/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2004-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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
#include "fennel/tuple/TupleDataWithBuffer.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TupleDataWithBuffer::TupleDataWithBuffer()
{
}

TupleDataWithBuffer::TupleDataWithBuffer(TupleDescriptor const& tupleDesc)
{
    computeAndAllocate(tupleDesc);
}

void TupleDataWithBuffer::computeAndAllocate(TupleDescriptor const& tupleDesc)
{
    TupleAccessor tupleAccessor;
    tupleAccessor.compute(tupleDesc, TUPLE_FORMAT_ALL_NOT_NULL_AND_FIXED);
    array.reset(new FixedBuffer[tupleAccessor.getMaxByteCount()]);
    tupleAccessor.setCurrentTupleBuf(array.get());
    compute(tupleDesc);
    tupleAccessor.unmarshal(*this);
}

TupleDataWithBuffer::~TupleDataWithBuffer()
{
}

FENNEL_END_CPPFILE("$Id$");

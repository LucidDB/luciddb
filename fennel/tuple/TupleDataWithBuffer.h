/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2004-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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

#ifndef Fennel_TupleDataWithBuffer_Included
#define Fennel_TupleDataWithBuffer_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * TupleDataWithBuffer is a convenience that creates a TupleData, and
 * a supporting buffer from a TupleDescriptor.
 *
 * A common use is to create an input and output tuple for Calculator
 * given the TupleDescriptor obtained from
 * Calculator::getOutputRegisterDescriptor and from
 * Calculator::getInputRegisterDescriptor()
 *
 */
class TupleDataWithBuffer : public TupleData
{
public:
    explicit TupleDataWithBuffer();
    explicit TupleDataWithBuffer(TupleDescriptor const& tupleDesc);
    void computeAndAllocate(TupleDescriptor const& tupleDesc);
    void resetBuffer();
    ~TupleDataWithBuffer();
private:
    TupleAccessor tupleAccessor;
    boost::scoped_array<FixedBuffer> array;
};

FENNEL_END_NAMESPACE

#endif
// End TupleDataWithBuffer.h

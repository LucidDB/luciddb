/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2004-2004 Disruptive Technologies, Inc.
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

#ifndef Fennel_TupleDataWithBuffer_Included
#define Fennel_tupleDataWithBuffer_Included

#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * TupleDataWithBuffer is a convienence that creates a TupleData, and
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
    explicit
    TupleDataWithBuffer(TupleDescriptor const& tupleDesc);
    ~TupleDataWithBuffer();
private:
    boost::scoped_array<FixedBuffer> array;
};

FENNEL_END_NAMESPACE

#endif
// End TupleDataWithBuffer.h

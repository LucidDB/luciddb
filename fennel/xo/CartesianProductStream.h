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

#ifndef Fennel_CartesianProductStream_Included
#define Fennel_CartesianProductStream_Included

#include "fennel/xo/DoubleInputTupleStream.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CartesianProductStreamParams defines parameters for instantiating a
 * CartesianProductStream.
 *
 *<p>
 *
 * TODO:  Take a join filter once we have a calculator?
 */
struct CartesianProductStreamParams : public TupleStreamParams
{
};

/**
 * CartesianProductStream produces the Cartesian product of two input
 * TupleStreams.  The first input will be iterated only once, while the second
 * input will be opened, iterated, and closed for each tuple from the first
 * input.
 */
class CartesianProductStream : public DoubleInputTupleStream
{
    TupleDescriptor outputDesc;
    TupleAccessor leftAccessor;
    TupleAccessor rightAccessor;
    TupleAccessor outputAccessor;
    TupleData outputData;
    
public:
    void prepare(CartesianProductStreamParams const &params);
    virtual void open(bool restart);
    virtual bool writeResultToConsumerBuffer(
        ByteOutputStream &resultOutputStream);
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual BufferProvision getInputBufferRequirement() const;
    virtual BufferProvision getResultBufferProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End CartesianProductStream.h

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 2003-2004 Disruptive Tech
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation; either version 2 of the License, or
// (at your option) any later version.
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

#ifndef Fennel_CalcTupleStream_Included
#define Fennel_CalcTupleStream_Included

#include "fennel/xo/SingleInputTupleStream.h"
#include "fennel/disruptivetech/xo/CalcExecutionStream.h"
#include "fennel/disruptivetech/calc/CalcCommon.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CalcTupleStreamParams defines parameters for instantiating a
 * CalcTupleStream.
 */
struct CalcTupleStreamParams :
    public TupleStreamParams, public CalcExecutionStreamParams
{
};

/**
 * CalcTupleStream reads tuples from a child stream and performs calculations
 * of SQL expressions.  For every input tuple which passes a boolean filter
 * expression, an output tuple is computed based on projection expressions.
 */
class CalcTupleStream :
    public SingleInputTupleStream, public CalcExecutionStream
{
public:
    // implement TupleStream
    virtual void prepare(CalcTupleStreamParams const &params);
    virtual TupleDescriptor const &getOutputDesc() const;
    virtual bool writeResultToConsumerBuffer(ByteOutputStream &);
    virtual void closeImpl();
    virtual BufferProvision getResultBufferProvision() const;
    virtual BufferProvision getInputBufferRequirement() const;
};

FENNEL_END_NAMESPACE

#endif

// End CalcTupleStream.h

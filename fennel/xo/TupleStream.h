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

#ifndef Fennel_TupleStream_Included
#define Fennel_TupleStream_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/tuple/TupleFormat.h"
#include "fennel/xo/ExecutionStream.h"
#include "fennel/xo/TupleStreamGraph.h"
#include "fennel/segment/SegmentAccessor.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Common parameters for instantiating any TupleStream.
 */
struct TupleStreamParams : public ExecutionStreamParams
{};

/**
 * TupleStream is an abstract base class for all tuple stream execution objects
 * (also known as XO's).  A TupleStream produces tuples according to a fixed
 * tuple descriptor.  A TupleStream has zero or more input streams which it may
 * transform to produce its output.  Dataflow is always initiated by the
 * consumer, and takes place in batches of tuples.  
 */
class TupleStream : public ExecutionStream<
        TupleStreamGraph, SharedTupleStream>
    
{
    friend class TupleStreamGraphImpl;
protected:
    /**
     * Constructor.  Note that derived class constructors must never take any
     * parameters in order to support deserialization.  See notes on method
     * prepare() for more information.
     */
    explicit TupleStream();

public:
    virtual void prepare(TupleStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End TupleStream.h

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

#ifndef Fennel_TupleStream_Included
#define Fennel_TupleStream_Included

#include "fennel/xo/ExecutionStream.h"
#include "fennel/xo/TupleStreamGraph.h"

FENNEL_BEGIN_NAMESPACE

/**
 * Common parameters for instantiating any TupleStream.
 */
struct TupleStreamParams : public ExecutionStreamParams
{
};

/**
 * TupleStream is an abstract base class for all ExecutionStreams which expect
 * dataflow to be initiated by the consumer (pull-model).  A TupleStream has
 * zero or more input streams which it may transform to produce its output.
 */
class TupleStream : public ExecutionStream
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
    /**
     * Utility method for accessing a child input stream.
     *
     * @param ordinal 0-based ordinal of child
     *
     * @return reference to child
     */
    virtual SharedTupleStream getTupleStreamInput(uint ordinal);

    // refine ExecutionStream
    virtual void prepare(TupleStreamParams const &params);
};

FENNEL_END_NAMESPACE

#endif

// End TupleStream.h

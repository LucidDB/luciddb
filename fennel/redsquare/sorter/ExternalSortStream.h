/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Red Square, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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

#ifndef Fennel_ExternalSortStream_Included
#define Fennel_ExternalSortStream_Included

#include "fennel/xo/SingleInputTupleStream.h"
#include "fennel/common/FemEnums.h"
#include "fennel/tuple/TupleDescriptor.h"

FENNEL_BEGIN_NAMESPACE

// DEPRECATED
    
/**
 * ExternalSortStreamParams defines parameters for instantiating a
 * ExternalSortStream. Note that when distinctness is DUP_DISCARD, the key
 * should normally be the whole tuple to avoid non-determinism in
 * which tuples are discarded.
 */
struct ExternalSortStreamParams : public TupleStreamParams
{
    // TODO:  implement duplicate handling
    /**
     * Mode for dealing with duplicate values.
     */
    Distinctness distinctness;
    
    /**
     * Segment to use for storing temp pages.
     */
    SharedSegment pTempSegment;
    
    // TODO:  ASC/DESC and generalized collation support
    /**
     * Sort key projection (relative to tupleDesc).
     */
    TupleProjection keyProj;

    /**
     * Whether to materialize one big final run, or return results
     * directly from last merge stage.
     */
    bool storeFinalRun;
};

/**
 * ExternalSortStream sorts its input stream according to a parameterized key
 * and returns the sorted data as its output.  The implementation is a standard
 * external sort (degrading stepwise from in-memory quicksort to two-pass
 * merge-sort to multi-pass merge-sort).
 *
 *<p>
 *
 * The actual implementation is in ExternalSortStreamImpl.
 */
class ExternalSortStream : public SingleInputTupleStream
{
public:
    /**
     * Factory method.
     *
     * @return new ExternalSortStream instance
     */
    static ExternalSortStream &newExternalSortStream();
    
    // implement TupleStream
    virtual void prepare(ExternalSortStreamParams const &params) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortStream.h

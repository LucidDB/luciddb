/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2004-2009 LucidEra, Inc.
// Portions Copyright (C) 2004-2009 John V. Sichi
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

#ifndef Fennel_ExternalSortExecStream_Included
#define Fennel_ExternalSortExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/common/FemEnums.h"
#include "fennel/tuple/TupleDescriptor.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * ExternalSortExecStreamParams defines parameters for instantiating an
 * ExternalSortExecStream. Note that when distinctness is DUP_DISCARD, the key
 * should normally be the whole tuple to avoid non-determinism with
 * respect to which tuples are discarded.
 */
struct ExternalSortExecStreamParams : public ConduitExecStreamParams
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

    /**
     * Sort key projection (relative to tupleDesc).
     */
    TupleProjection keyProj;

    // TODO:  generalized collation support
    /**
     * Vector with positions corresponding to those of keyProj; true indicates
     * a descending key column, while false indicates asscending.  If this
     * vector is empty, all columns are assumed to be ascending; otherwise, it
     * must be the same length as keyProj.
     */
    std::vector<bool> descendingKeyColumns;

    /**
     * Whether to materialize one big final run, or return results
     * directly from last merge stage.
     */
    bool storeFinalRun;

    /**
     * Estimate of the number of rows in the sort input.  If MAXU, no stats
     * were available to estimate this value.
     */
    RecordNum estimatedNumRows;

    /**
     * If true, close producers once all input has been read
     */
    bool earlyClose;
};

/**
 * ExternalSortExecStream sorts its input stream according to a parameterized
 * key and returns the sorted data as its output.  The implementation is a
 * standard external sort (degrading stepwise from in-memory quicksort to
 * two-pass merge-sort to multi-pass merge-sort).
 *
 *<p>
 *
 * The actual implementation is in ExternalSortExecStreamImpl.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_SORTER_EXPORT ExternalSortExecStream
    : public ConduitExecStream
{
public:
    /**
     * Factory method.
     *
     * @return new ExternalSortExecStream instance
     */
    static ExternalSortExecStream *newExternalSortExecStream();

    // implement ExecStream
    virtual void prepare(ExternalSortExecStreamParams const &params) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortExecStream.h

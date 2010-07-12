/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2010 The Eigenbase Project
// Copyright (C) 2010 SQLstream, Inc.
// Copyright (C) 2010 Dynamo BI Corporation
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

#ifndef Fennel_LcsCountAggExecStream_Included
#define Fennel_LcsCountAggExecStream_Included

#include "fennel/lcs/LcsRowScanExecStream.h"

#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Parameters specific to the LCS row count execution stream.
 */
struct FENNEL_LCS_EXPORT LcsCountAggExecStreamParams
    : public LcsRowScanExecStreamParams
{
};

/**
 * LcsCountAggExecStream returns a count of the rows in a stream rather than
 * returning their contents.
 *
 * @author John Sichi
 * @version $Id$
 */
class FENNEL_LCS_EXPORT LcsCountAggExecStream
    : public LcsRowScanExecStream
{
    /**
     * Buffer holding the outputTuple to provide to the consumers.
     */
    boost::scoped_array<FixedBuffer> outputTupleBuffer;

    /**
     * Convenience reference to the scratch tuple accessor
     * contained in pOutAccessor.
     */
    TupleAccessor *pOutputTupleAccessor;

public:
    explicit LcsCountAggExecStream();

    // override LcsRowScanExecStream
    virtual void prepare(LcsRowScanExecStreamParams const &params);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    // override SingleOutputExecStream
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End LcsCountAggExecStream.h

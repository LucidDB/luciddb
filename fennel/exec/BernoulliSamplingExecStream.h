/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2007 The Eigenbase Project
// Copyright (C) 2007-2007 Disruptive Tech
// Copyright (C) 2007-2007 LucidEra, Inc.
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

#ifndef Fennel_BernoulliSamplingExecStream_Included
#define Fennel_BernoulliSamplingExecStream_Included

#include <boost/scoped_ptr.hpp>
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/common/BernoulliRng.h"
#include "fennel/tuple/TupleData.h"

FENNEL_BEGIN_NAMESPACE

/**
 * BernoulliSamplingExecStreamParams defines parameters for
 * BernoulliSamplingExecStream.
 */
struct BernoulliSamplingExecStreamParams : public ConduitExecStreamParams
{
    /** Sampling rate in the range [0, 1]. */
    float samplingRate;

    /** If true, the sample should be repeatable. */
    bool isRepeatable;

    /** The seed to use for repeatable sampling. */
    int32_t repeatableSeed;
};

/**
 * BernoulliSamplingExecStream implements TABLESAMPLE BERNOULLI.  Because the
 * sampling is applied to each input row independently, this XO may be placed
 * in any order w.r.t. other filters but must come before any aggregation XOs.
 *
 * @author Stephan Zuercher
 */
class BernoulliSamplingExecStream : public ConduitExecStream
{
    /** Sampling rate in the range [0, 1]. */
    float samplingRate;

    /** If true, the sample should be repeatable. */
    bool isRepeatable;

    /** The seed to use for repeatable sampling. */
    int32_t repeatableSeed;

    /**
     * RNG for Bernoulli sampling.
     */
    boost::scoped_ptr<BernoulliRng> samplingRng;

    /**
     * True if production of a tuple to the output stream is pending
     */
    bool producePending;

    TupleData data;

public:
    virtual void prepare(BernoulliSamplingExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
};

FENNEL_END_NAMESPACE

#endif

// End BernoulliSamplingExecStream.h

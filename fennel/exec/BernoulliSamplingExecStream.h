/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
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
struct FENNEL_EXEC_EXPORT BernoulliSamplingExecStreamParams
    : public ConduitExecStreamParams
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
class FENNEL_EXEC_EXPORT BernoulliSamplingExecStream
    : public ConduitExecStream
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

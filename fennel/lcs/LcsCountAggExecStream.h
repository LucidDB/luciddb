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

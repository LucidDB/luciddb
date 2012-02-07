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

#ifndef Fennel_ValuesExecStream_Included
#define Fennel_ValuesExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"

#include <boost/shared_array.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * ValuesExecStreamParams defines parameters for ValuesExecStream.
 */
struct FENNEL_EXEC_EXPORT ValuesExecStreamParams
    : public SingleOutputExecStreamParams
{
    /**
     * Number of bytes in buffer
     */
    uint bufSize;

    /**
     * Buffer containing tuples that stream will produce
     */
    boost::shared_array<FixedBuffer> pTupleBuffer;
};

/**
 * ValuesExecStream passes a buffer of tuples passed in as a parameter into
 * the stream on to its consumer to process.
 *
 * @author Zelaine Fong
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ValuesExecStream
    : public SingleOutputExecStream
{
    /**
     * Number of bytes in input buffer
     */
    uint bufSize;

    /**
     * Pointer to start of input tuple buffer
     */
    boost::shared_array<FixedBuffer> pTupleBuffer;

    /**
     * True if stream has passed on its buffer to its consumer
     */
    bool produced;

public:
    // implement ExecStream
    virtual void prepare(ValuesExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End ValuesExecStream.h

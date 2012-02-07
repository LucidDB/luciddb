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

#ifndef Fennel_SplitterExecStream_Included
#define Fennel_SplitterExecStream_Included

#include "fennel/exec/DiffluenceExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SplitterExecStreamParams defines parameters for SplitterExecStream.
 */
struct FENNEL_EXEC_EXPORT SplitterExecStreamParams
    : public DiffluenceExecStreamParams
{
};

/**
 * SplitterExecStream is an adapter for aliasing the output of an upstream
 * producer for use by several downstream consumers.  SplitterExecStream itself
 * does not allocate any buffers.  Instead, it requires the upstream producer
 * to supply a buffer, and the downstream consumers all read from that same
 * buffer.
 *
 * @author Rushan Chen
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SplitterExecStream
    : public DiffluenceExecStream
{
    /**
     * 0-based ordinal of next output from which to retrieve state
     */
    uint iOutput;

    /**
     * End of input buffer currently being consumed.
     */
    PConstBuffer pLastConsumptionEnd;

public:
    // implement ExecStream
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);

    // override DiffluenceExecStream
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SplitterExecStream.h

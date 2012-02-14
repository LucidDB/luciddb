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

#ifndef Fennel_ScratchBufferExecStream_Included
#define Fennel_ScratchBufferExecStream_Included

#include "fennel/exec/ConduitExecStream.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ScratchBufferExecStreamParams defines parameters for ScratchBufferExecStream.
 */
struct FENNEL_EXEC_EXPORT ScratchBufferExecStreamParams
    : public ConduitExecStreamParams
{
};

/**
 * ScratchBufferExecStream is an adapter for converting the output of an
 * upstream BUFPROV_CONSUMER producer for use by a downstream BUFPROV_PRODUCER
 * consumer.  The implementation works by allocating a scratch buffer; the
 * upstream producer writes its results into this buffer, and the downstream
 * consumer reads input from this buffer.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ScratchBufferExecStream
    : public ConduitExecStream
{
    SegmentAccessor scratchAccessor;
    SegPageLock bufferLock;
    PConstBuffer pLastConsumptionEnd;

public:
    // implement ExecStream
    virtual void prepare(ScratchBufferExecStreamParams const &params);
    virtual void getResourceRequirements(
        ExecStreamResourceQuantity &minQuantity,
        ExecStreamResourceQuantity &optQuantity);
    virtual void open(bool restart);
    virtual ExecStreamResult execute(ExecStreamQuantum const &quantum);
    virtual void closeImpl();
    virtual ExecStreamBufProvision getOutputBufProvision() const;
    virtual ExecStreamBufProvision getInputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End ScratchBufferExecStream.h

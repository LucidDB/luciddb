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

#ifndef Fennel_ConduitExecStream_Included
#define Fennel_ConduitExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/exec/SingleInputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ConduitExecStreamParams defines parameters for ConduitExecStream.
 */
struct FENNEL_EXEC_EXPORT ConduitExecStreamParams
    : virtual public SingleInputExecStreamParams,
        virtual public SingleOutputExecStreamParams
{
};

/**
 * ConduitExecStream is an abstract base for any ExecStream with exactly
 * one input and one output.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ConduitExecStream
    : virtual public SingleInputExecStream,
        virtual public SingleOutputExecStream
{
protected:
    /**
     * Checks the state of the input and output buffers.  If input empty,
     * requests production.  If input EOS, propagates that to output buffer.
     * If output full, returns EXECRC_OVERFLOW.
     *
     * @return result of precheck; anything but EXECRC_YIELD indicates
     * that execution should terminate immediately with returned code
     */
    ExecStreamResult precheckConduitBuffers();

public:
    // implement ExecStream
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors);
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors);
    virtual void prepare(ConduitExecStreamParams const &params);
    virtual void open(bool restart);
};

FENNEL_END_NAMESPACE

#endif

// End ConduitExecStream.h

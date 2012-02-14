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

#ifndef Fennel_SingleOutputExecStream_Included
#define Fennel_SingleOutputExecStream_Included

#include "fennel/exec/ExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SingleOutputExecStreamParams defines parameters for SingleOutputExecStream.
 */
struct FENNEL_EXEC_EXPORT SingleOutputExecStreamParams
    : virtual public ExecStreamParams
{
    TupleDescriptor outputTupleDesc;

    TupleFormat outputTupleFormat;

    explicit SingleOutputExecStreamParams();
};

/**
 * SingleOutputExecStream is an abstract base for all implementations
 * of ExecStream which have exactly one output.  By default no
 * inputs are consumed, but derived classes may override.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT SingleOutputExecStream
    : virtual public ExecStream
{
protected:
    SharedExecStreamBufAccessor pOutAccessor;

public:
    // implement ExecStream
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors);
    virtual void setOutputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &outAccessors);
    virtual void prepare(SingleOutputExecStreamParams const &params);
    virtual void open(bool restart);
    virtual ExecStreamBufProvision getOutputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End SingleOutputExecStream.h

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

#ifndef Fennel_ConfluenceExecStream_Included
#define Fennel_ConfluenceExecStream_Included

#include "fennel/exec/SingleOutputExecStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ConfluenceExecStreamParams defines parameters for ConfluenceExecStream.
 */
struct FENNEL_EXEC_EXPORT ConfluenceExecStreamParams
    : virtual public SingleOutputExecStreamParams
{
};

/**
 * ConfluenceExecStream is an abstract base for any ExecStream with
 * multiple inputs and exactly one output.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class FENNEL_EXEC_EXPORT ConfluenceExecStream
    : virtual public SingleOutputExecStream
{
protected:
    std::vector<SharedExecStreamBufAccessor> inAccessors;

public:
    // implement ExecStream
    virtual void prepare(ConfluenceExecStreamParams const &params);
    virtual void setInputBufAccessors(
        std::vector<SharedExecStreamBufAccessor> const &inAccessors);
    virtual void open(bool restart);
    virtual ExecStreamBufProvision getInputBufProvision() const;
};

FENNEL_END_NAMESPACE

#endif

// End ConfluenceExecStream.h

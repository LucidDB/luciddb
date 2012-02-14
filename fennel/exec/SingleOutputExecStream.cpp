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

#include "fennel/common/CommonPreamble.h"
#include "fennel/exec/SingleOutputExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SingleOutputExecStreamParams::SingleOutputExecStreamParams()
{
    outputTupleFormat = TUPLE_FORMAT_STANDARD;
}

void SingleOutputExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    assert(inAccessors.size() == 0);
}

void SingleOutputExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    assert(outAccessors.size() == 1);
    pOutAccessor = outAccessors[0];
}

void SingleOutputExecStream::prepare(SingleOutputExecStreamParams const &params)
{
    ExecStream::prepare(params);
    assert(pOutAccessor);
    assert(pOutAccessor->getProvision() == getOutputBufProvision());
    if (pOutAccessor->getTupleDesc().empty()) {
        assert(!params.outputTupleDesc.empty());
        pOutAccessor->setTupleShape(
            params.outputTupleDesc,
            params.outputTupleFormat);
    }
}

void SingleOutputExecStream::open(bool restart)
{
    ExecStream::open(restart);
    if (restart) {
        pOutAccessor->clear();
    }
}

ExecStreamBufProvision SingleOutputExecStream::getOutputBufProvision() const
{
    return BUFPROV_CONSUMER;
}

FENNEL_END_CPPFILE("$Id$");

// End SingleOutputExecStream.cpp

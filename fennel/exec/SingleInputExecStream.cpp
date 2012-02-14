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
#include "fennel/exec/SingleInputExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void SingleInputExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    assert(outAccessors.size() == 0);
}

void SingleInputExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    assert(inAccessors.size() == 1);
    pInAccessor = inAccessors[0];
}

void SingleInputExecStream::prepare(SingleInputExecStreamParams const &params)
{
    ExecStream::prepare(params);

    assert(pInAccessor);
    assert(pInAccessor->getProvision() == getInputBufProvision());
}

void SingleInputExecStream::open(bool restart)
{
    ExecStream::open(restart);
    if (restart) {
        // restart input
        pInAccessor->clear();
        pGraph->getStreamInput(getStreamId(), 0)->open(true);
    }
}

ExecStreamBufProvision SingleInputExecStream::getInputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End SingleInputExecStream.cpp

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
#include "fennel/exec/ConfluenceExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ConfluenceExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessorsInit)
{
    inAccessors = inAccessorsInit;
}

void ConfluenceExecStream::prepare(ConfluenceExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);

    for (uint i = 0; i < inAccessors.size(); ++i) {
        assert(inAccessors[i]->getProvision() == getInputBufProvision());
    }
}

void ConfluenceExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    if (restart) {
        // restart inputs
        for (uint i = 0; i < inAccessors.size(); ++i) {
            inAccessors[i]->clear();
            pGraph->getStreamInput(getStreamId(), i)->open(true);
        }
    }
}

ExecStreamBufProvision ConfluenceExecStream::getInputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End ConfluenceExecStream.cpp

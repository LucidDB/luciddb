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
#include "fennel/exec/ConduitExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");
void ConduitExecStream::setInputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &inAccessors)
{
    SingleInputExecStream::setInputBufAccessors(inAccessors);
}

void ConduitExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessors)
{
    SingleOutputExecStream::setOutputBufAccessors(outAccessors);
}

void ConduitExecStream::prepare(ConduitExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);

    if (params.outputTupleDesc.empty()) {
        pOutAccessor->setTupleShape(
            pInAccessor->getTupleDesc(),
            pInAccessor->getTupleFormat());
    }

    SingleOutputExecStream::prepare(params);
}

void ConduitExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    SingleInputExecStream::open(restart);
}

ExecStreamResult ConduitExecStream::precheckConduitBuffers()
{
    switch (pInAccessor->getState()) {
    case EXECBUF_EMPTY:
        pInAccessor->requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        break;
    default:
        permAssert(false);
    }
    if (pOutAccessor->getState() == EXECBUF_OVERFLOW) {
        return EXECRC_BUF_OVERFLOW;
    }
    return EXECRC_YIELD;
}

FENNEL_END_CPPFILE("$Id$");

// End ConduitExecStream.cpp

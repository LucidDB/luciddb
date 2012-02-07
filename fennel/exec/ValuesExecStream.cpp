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
#include "fennel/exec/ValuesExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void ValuesExecStream::prepare(ValuesExecStreamParams const &params)
{
    SingleOutputExecStream::prepare(params);
    pTupleBuffer = params.pTupleBuffer;
    bufSize = params.bufSize;
}

void ValuesExecStream::open(bool restart)
{
    SingleOutputExecStream::open(restart);
    produced = false;
}

ExecStreamResult ValuesExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (produced || bufSize == 0) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    pOutAccessor->provideBufferForConsumption(
        pTupleBuffer.get(), pTupleBuffer.get() + bufSize);
    produced = true;
    return EXECRC_BUF_OVERFLOW;
}

ExecStreamBufProvision ValuesExecStream::getOutputBufProvision() const
{
    return BUFPROV_PRODUCER;
}

FENNEL_END_CPPFILE("$Id$");

// End ValuesExecStream.cpp

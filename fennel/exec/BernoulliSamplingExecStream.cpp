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
#include "fennel/exec/BernoulliSamplingExecStream.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BernoulliSamplingExecStream::prepare(
    BernoulliSamplingExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    samplingRate = params.samplingRate;
    isRepeatable = params.isRepeatable;
    repeatableSeed = params.repeatableSeed;

    samplingRng.reset(new BernoulliRng(samplingRate));

    assert(pInAccessor->getTupleDesc() == pOutAccessor->getTupleDesc());

    data.compute(pOutAccessor->getTupleDesc());
}

void BernoulliSamplingExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);

    if (isRepeatable) {
        samplingRng->reseed(repeatableSeed);
    } else if (!restart) {
        samplingRng->reseed(static_cast<uint32_t>(time(0)));
    }

    producePending = false;
}

ExecStreamResult BernoulliSamplingExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    if (producePending) {
        if (!pOutAccessor->produceTuple(data)) {
            return EXECRC_BUF_OVERFLOW;
        }
        pInAccessor->consumeTuple();
        producePending = false;
    }

    for (uint i = 0; i < quantum.nTuplesMax; i++) {
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        pInAccessor->accessConsumptionTuple();

        if (!samplingRng->nextValue()) {
            pInAccessor->consumeTuple();
            continue;
        }

        pInAccessor->getConsumptionTupleAccessor().unmarshal(data);

        producePending = true;
        if (!pOutAccessor->produceTuple(data)) {
            return EXECRC_BUF_OVERFLOW;
        }
        producePending = false;
        pInAccessor->consumeTuple();
    }

    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End BernoulliSamplingExecStream.cpp

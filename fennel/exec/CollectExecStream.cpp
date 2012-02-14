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
#include "fennel/exec/CollectExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/TuplePrinter.h"
#include "fennel/tuple/StandardTypeDescriptor.h"


FENNEL_BEGIN_CPPFILE("$Id$");

void CollectExecStream::prepare(CollectExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
    FENNEL_TRACE(
        TRACE_FINER,
        "collect xo input TupleDescriptor = "
        << pInAccessor->getTupleDesc());

    FENNEL_TRACE(
        TRACE_FINER,
        "collect xo output TupleDescriptor = "
        << pOutAccessor->getTupleDesc());

    StandardTypeDescriptorOrdinal ordinal =
        StandardTypeDescriptorOrdinal(
            pOutAccessor->getTupleDesc()[0].pTypeDescriptor->getOrdinal());
    assert(ordinal == STANDARD_TYPE_VARBINARY);
    assert(1 == pOutAccessor->getTupleDesc().size());
}

void CollectExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    outputTupleData.compute(pOutAccessor->getTupleDesc());
    inputTupleData.compute(pInAccessor->getTupleDesc());

    uint cbOutMaxsize =
        pOutAccessor->getConsumptionTupleAccessor().getMaxByteCount();
    pOutputBuffer.reset(new FixedBuffer[cbOutMaxsize]);
    bytesWritten = 0;
    alreadyWrittenToOutput = false;
}

void CollectExecStream::close()
{
    pOutputBuffer.reset();
    ConduitExecStream::closeImpl();
}

ExecStreamResult CollectExecStream::execute(ExecStreamQuantum const &quantum)
{
    if (!alreadyWrittenToOutput && (EXECBUF_EOS == pInAccessor->getState())) {
        outputTupleData[0].pData = pOutputBuffer.get();
        outputTupleData[0].cbData = bytesWritten;
        if (!pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        }
        alreadyWrittenToOutput = true;
    }

    ExecStreamResult rc = precheckConduitBuffers();
    if (EXECRC_YIELD != rc) {
        return rc;
    }

    for (uint nTuples = 0; nTuples < quantum.nTuplesMax; ++nTuples) {
        assert(!pInAccessor->isTupleConsumptionPending());
        if (!pInAccessor->demandData()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        pInAccessor->unmarshalTuple(inputTupleData);

#if 0
    TupleDescriptor statusDesc = pInAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, inputTupleData);
    std::cout << std::endl;
#endif

        // write one input tuple to the staging output buffer
        memcpy(
            pOutputBuffer.get() + bytesWritten,
            pInAccessor->getConsumptionStart(),
            pInAccessor->getConsumptionTupleAccessor().getCurrentByteCount());
        bytesWritten +=
            pInAccessor->getConsumptionTupleAccessor().getCurrentByteCount();
        pInAccessor->consumeTuple();
    }
    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End CollectExecStream.cpp

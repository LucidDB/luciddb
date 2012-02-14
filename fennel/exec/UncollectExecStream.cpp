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
#include "fennel/exec/UncollectExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/tuple/TuplePrinter.h"


FENNEL_BEGIN_CPPFILE("$Id$");

void UncollectExecStream::prepare(UncollectExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);

    FENNEL_TRACE(
        TRACE_FINER,
        "uncollect xo input TupleDescriptor = "
        << pInAccessor->getTupleDesc());

    FENNEL_TRACE(
        TRACE_FINER,
        "uncollect xo output TupleDescriptor = "
        << pOutAccessor->getTupleDesc());

    StandardTypeDescriptorOrdinal ordinal =
        StandardTypeDescriptorOrdinal(
            pInAccessor->getTupleDesc()[0].pTypeDescriptor->getOrdinal());
    assert(ordinal == STANDARD_TYPE_VARBINARY);
    assert(1 == pInAccessor->getTupleDesc().size());

    inputTupleData.compute(pInAccessor->getTupleDesc());
    outputTupleData.compute(pOutAccessor->getTupleDesc());
}


void UncollectExecStream::open(bool restart)
{
    ConduitExecStream::open(restart);
    bytesWritten = 0;
}

ExecStreamResult UncollectExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (EXECRC_YIELD != rc) {
        return rc;
    }

    if (!pInAccessor->demandData()) {
        return EXECRC_BUF_UNDERFLOW;
    }

    pInAccessor->unmarshalTuple(inputTupleData);

#if 0
    std::cout << "input tuple descriptor" << pInAccessor->getTupleDesc()
              << std::endl;
    std::cout << "input tuple = ";
    TupleDescriptor statusDesc = pInAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, inputTupleData);
    std::cout << std::endl;
#endif

    TupleAccessor& outTa = pOutAccessor->getScratchTupleAccessor();
    while (bytesWritten < inputTupleData[0].cbData) {
        // write one item in the input array to the output buffer
        outTa.setCurrentTupleBuf(inputTupleData[0].pData + bytesWritten);
        outTa.unmarshal(outputTupleData);
#if 0
    std::cout << "unmarshalling ouput tuple= ";
    TupleDescriptor statusDesc = pOutAccessor->getTupleDesc();
    TuplePrinter tuplePrinter;
    tuplePrinter.print(std::cout, statusDesc, outputTupleData);
    std::cout << std::endl;
#endif

        if (!pOutAccessor->produceTuple(outputTupleData)) {
            return EXECRC_BUF_OVERFLOW;
        }
        bytesWritten += outTa.getCurrentByteCount();
    }

    assert(pInAccessor->isTupleConsumptionPending());
    assert(bytesWritten == inputTupleData[0].cbData);
    pInAccessor->consumeTuple();

    return EXECRC_QUANTUM_EXPIRED;
}

FENNEL_END_CPPFILE("$Id$");

// End UncollectExecStream.cpp

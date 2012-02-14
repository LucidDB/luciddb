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
#include "fennel/exec/MockConsumerExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExecStreamResult MockConsumerExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamBufAccessor &inAccessor = *pInAccessor;
    switch (inAccessor.getState()) {
    case EXECBUF_EMPTY:
        inAccessor.requestProduction();
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_UNDERFLOW:
        return EXECRC_BUF_UNDERFLOW;
    case EXECBUF_EOS:
        recvEOS = true;
        return EXECRC_EOS;
    case EXECBUF_NONEMPTY:
    case EXECBUF_OVERFLOW:
        break;
    default:
        permFail("Bad state " << inAccessor.getState());
    }
    assert(inAccessor.isConsumptionPossible());

    // Read rows from the input buffer until we exceed the quantum or read all
    // of the rows. Convert each row to a string, and append to the rows
    // vector.
    for (uint iRow = 0; iRow < quantum.nTuplesMax; ++iRow) {
        if (!inAccessor.demandData()) {
            // Convert buf return code into stream return code.
            switch (inAccessor.getState()) {
            case EXECBUF_UNDERFLOW:
                return EXECRC_BUF_UNDERFLOW;
            case EXECBUF_EOS:
                return EXECRC_EOS;
            default:
                permAssert(false);
            }
        }
        inAccessor.unmarshalTuple(inputTuple);
        rowCount++;
        if (echoData) {
            tuplePrinter.print(
                *echoData, inAccessor.getTupleDesc(), inputTuple);
        }
        if (saveData) {
            std::ostringstream oss;
            tuplePrinter.print(oss, inAccessor.getTupleDesc(), inputTuple);
            const string &s = oss.str();
            rowStrings.push_back(s);
        }
        inAccessor.consumeTuple();
    }
    return EXECRC_QUANTUM_EXPIRED;
}

void MockConsumerExecStream::prepare(
    MockConsumerExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);
    saveData = params.saveData;
    echoData = params.echoData;
    recvEOS = false;
}

void MockConsumerExecStream::open(bool restart)
{
    SingleInputExecStream::open(restart);
    rowCount = 0;
    rowStrings.clear();
    inputTuple.compute(pInAccessor->getTupleDesc());
    recvEOS = false;
}


FENNEL_END_CPPFILE("$Id$");

// End MockConsumerExecStream.cpp

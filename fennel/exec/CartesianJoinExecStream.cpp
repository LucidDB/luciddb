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
#include "fennel/exec/CartesianJoinExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

#include <ostream>

FENNEL_BEGIN_CPPFILE("$Id$");

void CartesianJoinExecStream::prepare(
    CartesianJoinExecStreamParams const &params)
{
    assert(checkNumInputs());
    pLeftBufAccessor = inAccessors[0];
    assert(pLeftBufAccessor);

    pRightBufAccessor = inAccessors[1];
    assert(pRightBufAccessor);

    leftOuter = params.leftOuter;

    SharedExecStream pLeftInput = pGraph->getStreamInput(getStreamId(), 0);
    assert(pLeftInput);
    pRightInput = pGraph->getStreamInput(getStreamId(), 1);
    assert(pRightInput);
    FENNEL_TRACE(
        TRACE_FINE,
        "left input " << pLeftInput->getStreamId()
        << ' ' << pLeftInput->getName()
        << ", right input " << pRightInput->getStreamId()
        << ' ' << pRightInput->getName());


    TupleDescriptor const &leftDesc = pLeftBufAccessor->getTupleDesc();
    TupleDescriptor const &rightDesc = pRightBufAccessor->getTupleDesc();

    TupleDescriptor outputDesc;
    outputDesc.insert(outputDesc.end(), leftDesc.begin(), leftDesc.end());
    uint iFirstRight = outputDesc.size();
    outputDesc.insert(outputDesc.end(), rightDesc.begin(), rightDesc.end());
    if (leftOuter) {
        // Right side is null-generating; have to adjust tuple descriptor
        // accordingly.
        for (uint i = iFirstRight; i < outputDesc.size(); ++i) {
            outputDesc[i].isNullable = true;
        }
    }
    if (params.outputTupleDesc.size()) {
        assert(params.outputTupleDesc == outputDesc);
    }
    outputData.compute(outputDesc);
    pOutAccessor->setTupleShape(outputDesc);

    nLeftAttributes = leftDesc.size();

    ConfluenceExecStream::prepare(params);
}

bool CartesianJoinExecStream::checkNumInputs()
{
    return (inAccessors.size() == 2);
}

void CartesianJoinExecStream::open(bool restart)
{
    ConfluenceExecStream::open(restart);
}

// trace buffer state
inline std::ostream& operator<< (
    std::ostream& os, SharedExecStreamBufAccessor buf)
{
    os << ExecStreamBufState_names[buf->getState()];
    if (buf->hasPendingEOS()) {
        os << "(EOS pending)";
    }
    return os;
}

ExecStreamResult CartesianJoinExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    // TODO:  lots of small optimizations possible here

    // TODO jvs 6-Nov-2004: one big optimization would be to perform
    // buffer-to-buffer joins instead of row-to-buffer joins.  This would
    // reduce the number of times the right input needs to be iterated by the
    // average number of rows in a buffer from the left input.  However,  the
    // output ordering would also be affected, so we might want to provide a
    // parameter to control this behavior.

    // Also, for outer join, once we know the right input is empty, it's always
    // going to be empty for every left row, so we could just stop trying to
    // re-execute the right hand side.

    uint nTuplesProduced = 0;

    for (;;) {
        if (!pLeftBufAccessor->isTupleConsumptionPending()) {
            if (pLeftBufAccessor->getState() == EXECBUF_EOS) {
                pOutAccessor->markEOS();
                return EXECRC_EOS;
            }
            if (!pLeftBufAccessor->demandData()) {
                FENNEL_TRACE_THREAD(
                    TRACE_FINE,
                    "left underflow; left input " << pLeftBufAccessor
                    << " right input " << pRightBufAccessor);
                return EXECRC_BUF_UNDERFLOW;
            }
            pLeftBufAccessor->unmarshalTuple(outputData);
            processLeftInput();
            rightInputEmpty = true;
        }
        ExecStreamResult rc = preProcessRightInput();
        if (rc != EXECRC_YIELD) {
            return rc;
        }
        for (;;) {
            if (!pRightBufAccessor->isTupleConsumptionPending()) {
                if (pRightBufAccessor->getState() == EXECBUF_EOS) {
                    if (leftOuter && rightInputEmpty) {
                        // put null in outputdata to the right of
                        // nLeftAttributes
                        for (int i = nLeftAttributes;
                             i < outputData.size(); ++i)
                        {
                            outputData[i].pData = NULL;
                        }

                        if (pOutAccessor->produceTuple(outputData)) {
                            ++nTuplesProduced;
                        } else {
                            return EXECRC_BUF_OVERFLOW;
                        }

                        if (nTuplesProduced >= quantum.nTuplesMax) {
                            return EXECRC_QUANTUM_EXPIRED;
                        }
                    }

                    pLeftBufAccessor->consumeTuple();
                    // restart right input stream
                    pRightInput->open(true);
                    FENNEL_TRACE_THREAD(
                        TRACE_FINE,
                        "re-opened right input " << pRightBufAccessor);
                    // NOTE: break out of the inner for loop, which will take
                    // us back to the top of the outer for loop
                    break;
                }
                if (!pRightBufAccessor->demandData()) {
                    FENNEL_TRACE_THREAD(
                        TRACE_FINE,
                        "right underflow; left input " << pLeftBufAccessor
                        << " right input " << pRightBufAccessor);
                    return EXECRC_BUF_UNDERFLOW;
                }
                rightInputEmpty = false;
                pRightBufAccessor->unmarshalTuple(
                    outputData, nLeftAttributes);
                break;
            }

            if (pOutAccessor->produceTuple(outputData)) {
                ++nTuplesProduced;
            } else {
                return EXECRC_BUF_OVERFLOW;
            }

            pRightBufAccessor->consumeTuple();

            if (nTuplesProduced >= quantum.nTuplesMax) {
                return EXECRC_QUANTUM_EXPIRED;
            }
        }
    }
}

ExecStreamResult CartesianJoinExecStream::preProcessRightInput()
{
    return EXECRC_YIELD;
}

void CartesianJoinExecStream::processLeftInput()
{
}

FENNEL_END_CPPFILE("$Id$");

// End CartesianJoinExecStream.cpp

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
#include "fennel/exec/DiffluenceExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"
#include "fennel/exec/ExecStreamGraph.h"

FENNEL_BEGIN_CPPFILE("$Id$");

DiffluenceExecStreamParams::DiffluenceExecStreamParams()
{
    outputTupleFormat = TUPLE_FORMAT_STANDARD;
}

void DiffluenceExecStream::setOutputBufAccessors(
    std::vector<SharedExecStreamBufAccessor> const &outAccessorsInit)
{
    outAccessors = outAccessorsInit;
}

void DiffluenceExecStream::prepare(DiffluenceExecStreamParams const &params)
{
    SingleInputExecStream::prepare(params);

    // By default, shape for all outputs is the same as the input if the
    // outputTupleDesc wasn't explicitly set.
    TupleDescriptor tupleDesc;
    TupleFormat tupleFormat;
    if (params.outputTupleDesc.empty()) {
        tupleDesc = pInAccessor->getTupleDesc();
        tupleFormat = pInAccessor->getTupleFormat();
    } else {
        tupleDesc = params.outputTupleDesc;
        tupleFormat = params.outputTupleFormat;
    }
    for (uint i = 0; i < outAccessors.size(); ++i) {
        assert(outAccessors[i]->getProvision() == getOutputBufProvision());
        outAccessors[i]->setTupleShape(tupleDesc, tupleFormat);
    }
}

void DiffluenceExecStream::open(bool restart)
{
    SingleInputExecStream::open(restart);

    if (restart) {
        // restart outputs
        for (uint i = 0; i < outAccessors.size(); ++i) {
            outAccessors[i]->clear();
        }
    }
}

ExecStreamBufProvision DiffluenceExecStream::getOutputBufProvision() const
{
    /*
     * Indicate to the consumer that buffer should be provided by the consumer.
     * By default, DiffluenceExecStream does not have any associated buffers.
     */
    return BUFPROV_CONSUMER;
}


FENNEL_END_CPPFILE("$Id$");

// End DiffluenceExecStream.cpp

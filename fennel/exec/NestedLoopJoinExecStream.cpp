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
#include "fennel/exec/NestedLoopJoinExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void NestedLoopJoinExecStream::prepare(
    NestedLoopJoinExecStreamParams const &params)
{
    CartesianJoinExecStream::prepare(params);

    leftJoinKeys.assign(params.leftJoinKeys.begin(), params.leftJoinKeys.end());
    assert(leftJoinKeys.size() <= nLeftAttributes);
}

bool NestedLoopJoinExecStream::checkNumInputs()
{
    return (inAccessors.size() >= 2 && inAccessors.size() <= 3);
}

void NestedLoopJoinExecStream::open(bool restart)
{
    CartesianJoinExecStream::open(restart);

    if (!restart) {
        std::vector<NestedLoopJoinKey>::iterator it;
        for (it = leftJoinKeys.begin(); it != leftJoinKeys.end(); it++) {
            pDynamicParamManager->createParam(
                it->dynamicParamId,
                pLeftBufAccessor->getTupleDesc()[it->leftAttributeOrdinal]);
        }

        // Initialize this here and don't reset on restarts, since the
        // defined behavior is that the pre-processing is only done once
        // per stream graph execution, even if the stream is re-opened in
        // restart mode
        preProcessingDone = false;
    }
}

ExecStreamResult NestedLoopJoinExecStream::preProcessRightInput()
{
    // Create the temporary index by requesting production on the 3rd input
    if (!preProcessingDone && inAccessors.size() == 3) {
        if (inAccessors[2]->getState() != EXECBUF_EOS) {
            inAccessors[2]->requestProduction();
            return EXECRC_BUF_UNDERFLOW;
        }
    }
    preProcessingDone = true;
    return EXECRC_YIELD;
}

void NestedLoopJoinExecStream::processLeftInput()
{
    std::vector<NestedLoopJoinKey>::iterator it;
    for (it = leftJoinKeys.begin(); it != leftJoinKeys.end(); it++) {
        pDynamicParamManager->writeParam(
            it->dynamicParamId,
            outputData[it->leftAttributeOrdinal]);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End NestedLoopJoinExecStream.cpp

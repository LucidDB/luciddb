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
#include "fennel/ftrs/BTreeSearchUniqueExecStream.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSearchUniqueExecStream::prepare(
    BTreeSearchExecStreamParams const &params)
{
    // We don't allow directives for unique searches:  every input key
    // must be interpreted as a point search.
    assert(params.inputDirectiveProj.size() == 0);

    BTreeSearchExecStream::prepare(params);
}

ExecStreamResult BTreeSearchUniqueExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    uint nTuples = 0;
    assert(quantum.nTuplesMax > 0);

    // outer loop
    for (;;) {
        if (!innerSearchLoop()) {
            return EXECRC_BUF_UNDERFLOW;
        }

        if (nTuples >= quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }

        if (pOutAccessor->produceTuple(tupleData)) {
            ++nTuples;
        } else {
            return EXECRC_BUF_OVERFLOW;
        }

        pReader->endSearch();
        pInAccessor->consumeTuple();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSearchUniqueExecStream.cpp

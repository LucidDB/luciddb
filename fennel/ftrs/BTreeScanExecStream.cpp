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
#include "fennel/ftrs/BTreeScanExecStream.h"
#include "fennel/btree/BTreeReader.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeScanExecStream::prepare(BTreeScanExecStreamParams const &params)
{
    BTreeReadExecStream::prepare(params);
}

void BTreeScanExecStream::open(bool restart)
{
    BTreeReadExecStream::open(restart);
    if (!pReader->searchFirst()) {
        pReader->endSearch();
    }
}

ExecStreamResult BTreeScanExecStream::execute(ExecStreamQuantum const &quantum)
{
    // TODO: (under parameter control) unlock current leaf before return and
    // relock it on next fetch

    uint nTuples = 0;

    while (pReader->isPositioned()) {
        projAccessor.unmarshal(tupleData);
        if (pOutAccessor->produceTuple(tupleData)) {
            ++nTuples;
        } else {
            return EXECRC_BUF_OVERFLOW;
        }
        if (!pReader->searchNext()) {
            pReader->endSearch();
            break;
        }
        if (nTuples >= quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
    }

    if (!nTuples) {
        pOutAccessor->markEOS();
    }
    return EXECRC_EOS;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeScanExecStream.cpp

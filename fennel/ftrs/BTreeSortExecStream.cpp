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
#include "fennel/ftrs/BTreeSortExecStream.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BTreeSortExecStream::prepare(BTreeSortExecStreamParams const &params)
{
    assert(params.rootPageId == NULL_PAGE_ID);
    assert(!params.pRootMap);

    BTreeInsertExecStream::prepare(params);
    dynamicBTree = true;
    truncateOnRestart = true;
}

// REVIEW:  do we ever want to save results on restart?
void BTreeSortExecStream::open(bool restart)
{
    sorted = false;
    BTreeInsertExecStream::open(restart);
}

ExecStreamResult BTreeSortExecStream::execute(
    ExecStreamQuantum const &quantum)
{
    if (!sorted) {
        if (pInAccessor->getState() == EXECBUF_EOS) {
            sorted = true;
            bool found = pWriter->searchFirst();
            if (!found) {
                pWriter->endSearch();
            }
        } else {
            return BTreeInsertExecStream::execute(quantum);
        }
    }

    if (!pWriter->isPositioned()) {
        pOutAccessor->markEOS();
        return EXECRC_EOS;
    }

    if (pOutAccessor->getState() == EXECBUF_OVERFLOW) {
        return EXECRC_BUF_OVERFLOW;
    }

    uint nTuples = 0;
    TupleAccessor const &readAccessor = pWriter->getTupleAccessorForRead();

    do {
        uint cbBuffer = pOutAccessor->getProductionAvailable();
        PBuffer pBuffer = pOutAccessor->getProductionStart();
        uint cbTuple = readAccessor.getCurrentByteCount();
        if (cbBuffer < cbTuple) {
            pOutAccessor->requestConsumption();
            return EXECRC_BUF_OVERFLOW;
        }
        memcpy(
            pBuffer,
            readAccessor.getCurrentTupleBuf(),
            cbTuple);
        pOutAccessor->produceData(pBuffer + cbTuple);
        ++nTuples;
        if (!pWriter->searchNext()) {
            pWriter->endSearch();
        }
        if (nTuples >= quantum.nTuplesMax) {
            return EXECRC_QUANTUM_EXPIRED;
        }
    } while (pWriter->isPositioned());
    return EXECRC_EOS;
}

FENNEL_END_CPPFILE("$Id$");

// End BTreeSortExecStream.cpp

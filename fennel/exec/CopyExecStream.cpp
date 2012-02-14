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
#include "fennel/exec/CopyExecStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void CopyExecStream::prepare(CopyExecStreamParams const &params)
{
    ConduitExecStream::prepare(params);
}

ExecStreamResult CopyExecStream::execute(ExecStreamQuantum const &quantum)
{
    ExecStreamResult rc = precheckConduitBuffers();
    if (rc != EXECRC_YIELD) {
        return rc;
    }

    uint cbAvailableIn = pInAccessor->getConsumptionAvailable();
    uint cbAvailableOut = pOutAccessor->getProductionAvailable();


    if (cbAvailableOut < cbAvailableIn) {
        // With a DFS scheduler, the consumer must have room.
        // With a non-DFS scheduler, there may be no consumer buffer yet, or a
        // buffer too full to accept all (or any) of the input. Always copy
        // entire rows.
        cbAvailableIn =
            pInAccessor->getConsumptionAvailableBounded(cbAvailableOut);
    } else {
        rc = EXECRC_BUF_UNDERFLOW;
    }

    if (cbAvailableIn > 0) {
        PConstBuffer pSrc = pInAccessor->getConsumptionStart();
        PBuffer pDst = pOutAccessor->getProductionStart();
        memcpy(pDst, pSrc, cbAvailableIn);
        pInAccessor->consumeData(pSrc + cbAvailableIn);
        pOutAccessor->produceData(pDst + cbAvailableIn);
        // we can't use whatever's left in output buffer, so tell consumer
        // to give us a fresh one next time
        pOutAccessor->requestConsumption();
    }

    if ((rc == EXECRC_BUF_UNDERFLOW)
        && (pInAccessor->getState() != EXECBUF_EOS))
    {
        pInAccessor->requestProduction();
    }
    return EXECRC_BUF_OVERFLOW;
}

// TODO jvs 20-Nov-2006:  move this to ExecStreamBufAccessor.cpp
// once it exists
uint ExecStreamBufAccessor::getConsumptionAvailableBounded(uint cbLimit)
{
    if (cbLimit == 0) {
        return 0;
    }

    uint cbAvailable = getConsumptionAvailable();
    if (cbAvailable <= cbLimit) {
        return cbAvailable;
    }

    TupleAccessor const &tupleAccessor = getConsumptionTupleAccessor();
    PConstBuffer pSrc = getConsumptionStart();

    PConstBuffer pTuple = pSrc;
    PConstBuffer pTupleSafe = pTuple;
    PConstBuffer pEnd = pSrc + cbLimit;
    for (;;) {
        uint cbTuple = tupleAccessor.getBufferByteCount(pTuple);
        pTuple += cbTuple;
        if (pTuple > pEnd) {
            // this tuple would put us over the limit
            break;
        }
        // this tuple will fit
        pTupleSafe = pTuple;
    }
    return pTupleSafe - pSrc;
}

FENNEL_END_CPPFILE("$Id$");

// End CopyExecStream.cpp

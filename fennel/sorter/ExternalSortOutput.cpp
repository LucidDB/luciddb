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
#include "fennel/sorter/ExternalSortOutput.h"
#include "fennel/sorter/ExternalSortInfo.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/exec/ExecStreamBufAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExternalSortOutput::ExternalSortOutput(ExternalSortInfo &sortInfoIn)
    : sortInfo(sortInfoIn)
{
    pSubStream = NULL;
    pFetchArray = NULL;
    iCurrentTuple = 0;

    tupleAccessor.compute(sortInfo.tupleDesc);
}

ExternalSortOutput::~ExternalSortOutput()
{
    releaseResources();
}

void ExternalSortOutput::releaseResources()
{
}

void ExternalSortOutput::setSubStream(ExternalSortSubStream &subStream)
{
    iCurrentTuple = 0;

    pSubStream = &subStream;
    pFetchArray = &(subStream.bindFetchArray());
}

ExecStreamResult ExternalSortOutput::fetch(
    ExecStreamBufAccessor &bufAccessor)
{
    uint cbRemaining = bufAccessor.getProductionAvailable();
    PBuffer pOutBuf = bufAccessor.getProductionStart();
    PBuffer pNextTuple = pOutBuf;

    for (;;) {
        if (iCurrentTuple >= pFetchArray->nTuples) {
            ExternalSortRC rc = pSubStream->fetch(EXTSORT_FETCH_ARRAY_SIZE);
            if (rc == EXTSORT_ENDOFDATA) {
                goto done;
            }
            iCurrentTuple = 0;
        }

        while (iCurrentTuple < pFetchArray->nTuples) {
            PConstBuffer pSrcTuple =
                pFetchArray->ppTupleBuffers[iCurrentTuple];
            uint cbTuple = tupleAccessor.getBufferByteCount(pSrcTuple);
            if (cbTuple > cbRemaining) {
                if (pNextTuple == pOutBuf) {
                    bufAccessor.requestConsumption();
                    return EXECRC_BUF_OVERFLOW;
                }
                goto done;
            }
            memcpy(pNextTuple, pSrcTuple, cbTuple);
            cbRemaining -= cbTuple;
            pNextTuple += cbTuple;
            iCurrentTuple++;
        }
    }

 done:
    if (pNextTuple == pOutBuf) {
        return EXECRC_EOS;
    } else {
        bufAccessor.produceData(pNextTuple);
        bufAccessor.requestConsumption();
        // REVIEW:  sometimes should be EXECRC_EOS instead
        return EXECRC_BUF_OVERFLOW;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ExternalSortOutput.cpp

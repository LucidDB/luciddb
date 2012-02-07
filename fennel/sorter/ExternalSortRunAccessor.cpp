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
#include "fennel/sorter/ExternalSortRunAccessor.h"
#include "fennel/sorter/ExternalSortInfo.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/segment/SegStreamAllocation.h"
#include "fennel/exec/ExecStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExternalSortRunAccessor::ExternalSortRunAccessor(ExternalSortInfo &sortInfoIn)
    : sortInfo(sortInfoIn)
{
    releaseResources();

    tupleAccessor.compute(sortInfo.tupleDesc);
}

ExternalSortRunAccessor::~ExternalSortRunAccessor()
{
    releaseResources();
}

SharedSegStreamAllocation ExternalSortRunAccessor::getStoredRun()
{
    return pStoredRun;
}

void ExternalSortRunAccessor::startRead(
    SharedSegStreamAllocation pStoredRunInit)
{
    pStoredRun = pStoredRunInit;
    pStoredRun->getInputStream()->startPrefetch();
}

void ExternalSortRunAccessor::resetRead()
{
    fetchArray.nTuples = 0;
}

void ExternalSortRunAccessor::initRead()
{
    releaseResources();
    tupleAccessor.compute(sortInfo.tupleDesc);
}

void ExternalSortRunAccessor::releaseResources()
{
    pStoredRun.reset();
    clearFetch();
}

void ExternalSortRunAccessor::storeRun(
    ExternalSortSubStream &pObjLoad)
{
    pStoredRun = SegStreamAllocation::newSegStreamAllocation();

    SharedSegOutputStream pSegOutputStream =
        SegOutputStream::newSegOutputStream(
            sortInfo.externalSegmentAccessor);
    pStoredRun->beginWrite(pSegOutputStream);

    ExternalSortFetchArray &fetchArray = pObjLoad.bindFetchArray();

    ExternalSortRC rc;
    uint iTuple = 0;
    do {
        for (; iTuple < fetchArray.nTuples; iTuple++) {
            PBuffer pSrcBuf = fetchArray.ppTupleBuffers[iTuple];
            uint cbTuple = tupleAccessor.getBufferByteCount(pSrcBuf);
            PBuffer pTarget = pSegOutputStream->getWritePointer(cbTuple);
            memcpy(pTarget, pSrcBuf, cbTuple);
            pSegOutputStream->consumeWritePointer(cbTuple);
        }
        iTuple = 0;

        rc = pObjLoad.fetch(EXTSORT_FETCH_ARRAY_SIZE);
    } while (rc == EXTSORT_SUCCESS);

    assert(rc == EXTSORT_ENDOFDATA);

    pStoredRun->endWrite();
}

ExternalSortFetchArray &ExternalSortRunAccessor::bindFetchArray()
{
    return fetchArray;
}

ExternalSortRC ExternalSortRunAccessor::fetch(uint nTuplesRequested)
{
    sortInfo.stream.checkAbort();

    if (nTuplesRequested > EXTSORT_FETCH_ARRAY_SIZE) {
        nTuplesRequested = EXTSORT_FETCH_ARRAY_SIZE;
    }

    uint cb;
    SharedSegInputStream const &pSegInputStream = pStoredRun->getInputStream();
    PConstBuffer pStart = pSegInputStream->getReadPointer(1,&cb);
    PConstBuffer pBuf = pStart;
    if (!pBuf) {
        return EXTSORT_ENDOFDATA;
    }
    PConstBuffer pStopMark = pBuf + cb;
    uint cbTuple;

    fetchArray.nTuples = 0;
    while (nTuplesRequested-- && (pBuf < pStopMark)) {
        ppTupleBuffers[fetchArray.nTuples] = const_cast<PBuffer>(pBuf);
        cbTuple = tupleAccessor.getBufferByteCount(pBuf);
        pBuf += cbTuple;
        fetchArray.nTuples++;
    }
    pSegInputStream->consumeReadPointer(pBuf - pStart);

    return EXTSORT_SUCCESS;
}

FENNEL_END_CPPFILE("$Id$");

// End ExternalSortRunAccessor.cpp

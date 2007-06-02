/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2007 LucidEra, Inc.
// Copyright (C) 2005-2007 The Eigenbase Project
// Portions Copyright (C) 2004-2007 John V. Sichi
//
// This program is free software; you can redistribute it and/or modify it
// under the terms of the GNU General Public License as published by the Free
// Software Foundation; either version 2 of the License, or (at your option)
// any later version approved by The Eigenbase Project.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/sorter/ExternalSortRunAccessor.h"
#include "fennel/lucidera/sorter/ExternalSortInfo.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/segment/SegStreamAllocation.h"

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
        for ( ; iTuple < fetchArray.nTuples; iTuple++) {
            PBuffer pSrcBuf = fetchArray.ppTupleBuffers[iTuple];
            uint cbTuple = tupleAccessor.getBufferByteCount(pSrcBuf);
            PBuffer pTarget = pSegOutputStream->getWritePointer(cbTuple);
            memcpy(pTarget,pSrcBuf,cbTuple);
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

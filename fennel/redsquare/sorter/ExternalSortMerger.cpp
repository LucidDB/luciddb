/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2004-2005 Red Square, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
// Portions Copyright (C) 2004-2005 John V. Sichi
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
#include "fennel/redsquare/sorter/ExternalSortMerger.h"
#include "fennel/redsquare/sorter/ExternalSortInfo.h"
#include "fennel/redsquare/sorter/ExternalSortRunAccessor.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ExternalSortMerger::ExternalSortMerger(ExternalSortInfo &sortInfoIn)
    : sortInfo(sortInfoIn)
{
    nRuns = 0;

    tupleAccessor.compute(sortInfo.tupleDesc);
    tupleAccessor2.compute(sortInfo.tupleDesc);

    keyData.compute(sortInfo.keyDesc);
    keyData2 = keyData;

    keyAccessor.bind(tupleAccessor,sortInfo.keyProj);
    keyAccessor2.bind(tupleAccessor2,sortInfo.keyProj);
    
    nMergeMemPages = sortInfo.nSortMemPages + 1;

    ppRunAccessors.reset(new SharedExternalSortRunAccessor[nMergeMemPages]);
    ppFetchArrays.reset(new ExternalSortFetchArray *[nMergeMemPages]);

    pOrds.reset(new uint[nMergeMemPages]);
    memset(pOrds.get(),0,sizeof(uint) * nMergeMemPages);

    mergeInfo.reset(new ExternalSortMergeInfo[nMergeMemPages]);

    fetchArray.ppTupleBuffers = &(ppTupleBuffers[0]);
}

ExternalSortMerger::~ExternalSortMerger()
{
    releaseResources();
}

void ExternalSortMerger::initRunAccess()
{
    for (uint i = 1; i < nMergeMemPages; i++) {
        ppRunAccessors[i].reset(new ExternalSortRunAccessor(sortInfo));
        ppRunAccessors[i]->initRead();
        ppFetchArrays[i] = &(ppRunAccessors[i]->bindFetchArray());
    }
}

void ExternalSortMerger::releaseResources()
{
    ppRunAccessors.reset();
    ppFetchArrays.reset();
    pOrds.reset();
    mergeInfo.reset();
}

void ExternalSortMerger::startMerge(
    std::vector<ExternalSortStoredRun>::iterator pStoredRun,
    uint nRunsToMerge,bool subMerge)
{
    assert (nRunsToMerge < nMergeMemPages);

    nRuns = nRunsToMerge;

    for (uint i = 1; i < nMergeMemPages; i++) {
        ppRunAccessors[i]->resetRead();
        pOrds[i] = 0;
    }

    for (uint i = 1; i <= nRuns; i++) {
        ppRunAccessors[i]->startRead(
            pStoredRun[i - 1],
            subMerge);
        ppRunAccessors[i]->fetch(EXTSORT_FETCH_ARRAY_SIZE);
        mergeInfo[i].val = ppFetchArrays[i]->ppTupleBuffers[0];
        mergeInfo[i].runOrd = i;
    }
    fetchArray.nTuples = 0;

    heapBuild();
}

ExternalSortFetchArray &ExternalSortMerger::bindFetchArray()
{
    return fetchArray;
}

inline uint ExternalSortMerger::heapParent(uint i)
{
    return (i >> 1);
}

inline uint ExternalSortMerger::heapLeft(uint i)
{
    return (i << 1);
}

inline uint ExternalSortMerger::heapRight(uint i)
{
    return (i << 1) + 1;
}

inline void ExternalSortMerger::heapExchange(uint i,uint j)
{
    std::swap(mergeInfo[i],mergeInfo[j]);
}

inline ExternalSortMergeInfo &ExternalSortMerger::getMergeHigh()
{
    return mergeInfo[1];
}

void ExternalSortMerger::heapify(uint i)
{
    uint l, r, highest = i;

    l = heapLeft(i);
    r = heapRight(i);

    if (l <= nRuns) {
        tupleAccessor.setCurrentTupleBuf(mergeInfo[l].val);
        tupleAccessor2.setCurrentTupleBuf(mergeInfo[i].val);
        keyAccessor.unmarshal(keyData);
        keyAccessor2.unmarshal(keyData2);
        if (sortInfo.keyDesc.compareTuples(keyData,keyData2) < 0) {
            highest = l;
        }
    }

    if (r <= nRuns) {
        tupleAccessor.setCurrentTupleBuf(mergeInfo[r].val);
        tupleAccessor2.setCurrentTupleBuf(mergeInfo[highest].val);
        keyAccessor.unmarshal(keyData);
        keyAccessor2.unmarshal(keyData2);
        if (sortInfo.keyDesc.compareTuples(keyData,keyData2) < 0) {
            highest = r;
        }
    }
    
    if (highest != i) {
        heapExchange(highest,i);
        heapify(highest);
    }
}

void ExternalSortMerger::heapBuild()
{
    for (uint i = (nRuns / 2); i > 0; i--) {
        heapify(i);
    }
}

ExternalSortRC ExternalSortMerger::checkFetch()
{
    if (nRuns == 0) {
        return EXTSORT_ENDOFDATA;
    }
    if (pOrds[getMergeHigh().runOrd]
        >= ppFetchArrays[getMergeHigh().runOrd]->nTuples)
    {
        ExternalSortRC rc = ppRunAccessors[getMergeHigh().runOrd]->fetch(
            EXTSORT_FETCH_ARRAY_SIZE);
        if (rc != EXTSORT_SUCCESS) {
            assert(rc == EXTSORT_ENDOFDATA);
            heapExchange(1, nRuns);
            if (--nRuns == 0) {
                return EXTSORT_ENDOFDATA;
            }
        } else {
            pOrds[getMergeHigh().runOrd] = 0;
        }
    }

    return EXTSORT_SUCCESS;
}

ExternalSortRC ExternalSortMerger::fetch(uint nTuplesRequested)
{
    if (nTuplesRequested > EXTSORT_FETCH_ARRAY_SIZE) {
        nTuplesRequested = EXTSORT_FETCH_ARRAY_SIZE;
    }

    ExternalSortRC rc = checkFetch();
    if (rc != EXTSORT_SUCCESS) {
        // error handling will be done by checkFetch here
        return rc;
    }

    fetchArray.nTuples = 0;
    do {
        getMergeHigh().val =
            ppFetchArrays[getMergeHigh().runOrd]->ppTupleBuffers[
                pOrds[getMergeHigh().runOrd]];

        heapify(1);

        fetchArray.ppTupleBuffers[fetchArray.nTuples] = getMergeHigh().val;
        fetchArray.nTuples++;
        nTuplesRequested--;

        pOrds[getMergeHigh().runOrd]++;
    } while (nTuplesRequested
              && (pOrds[getMergeHigh().runOrd]
                  < ppFetchArrays[getMergeHigh().runOrd]->nTuples));

    return EXTSORT_SUCCESS;
}

FENNEL_END_CPPFILE("$Id$");

// End ExternalSortMerger.cpp

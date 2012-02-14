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

#ifndef Fennel_ExternalSortMerger_Included
#define Fennel_ExternalSortMerger_Included

#include "fennel/sorter/ExternalSortSubStream.h"
#include "fennel/tuple/TupleAccessor.h"
#include "fennel/tuple/TupleData.h"
#include "fennel/tuple/TupleProjectionAccessor.h"

#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * TODO:  doc
 */
struct ExternalSortMergeInfo
{
    PBuffer val;
    uint runOrd;

    explicit ExternalSortMergeInfo()
    {
        val = NULL;
        runOrd = 0;
    }
};

class ExternalSortInfo;
class ExternalSortRunAccessor;

typedef boost::shared_ptr<ExternalSortRunAccessor>
SharedExternalSortRunAccessor;

/**
 * ExternalSortMerger manages the process of merging stored runs.
 */
class FENNEL_SORTER_EXPORT ExternalSortMerger
    : public ExternalSortSubStream
{
    /**
     * Global information.
     */
    ExternalSortInfo &sortInfo;

    /**
     * Array of accessors for runs to be merged.
     */
    boost::scoped_array<SharedExternalSortRunAccessor> ppRunAccessors;

    /**
     * Array of fetch bindings corresponding to ppRunAccessors.
     */
    boost::scoped_array<ExternalSortFetchArray *> ppFetchArrays;

    /**
     * Array of something-or-others corresponding to ppRunAccessors.
     */
    boost::scoped_array<uint> pOrds;

    /**
     * Number of memory pages available for merging.
     */
    uint nMergeMemPages;

    /**
     * Number of runs being merged.
     */
    uint nRuns;

    /**
     * Array of info on runs being merged.
     */
    boost::scoped_array<ExternalSortMergeInfo> mergeInfo;

    /**
     * Array used to return fetch results.  This is permanently bound to
     * ppTupleBuffers.
     */
    ExternalSortFetchArray fetchArray;

    /**
     * Pointer array used to return fetch results.  These pointers
     * gather discontiguous tuples from run pages based on heap order.
     */
    PBuffer ppTupleBuffers[EXTSORT_FETCH_ARRAY_SIZE];

    // TODO:  comment or replace
    TupleAccessor tupleAccessor;
    TupleAccessor tupleAccessor2;
    TupleProjectionAccessor keyAccessor;
    TupleProjectionAccessor keyAccessor2;
    TupleData keyData;
    TupleData keyData2;

// ----------------------------------------------------------------------
// private methods
// ----------------------------------------------------------------------

    inline uint heapParent(uint i);
    inline uint heapLeft(uint i);
    inline uint heapRight(uint i);
    inline void heapExchange(uint i, uint j);
    void heapify(uint i);
    void heapBuild();

    // TODO:  doc
    ExternalSortRC checkFetch();
    inline ExternalSortMergeInfo &getMergeHigh();

public:
    explicit ExternalSortMerger(ExternalSortInfo &info);
    virtual ~ExternalSortMerger();

    /**
     * Initializes state used to access stored runs.
     */
    void initRunAccess();

    /**
     * Begins merge.
     *
     * @param pStoredRun iterator to first run to merge
     *
     * @param nRunsToMerge number of runs to merge
     */
    void startMerge(
        std::vector<SharedSegStreamAllocation>::iterator pStoredRun,
        uint nRunsToMerge);

    /**
     * Releases any resources acquired by this merger.
     */
    void releaseResources();

    // implement ExternalSortSubStream
    virtual ExternalSortFetchArray &bindFetchArray();
    virtual ExternalSortRC fetch(uint nTuplesRequested);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortMerger.h

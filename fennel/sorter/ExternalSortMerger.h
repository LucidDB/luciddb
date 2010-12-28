/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2009 SQLstream, Inc.
// Copyright (C) 2004 Dynamo BI Corporation
// Portions Copyright (C) 2004 John V. Sichi
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

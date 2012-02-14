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

#ifndef Fennel_ExternalSortRunAccessor_Included
#define Fennel_ExternalSortRunAccessor_Included

#include "fennel/tuple/TupleAccessor.h"
#include "fennel/sorter/ExternalSortSubStream.h"

FENNEL_BEGIN_NAMESPACE

class ExternalSortInfo;

/**
 * ExternalSortRunAccessor manages I/O for storing runs and reading them back.
 */
class FENNEL_SORTER_EXPORT ExternalSortRunAccessor
    : public ExternalSortSubStream
{
    /**
     * Global information.
     */
    ExternalSortInfo &sortInfo;

    // TODO:  comment or replace
    TupleAccessor tupleAccessor;

    /**
     * Array used to return fetch results.  This is permanently bound to
     * ppTupleBuffers.
     */
    ExternalSortFetchArray fetchArray;

    /**
     * Pointer array used to return fetch results.  These pointers get bound to
     * contiguous tuples on stored run pages as they are read in.
     */
    PBuffer ppTupleBuffers[EXTSORT_FETCH_ARRAY_SIZE];

    /**
     * Helper used for reading stored runs.
     */
    SharedSegInputStream pSegInputStream;

    /**
     * Helper used for writing stored runs.
     */
    SharedSegOutputStream pSegOutputStream;

    /**
     * Information about run being accessed.
     */
    SharedSegStreamAllocation pStoredRun;

// ----------------------------------------------------------------------
// private methods
// ----------------------------------------------------------------------

    void clearFetch()
    {
        fetchArray.nTuples = 0;
        fetchArray.ppTupleBuffers = ppTupleBuffers;
        memset(ppTupleBuffers, 0, sizeof(ppTupleBuffers));
    }

public:
    explicit ExternalSortRunAccessor(ExternalSortInfo &);
    virtual ~ExternalSortRunAccessor();

    /**
     * Prepares this accessor to read (but does not specify a particular
     * stored run yet).
     */
    void initRead();

    /**
     * Begins reading a particular run.
     *
     * @param pStoredRunInit run to read
     */
    void startRead(
        SharedSegStreamAllocation pStoredRunInit);

    /**
     * Terminates read for the current run if any.
     */
    void resetRead();

    /**
     * Stores a run.
     *
     * @param subStream substream whose contents are to be fetched
     * and stored as a run
     */
    void storeRun(ExternalSortSubStream &subStream);

    /**
     * @return information about run created by storeRun()
     */
    SharedSegStreamAllocation getStoredRun();

    /**
     * Releases any resources acquired by this accessor.
     */
    void releaseResources();

    // implement ExternalSortSubStream
    virtual ExternalSortFetchArray &bindFetchArray();
    virtual ExternalSortRC fetch(uint nTuplesRequested);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortRunAccessor.h

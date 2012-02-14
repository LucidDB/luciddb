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

#ifndef Fennel_ExternalSortInfo_Included
#define Fennel_ExternalSortInfo_Included

#include "fennel/tuple/TupleDescriptor.h"
#include "fennel/segment/SegmentAccessor.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

/**
 * Global information shared with sorter sub-components.
 */
struct ExternalSortInfo
{
    /**
     * Main stream, for abort checking.  Note that we intentionally
     * don't give subcomponents access to ExternalSortExecStreamImpl
     * details.
     */
    ExecStream &stream;

    /**
     * Accessor for segment used to store runs externally.
     */
    SegmentAccessor externalSegmentAccessor;

    /**
     * Accessor for scratch segment used for building runs in-memory.
     */
    SegmentAccessor memSegmentAccessor;

    /**
     * Descriptor for tuples to be sorted.
     */
    TupleDescriptor tupleDesc;

    /**
     * Projection of sort keys from tupleDesc.
     */
    TupleProjection keyProj;

    /**
     * @see ExternalSortExecStreamParams
     */
    std::vector<bool> descendingKeyColumns;

    /**
     * Descriptor for projected sort key tuples.
     */
    TupleDescriptor keyDesc;

    /**
     * Maximum number of memory pages available for sorting.
     */
    uint nSortMemPages;

    /**
     * Maximum number of memory pages to use for indexing keys to
     * keep things cache-conscious.
     */
    uint nIndexMemPages;

    /**
     * Maximum number of memory pages which can be filled per run.
     */
    uint nSortMemPagesPerRun;

    /**
     * Number of bytes per memory page.  Must be a power of 2.
     */
    uint cbPage;

    /**
     * The number of leading key columns which are already sorted or
     * partitioned. The XO will sort rows by trailing key columns for each
     * "partition" of rows. If 0, sort the entire input by sortKey.
     */
    uint partitionKeyCount;


    explicit ExternalSortInfo(ExecStream &);

    /**
     * Compares two keys, taking ASC/DESC into account.
     *
     * @param key1 first key to compare
     *
     * @param key2 second key to compare
     *
     * @return negative for key1 < key2; zero for key1 == key2;
     * positive for key1 > key2
     */
    int compareKeys(TupleData const &key1, TupleData const &key2);
};

FENNEL_END_NAMESPACE

#endif

// End ExternalSortInfo.h

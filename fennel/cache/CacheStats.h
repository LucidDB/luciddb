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

#ifndef Fennel_CacheStats_Included
#define Fennel_CacheStats_Included

FENNEL_BEGIN_NAMESPACE

/**
 * CacheStats defines performance/activity statistics collected by the cache;
 * these can be obtained as a snapshot from Cache::collectStats().
 */
class FENNEL_CACHE_EXPORT CacheStats
{
public:
    /**
     * Number of times a page access was satisfied without a disk
     * read (since last snapshot).
     */
    uint nHits;

    /**
     * Number of times a page access was satisfied without a disk
     * read (since cache initialization).
     */
    uint nHitsSinceInit;

    /**
     * Number of page accesses (since last snapshot).
     */
    uint nRequests;

    /**
     * Number of page accesses (since cache initialization).
     */
    uint nRequestsSinceInit;

    /**
     * Number of times a page had to be discarded to satisfy a request for
     * another page (since last snapshot).
     */
    uint nVictimizations;

    /**
     * Number of times a page had to be discarded to satisfy a request for
     * another page (since cache initialization).
     */
    uint nVictimizationsSinceInit;

    /**
     * Number of dirty pages (instantaneous).
     */
    uint nDirtyPages;

    /**
     * Number of disk pages read (since last snapshot).
     */
    uint nPageReads;

    /**
     * Number of disk pages read (since cache initialization).
     */
    uint nPageReadsSinceInit;

    /**
     * Number of disk pages written (since last snapshot).
     */
    uint nPageWrites;

    /**
     * Number of disk pages written (since cache initialization).
     */
    uint nPageWritesSinceInit;

    /**
     * Number of rejected cache pre-fetch requests (since last snapshot).
     */
    uint nRejectedPrefetches;

    /**
     * Number of rejected cache pre-fetch requests (since cache initialization).
     */
    uint nRejectedPrefetchesSinceInit;

    /**
     * Number of I/O requests requiring retry (since last snapshot).
     */
    uint nIoRetries;

    /**
     * Number of I/O requests requiring retry (since cache initialization).
     */
    uint nIoRetriesSinceInit;

    /**
     * Number of successful cache pre-fetch requests (since last snapshot).
     */
    uint nSuccessfulPrefetches;

    /**
     * Number of successful cache pre-fetch requests (since cache
     * initialization).
     */
    uint nSuccessfulPrefetchesSinceInit;

    /**
     * Number of lazy cache page writes (since last snapshot).
     */
    uint nLazyWrites;

    /**
     * Number of lazy cache page writes (since last initialization).
     */
    uint nLazyWritesSinceInit;

    /**
     * Number of lazy write calls that encountered at least one dirty page
     * (since last snapshot);
     */
    uint nLazyWriteCalls;

    /**
     * Number of lazy write calls that encountered at least one dirty page
     * (since initialization);
     */
    uint nLazyWriteCallsSinceInit;

    /**
     * Number of cache page writes during page victimization (since last
     * snapshot).
     */
    uint nVictimizationWrites;

    /**
     * Number of lazy cache page writes during victimizations (since last
     * initialization).
     */
    uint nVictimizationWritesSinceInit;

    /**
     * Number of cache page writes during checkpoint (since last snapshot).
     */
    uint nCheckpointWrites;

    /**
     * Number of cache page writes during checkpoint (since initialization).
     */
    uint nCheckpointWritesSinceInit;

    /**
     * Number of memory pages currently allocated in buffer pool
     * (instantaneous).
     */
    uint nMemPagesAllocated;

    /**
     * Number of memory pages currently allocated but unused
     * (instantaneous).
     */
    uint nMemPagesUnused;

    /**
     * Maximum number of memory pages which can be allocated in buffer pool
     * (immutable after cache initialization).
     */
    uint nMemPagesMax;
};

FENNEL_END_NAMESPACE

#endif

// End CacheStats.h

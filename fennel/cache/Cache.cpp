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
#include "fennel/cache/CacheImpl.h"
#include "fennel/cache/LRUVictimPolicy.h"
#include "fennel/cache/TwoQVictimPolicy.h"
#include "fennel/common/StatsTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

const DeviceId Cache::NULL_DEVICE_ID = DeviceId(0);

Cache::~Cache()
{
}

CacheAccessor::~CacheAccessor()
{
}

class TwoQPage : public CachePage, public TwoQVictim
{
public:
    TwoQPage(Cache &cache,PBuffer buffer)
        : CachePage(cache, buffer)
    {
    }
};

SharedCache Cache::newCache(
    CacheParams const &cacheParams,
    CacheAllocator *bufferAllocator)
{
    typedef CacheImpl<
        TwoQPage,
        TwoQVictimPolicy<TwoQPage>
        > TwoQCache;
    return SharedCache(
        new TwoQCache(cacheParams, bufferAllocator),
        ClosableObjectDestructor());
}

SharedCache Cache::getCache()
{
    return shared_from_this();
}

uint Cache::getMaxLockedPages()
{
    return getAllocatedPageCount();
}

void Cache::setMaxLockedPages(uint)
{
}

void Cache::setTxnId(TxnId)
{
}

TxnId Cache::getTxnId() const
{
    return IMPLICIT_TXN_ID;
}

void Cache::writeStats(StatsTarget &target)
{
    CacheStats stats;
    collectStats(stats);
    target.writeCounter(
        "CacheHits", stats.nHits);
    target.writeCounter(
        "CacheHitsSinceInit", stats.nHitsSinceInit);
    target.writeCounter(
        "CacheRequests", stats.nRequests);
    target.writeCounter(
        "CacheRequestsSinceInit", stats.nRequestsSinceInit);
    target.writeCounter(
        "CacheVictimizations", stats.nVictimizations);
    target.writeCounter(
        "CacheVictimizationsSinceInit", stats.nVictimizationsSinceInit);
    target.writeCounter(
        "CacheDirtyPages", stats.nDirtyPages);
    target.writeCounter(
        "CachePagesRead", stats.nPageReads);
    target.writeCounter(
        "CachePagesReadSinceInit", stats.nPageReadsSinceInit);
    target.writeCounter(
        "CachePagesWritten", stats.nPageWrites);
    target.writeCounter(
        "CachePagesWrittenSinceInit", stats.nPageWritesSinceInit);
    target.writeCounter(
        "CachePagePrefetchesRejected", stats.nRejectedPrefetches);
    target.writeCounter(
        "CachePagePrefetchesRejectedSinceInit",
        stats.nRejectedPrefetchesSinceInit);
    target.writeCounter(
        "CachePageIoRetries", stats.nIoRetries);
    target.writeCounter(
        "CachePageIoRetriesSinceInit",
        stats.nIoRetriesSinceInit);
    target.writeCounter(
        "CachePagesPrefetched", stats.nSuccessfulPrefetches);
    target.writeCounter(
        "CachePagesPrefetchedSinceInit",
        stats.nSuccessfulPrefetchesSinceInit);
    target.writeCounter("CacheLazyWrites", stats.nLazyWrites);
    target.writeCounter("CacheLazyWritesSinceInit", stats.nLazyWritesSinceInit);
    target.writeCounter("CacheLazyWriteCalls", stats.nLazyWriteCalls);
    target.writeCounter(
        "CacheLazyWriteCallsSinceInit",
        stats.nLazyWriteCallsSinceInit);
    target.writeCounter("CacheVictimizationWrites", stats.nVictimizationWrites);
    target.writeCounter(
        "CacheVictimizationWritesSinceInit",
        stats.nVictimizationWritesSinceInit);
    target.writeCounter("CacheCheckpointWrites", stats.nCheckpointWrites);
    target.writeCounter(
        "CacheCheckpointWritesSinceInit",
        stats.nCheckpointWritesSinceInit);
    target.writeCounter(
        "CachePagesAllocated", stats.nMemPagesAllocated);
    target.writeCounter(
        "CachePagesUnused", stats.nMemPagesUnused);
    target.writeCounter(
        "CachePagesAllocationLimit", stats.nMemPagesMax);
}

// force references to some classes which aren't referenced elsewhere
#ifdef __MSVC__
class UnreferencedCacheStructs
{
    LRUVictim lruVictim;
};
#endif

FENNEL_END_CPPFILE("$Id$");

// End Cache.cpp

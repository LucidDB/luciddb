/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/cache/CacheStats.h"
#include "fennel/cache/LRUVictimPolicy.h"
#include "fennel/common/StatsTarget.h"

FENNEL_BEGIN_CPPFILE("$Id$");

const DeviceId Cache::NULL_DEVICE_ID = DeviceId(0);

Cache::~Cache()
{
}

CacheAccessor::~CacheAccessor()
{
}

class LRUPage : public CachePage, public LRUVictim
{
public:
    LRUPage(Cache &cache,PBuffer buffer)
        : CachePage(cache,buffer)
    {
    }
};

SharedCache Cache::newCache(
    CacheParams const &cacheParams,
    CacheAllocator *bufferAllocator)
{
    typedef CacheImpl<
        LRUPage,
        LRUVictimPolicy<LRUPage>
        > LRUCache;
    return SharedCache(
        new LRUCache(cacheParams,bufferAllocator),
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
    target.writeCounter("CacheHits",stats.nHits);
    target.writeCounter("CacheRequests",stats.nRequests);
    target.writeCounter("CacheVictimizations",stats.nVictimizations);
    target.writeCounter("CacheDirtyPages",stats.nDirtyPages);
    target.writeCounter("CachePagesRead",stats.nPageReads);
    target.writeCounter("CachePagesWritten",stats.nPageWrites);
}

FENNEL_END_CPPFILE("$Id$");

// End Cache.cpp

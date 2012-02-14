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
#include "fennel/segment/ScratchSegment.h"
#include "fennel/segment/ScratchMemExcn.h"
#include "fennel/cache/Cache.h"
#include "fennel/common/FennelExcn.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ScratchSegment::ScratchSegment(
    SharedCache pCache,
    uint nPagesMaxInit)
    : Segment(pCache)
{
    nPagesMax = nPagesMaxInit;
}

void ScratchSegment::closeImpl()
{
    // this overrides Segment::closeImpl; we know exactly which pages to
    // discard, so we can skip the full-cache sweep
    clearPages();
}

void ScratchSegment::clearPages()
{
    // TODO:  separate unlockScratchPage method in Cache

    for (PageListIter ppPage = pages.begin(); ppPage != pages.end(); ++ppPage) {
        CachePage &page = **ppPage;
        getCache()->unlockPage(page, LOCKMODE_X);
    }
    pages.clear();
}

BlockId ScratchSegment::translatePageId(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    BlockId blockId(0);
    CompoundId::setDeviceId(blockId, Cache::NULL_DEVICE_ID);
    CompoundId::setBlockNum(
        blockId,
        getLinearBlockNum(pageId));
    return blockId;
}

PageId ScratchSegment::translateBlockId(BlockId blockId)
{
    return getLinearPageId(CompoundId::getBlockNum(blockId));
}

BlockNum ScratchSegment::getAllocatedSizeInPages()
{
    return pages.size();
}

BlockNum ScratchSegment::getNumPagesOccupiedHighWater()
{
    return getAllocatedSizeInPages();
}

BlockNum ScratchSegment::getNumPagesExtended()
{
    return BlockNum(0);
}

PageId ScratchSegment::allocatePageId(PageOwnerId)
{
    StrictMutexGuard mutexGuard(mutex);

    // nothing to do with PageOwnerId

    if (getAllocatedSizeInPages() >= nPagesMax) {
        return NULL_PAGE_ID;
    }

    BlockNum blockNum = pages.size();
    CachePage &page = getCache()->lockScratchPage(blockNum);
    pages.push_back(&page);
    return getLinearPageId(blockNum);
}

void ScratchSegment::deallocatePageRange(PageId startPageId, PageId endPageId)
{
    assert(startPageId == NULL_PAGE_ID);
    assert(endPageId == NULL_PAGE_ID);

    StrictMutexGuard mutexGuard(mutex);
    clearPages();
}

bool ScratchSegment::isPageIdAllocated(PageId pageId)
{
    return isLinearPageIdAllocated(pageId);
}

PageId ScratchSegment::getPageSuccessor(PageId pageId)
{
    return getLinearPageSuccessor(pageId);
}

void ScratchSegment::setPageSuccessor(PageId pageId, PageId successorId)
{
    setLinearPageSuccessor(pageId, successorId);
}

Segment::AllocationOrder ScratchSegment::getAllocationOrder() const
{
    return LINEAR_ALLOCATION;
}

CachePage *ScratchSegment::lockPage(
    BlockId blockId,
    LockMode lockMode,
    bool readIfUnmapped,
    MappedPageListener *pMappedPageListener,
    TxnId txnId)
{
    StrictMutexGuard mutexGuard(mutex);

    assert(CompoundId::getDeviceId(blockId) == Cache::NULL_DEVICE_ID);
    BlockNum blockNum = CompoundId::getBlockNum(blockId);
    assert(blockNum < pages.size());
    // TODO:  should assert(pMappedPageListener == this), but that doesn't work
    // when tracing is enabled
    return pages[blockNum];
}

void ScratchSegment::unlockPage(
    CachePage &,
    LockMode,
    TxnId)
{
    // ignore; pages remain locked until segment is closed
}

// REVIEW:  should assert on some of the following currently ignored?

void ScratchSegment::discardPage(
    BlockId)
{
}

bool ScratchSegment::prefetchPage(
    BlockId,
    MappedPageListener *)
{
    return false;
}

void ScratchSegment::prefetchBatch(
    BlockId, uint,
    MappedPageListener *)
{
}

void ScratchSegment::flushPage(CachePage &,bool)
{
}

void ScratchSegment::nicePage(CachePage &)
{
    // ignore
}

SharedCache ScratchSegment::getCache()
{
    return Segment::getCache();
}

uint ScratchSegment::getMaxLockedPages()
{
    if (isMAXU(nPagesMax)) {
        return getCache()->getMaxLockedPages();
    } else {
        return nPagesMax;
    }
}

void ScratchSegment::setMaxLockedPages(uint nPages)
{
    StrictMutexGuard mutexGuard(mutex);
    assert(nPages >= pages.size());
    nPagesMax = nPages;
}

void ScratchSegment::setTxnId(TxnId)
{
}

TxnId ScratchSegment::getTxnId() const
{
    return IMPLICIT_TXN_ID;
}

void ScratchSegment::getPrefetchParams(
    uint &nPagesPerBatch,
    uint &nBatchPrefetches)
{
}

uint ScratchSegment::getProcessorCacheBytes()
{
    return getCache()->getProcessorCacheBytes();
}

FENNEL_END_CPPFILE("$Id$");

// End ScratchSegment.cpp

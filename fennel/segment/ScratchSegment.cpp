/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/segment/ScratchSegment.h"
#include "fennel/cache/Cache.h"

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
        getCache()->unlockPage(page,LOCKMODE_X);
    }
    pages.clear();
}

BlockId ScratchSegment::translatePageId(PageId pageId)
{
    assert(isPageIdAllocated(pageId));
    BlockId blockId;
    CompoundId::setDeviceId(blockId,Cache::NULL_DEVICE_ID);
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

PageId ScratchSegment::allocatePageId(PageOwnerId)
{
    StrictMutexGuard mutexGuard(mutex);
    
    // nothing to do with PageOwnerId
    
    if (getAllocatedSizeInPages() >= nPagesMax) {
        return NULL_PAGE_ID;
    }

    BlockNum blockNum = pages.size();
    CachePage *pPage = getCache()->lockScratchPage(blockNum);
    pages.push_back(pPage);
    return getLinearPageId(blockNum);
}

void ScratchSegment::deallocatePageRange(PageId startPageId,PageId endPageId)
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

void ScratchSegment::setPageSuccessor(PageId pageId,PageId successorId)
{
    setLinearPageSuccessor(pageId,successorId);
}

Segment::AllocationOrder ScratchSegment::getAllocationOrder() const
{
    return LINEAR_ALLOCATION;
}

CachePage *ScratchSegment::lockPage(
    BlockId blockId,
    LockMode lockMode,
    bool readIfUnmapped,
    MappedPageListener *pMappedPageListener)
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
    LockMode)
{
    // ignore; pages remain locked until segment is closed
}

// REVIEW:  should assert on some of the following currently ignored?

void ScratchSegment::discardPage(
    BlockId)
{
}

void ScratchSegment::prefetchPage(
    BlockId,
    MappedPageListener *)
{
}

void ScratchSegment::prefetchBatch(
    BlockId,uint,
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

FENNEL_END_CPPFILE("$Id$");

// End ScratchSegment.cpp

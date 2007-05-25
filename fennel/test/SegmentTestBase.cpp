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
#include "fennel/test/SegmentTestBase.h"
#include "fennel/segment/LinearDeviceSegment.h"
#include "fennel/segment/SegPageLock.h"

#ifdef HAVE_SCHED_H
#include <sched.h>
#endif

#include <boost/test/test_tools.hpp>

using namespace fennel;

void SegmentTestBase::openStorage(DeviceMode openMode)
{
    SegStorageTestBase::openStorage(openMode);
    cbPageUsable = pLinearSegment->getUsablePageSize();
    if (!pRandomSegment) {
        threadCounts[OP_ALLOCATE] = 0;
        threadCounts[OP_DEALLOCATE] = 0;
    }
}

CachePage *SegmentTestBase::lockPage(OpType opType,uint iPage)
{
    SegmentAccessor segmentAccessor(pLinearSegment,pCache);
    SegPageLock pageLock(segmentAccessor);
    if (opType == OP_ALLOCATE) {
        PageId pageId = pageLock.allocatePage(objId);
        assert(Segment::getLinearBlockNum(pageId) == iPage);
        CachePage &page = pageLock.getPage();
        fillPage(page,iPage);
        pageLock.dontUnlock();
        return &page;
    } else {
        PageId pageId = Segment::getLinearPageId(iPage);
        // Prepare the page for update before locking it
        if (opType == OP_WRITE_SEQ || opType == OP_WRITE_RAND ||
            opType == OP_WRITE_SKIP)
        {
            pLinearSegment->updatePage(pageId, true);
        }
        pageLock.lockPage(pageId,getLockMode(opType));
        CachePage *pPage = pageLock.isLocked() ? &(pageLock.getPage()) : NULL;
        pageLock.dontUnlock();
        return pPage;
    }
}

void SegmentTestBase::unlockPage(CachePage &page,LockMode lockMode)
{
    getCache().unlockPage(page,lockMode);
}

void SegmentTestBase::prefetchPage(uint iPage)
{
    PageId pageId = Segment::getLinearPageId(iPage);
    BlockId blockId = pLinearSegment->translatePageId(pageId);
    getCache().prefetchPage(blockId,pLinearSegment.get());
}

void SegmentTestBase::prefetchBatch(uint,uint)
{
    permAssert(false);
}

void SegmentTestBase::testAllocate()
{
    assert(pRandomSegment);

    uint i;
    SegmentAccessor segmentAccessor(pRandomSegment,pCache);
    for (i = 0; i < nRandomOps; ++i) {
#ifdef HAVE_SCHED_H
        sched_yield();
#else
        // TODO:  call Mingw equivalent
#endif
        SegPageLock pageLock(segmentAccessor);
        PageId pageId = pageLock.tryAllocatePage(objId);
        if (pageId == NULL_PAGE_ID) {
            break;
        }
        pageLock.unlock();
        StrictMutexGuard freeablePagesGuard(freeablePagesMutex);
        freeablePages.push_back(pageId);
    }
    if (i) {
        StrictMutexGuard logGuard(logMutex);
        BOOST_MESSAGE("completed " << i << " allocate ops");
    }
}

void SegmentTestBase::testDeallocate()
{
    assert(pRandomSegment);
        
    uint i;
    SegmentAccessor segmentAccessor(pRandomSegment,pCache);
    for (i = 0; i < nRandomOps; ++i) {
#ifdef HAVE_SCHED_H
        sched_yield();
#else
        // TODO:  call Mingw equivalent
#endif
        StrictMutexGuard freeablePagesGuard(freeablePagesMutex);
        if (freeablePages.empty()) {
            break;
        }
        uint i = generateRandomNumber(freeablePages.size());
        PageId pageId = freeablePages[i];
        freeablePages.erase(freeablePages.begin() + i);
        freeablePagesGuard.unlock();
        SegPageLock pageLock(segmentAccessor);
        pageLock.lockShared(pageId);
        pageLock.deallocateLockedPage();
    }
    if (i) {
        StrictMutexGuard logGuard(logMutex);
        BOOST_MESSAGE("completed " << i << " deallocate ops");
    }
}

void SegmentTestBase::testCheckpoint()
{
    pLinearSegment->checkpoint(CHECKPOINT_FLUSH_ALL);
}

SegmentTestBase::SegmentTestBase()
{
    // disable irrelevant threads
    threadCounts[OP_SCRATCH] = 0;
    threadCounts[OP_PREFETCH_BATCH] = 0;

    objId = ANON_PAGE_OWNER_ID;
}

void SegmentTestBase::testSingleThread()
{
    openStorage(DeviceMode::createNew);
    testAllocateAll();
    testSequentialRead();
    testSequentialWrite();
    testRandomRead();
    closeStorage();
    openStorage(DeviceMode::load);
    testRandomRead();
    testRandomWrite();
    testSequentialRead();
    closeStorage();
    freeablePages.clear();
}

// End SegmentTestBase.cpp

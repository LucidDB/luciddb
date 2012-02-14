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
    SegmentAccessor segmentAccessor(pLinearSegment, pCache);
    SegPageLock pageLock(segmentAccessor);
    if (opType == OP_ALLOCATE) {
        PageId pageId = pageLock.allocatePage(objId);
        assert(Segment::getLinearBlockNum(pageId) == iPage);
        CachePage &page = pageLock.getPage();
        fillPage(page, iPage);
        pageLock.dontUnlock();
        return &page;
    } else {
        PageId pageId = Segment::getLinearPageId(iPage);
        // Prepare the page for update before locking it
        if (opType == OP_WRITE_SEQ || opType == OP_WRITE_RAND
            || opType == OP_WRITE_SKIP)
        {
            pLinearSegment->updatePage(pageId, true);
        }
        pageLock.lockPage(pageId, getLockMode(opType));
        CachePage *pPage = pageLock.isLocked() ? &(pageLock.getPage()) : NULL;
        pageLock.dontUnlock();
        return pPage;
    }
}

void SegmentTestBase::unlockPage(CachePage &page,LockMode lockMode)
{
    getCache().unlockPage(page, lockMode);
}

void SegmentTestBase::prefetchPage(uint iPage)
{
    PageId pageId = Segment::getLinearPageId(iPage);
    BlockId blockId = pLinearSegment->translatePageId(pageId);
    getCache().prefetchPage(blockId, pLinearSegment.get());
}

void SegmentTestBase::prefetchBatch(uint, uint)
{
    permAssert(false);
}

void SegmentTestBase::testAllocate()
{
    assert(pRandomSegment);

    uint i;
    SegmentAccessor segmentAccessor(pRandomSegment, pCache);
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
    SegmentAccessor segmentAccessor(pRandomSegment, pCache);
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

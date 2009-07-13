/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include "fennel/test/PagingTestBase.h"
#include "fennel/common/FileSystem.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/synch/Thread.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/CacheImpl.h"
#include "fennel/cache/RandomVictimPolicy.h"
#include <boost/test/test_tools.hpp>


#include <functional>

using namespace fennel;

static boost::thread_specific_ptr<std::subtractive_rng> g_pRNG;

void PagingTestBase::threadInit()
{
    ThreadedTestBase::threadInit();
    g_pRNG.reset(new std::subtractive_rng());
}

void PagingTestBase::threadTerminate()
{
    g_pRNG.reset();
    ThreadedTestBase::threadTerminate();
}

uint PagingTestBase::generateRandomNumber(uint iMax)
{
    return (*g_pRNG)(iMax);
}

void PagingTestBase::fillPage(CachePage &page,uint x)
{
    uint *p = reinterpret_cast<uint *>(page.getWritableData());
    assert(cbPageUsable);
    uint n = cbPageUsable / sizeof(uint);
    for (uint i = 0; i < n; i++) {
        p[i] = x + i;
    }
    uint r = generateRandomNumber(n);
    p[r] = 0;
}

void PagingTestBase::verifyPage(CachePage &page,uint x)
{
    uint const *p = reinterpret_cast<uint const *>(page.getReadableData());
    assert(cbPageUsable);
    uint n = cbPageUsable / sizeof(uint);
    uint nZeros = 0;
    for (uint i = 0; i < n; i++) {
        if (p[i] != x + i) {
            assert(!p[i]);
            nZeros++;
        }
    }
    assert(nZeros < 2);
}

bool PagingTestBase::testOp(OpType opType, uint iPage, bool bNice)
{
    CachePage *pPage = lockPage(opType,iPage);
    LockMode lockMode = getLockMode(opType);
    if (!pPage) {
        // must be NoWait locking failed
        assert(lockMode >= LOCKMODE_S_NOWAIT);
        return false;
    }
    CachePage &page = *pPage;
    if (lockMode == LOCKMODE_S
        || lockMode == LOCKMODE_S_NOWAIT)
    {
        verifyPage(page, iPage);
    } else {
        fillPage(page, iPage);
    }
    switch (lockMode) {
    case LOCKMODE_X_NOWAIT:
        lockMode = LOCKMODE_X;
        break;
    case LOCKMODE_S_NOWAIT:
        lockMode = LOCKMODE_S;
        break;
    default:
        break;
    }
    if (bNice) {
        getCache().nicePage(page);
    }
    unlockPage(page, lockMode);
    return true;
}

char const *PagingTestBase::getOpName(OpType opType)
{
    switch (opType) {
    case OP_ALLOCATE:
        return "allocate";
    case OP_READ_SEQ:
        return "sequential read";
    case OP_WRITE_SEQ:
        return "sequential write";
    case OP_READ_RAND:
        return "random read";
    case OP_WRITE_RAND:
        return "random write";
    case OP_READ_NOWAIT:
        return "read no-wait";
    case OP_WRITE_NOWAIT:
        return "write no-wait";
    case OP_WRITE_SKIP:
        return "write every n pages";
    default:
        permAssert(false);
    }
}

LockMode PagingTestBase::getLockMode(OpType opType)
{
    switch (opType) {
    case OP_ALLOCATE:
        return LOCKMODE_X;
    case OP_READ_SEQ:
        return LOCKMODE_S;
    case OP_WRITE_SEQ:
        return LOCKMODE_X;
    case OP_READ_RAND:
        return LOCKMODE_S;
    case OP_WRITE_RAND:
        return LOCKMODE_X;
    case OP_READ_NOWAIT:
        return LOCKMODE_S_NOWAIT;
    case OP_WRITE_NOWAIT:
        return LOCKMODE_X_NOWAIT;
    case OP_WRITE_SKIP:
        return LOCKMODE_X;
    default:
        permAssert(false);
    }
}

void PagingTestBase::testSequentialOp(OpType opType)
{
    uint n = 0;
    for (uint i = 0; i < nDiskPages; i++) {
        if (testOp(opType, i, true)) {
            n++;
        }
    }
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE(
        "completed " << n << " " << getOpName(opType) << " ops");
}

void PagingTestBase::testRandomOp(OpType opType)
{
    uint n = 0;
    for (uint i = 0; i < nRandomOps; i++) {
        uint iPage = generateRandomNumber(nDiskPages);
        bool bNice = (generateRandomNumber(nRandomOps) == 0);
        if (testOp(opType, iPage, bNice)) {
            n++;
        }
    }
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE(
        "completed " << n << " " << getOpName(opType) << " ops");

}

void PagingTestBase::testSkipOp(OpType opType, uint n)
{
    uint numOps = 0;
    for (uint i = 0; i < nDiskPages; i += n) {
        if (testOp(opType, i, true)) {
            numOps++;
        }
    }
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE(
        "completed " << numOps << " " << getOpName(opType) << " ops");
}

void PagingTestBase::testScratch()
{
    for (uint i = 0; i < nRandomOps; i++) {
        CachePage &page = getCache().lockScratchPage();
        fillPage(page, generateRandomNumber(10000));
        getCache().unlockPage(page, LOCKMODE_X);
    }
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE("completed " << nRandomOps << " random scratch ops");
}

void PagingTestBase::testPrefetch()
{
    // TODO: parameterize this
    uint n = 3;
    for (uint i = 0; i < n; i++) {
        uint iPage = generateRandomNumber(nDiskPages);
        prefetchPage(iPage);
    }
    // give the prefetches a chance to complete
    snooze(1);
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE("completed " << n << " random prefetch ops");
}

void PagingTestBase::testPrefetchBatch()
{
    // TODO: parameterize this
    uint n = 2;
    uint nPagesPerBatch = 4;
    for (uint i = 0; i < n; i++) {
        uint iPage = generateRandomNumber(nDiskPages - nPagesPerBatch);
        prefetchBatch(iPage, nPagesPerBatch);
    }
    // give the prefetches a chance to complete
    snooze(1);
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE("completed " << n << " random prefetch batch ops");
}

void PagingTestBase::testAllocateAll()
{
    testSequentialOp(OP_ALLOCATE);
}

void PagingTestBase::testSequentialRead()
{
    testSequentialOp(OP_READ_SEQ);
}

void PagingTestBase::testSequentialWrite()
{
    testSequentialOp(OP_WRITE_SEQ);
}

void PagingTestBase::testRandomRead()
{
    testRandomOp(OP_READ_RAND);
}

void PagingTestBase::testRandomWrite()
{
    testRandomOp(OP_WRITE_RAND);
}

void PagingTestBase::testSkipWrite(uint n)
{
    testSkipOp(OP_WRITE_SKIP, n);
}

void PagingTestBase::testAllocate()
{
    permAssert(false);
}

void PagingTestBase::testDeallocate()
{
    permAssert(false);
}

void PagingTestBase::testCheckpoint()
{
    DeviceIdPagePredicate pagePredicate(dataDeviceId);
    getCache().checkpointPages(pagePredicate, CHECKPOINT_FLUSH_ALL);
}

void PagingTestBase::testCheckpointGuarded()
{
    snooze(nSecondsBetweenCheckpoints);
    StrictMutexGuard logGuard(logMutex);
    BOOST_MESSAGE("checkpoint started");
    logGuard.unlock();
    SXMutexExclusiveGuard checkpointExclusiveGuard(checkpointMutex);
    testCheckpoint();
    checkpointExclusiveGuard.unlock();
    logGuard.lock();
    BOOST_MESSAGE("checkpoint completed");
}

void PagingTestBase::testCacheResize()
{
    snooze(nSeconds / 3);
    getCache().setAllocatedPageCount(nMemPages / 2);
    StrictMutexGuard mutexGuard(logMutex);
    BOOST_MESSAGE("shrank cache");
    mutexGuard.unlock();
    snooze(nSeconds / 3);
    getCache().setAllocatedPageCount(nMemPages - 1);
    mutexGuard.lock();
    BOOST_MESSAGE("expanded cache");
    mutexGuard.unlock();
}

PagingTestBase::PagingTestBase()
{
    nRandomOps = configMap.getIntParam("randomOps", 5000);
    nSecondsBetweenCheckpoints =
        configMap.getIntParam("checkpointInterval", 20);
    bTestResize = configMap.getIntParam("resizeCache", 1);
    checkpointMutex.setSchedulingPolicy(SXMutex::SCHEDULE_FAVOR_EXCLUSIVE);

    threadCounts.resize(OP_MAX,-1);

    threadCounts[OP_READ_SEQ] = configMap.getIntParam(
        "readSeqThreads",-1);
    threadCounts[OP_WRITE_SEQ] = configMap.getIntParam(
        "writeSeqThreads",-1);
    threadCounts[OP_READ_RAND] = configMap.getIntParam(
        "readRandThreads",-1);
    threadCounts[OP_WRITE_RAND] = configMap.getIntParam(
        "writeRandThreads",-1);
    threadCounts[OP_READ_NOWAIT] = configMap.getIntParam(
        "readNoWaitThreads",-1);
    threadCounts[OP_WRITE_NOWAIT] = configMap.getIntParam(
        "writeNoWaitThreads",-1);
    threadCounts[OP_WRITE_SKIP] = configMap.getIntParam(
        "writeSkipThreads",-1);
    threadCounts[OP_SCRATCH] = configMap.getIntParam(
        "scratchThreads",-1);
    threadCounts[OP_PREFETCH] = configMap.getIntParam(
        "prefetchThreads",-1);
    threadCounts[OP_PREFETCH_BATCH] = configMap.getIntParam(
        "prefetchBatchThreads",-1);
    threadCounts[OP_ALLOCATE] = configMap.getIntParam(
        "allocateThreads",-1);
    threadCounts[OP_DEALLOCATE] = configMap.getIntParam(
        "deallocateThreads",-1);

    if (nSecondsBetweenCheckpoints < nSeconds) {
        threadCounts[OP_CHECKPOINT] = 1;
    } else {
        threadCounts[OP_CHECKPOINT] = 0;
    }

    if (bTestResize) {
        threadCounts[OP_RESIZE_CACHE] = 1;
    } else {
        threadCounts[OP_RESIZE_CACHE] = 0;
    }

    cbPageUsable = 0;

    threadInit();
}

PagingTestBase::~PagingTestBase()
{
    threadTerminate();
}

bool PagingTestBase::testThreadedOp(int iOp)
{
    SXMutexSharedGuard checkpointSharedGuard(checkpointMutex, false);
    assert(iOp < OP_MAX);
    OpType op = static_cast<OpType>(iOp);
    switch (op) {
    case PagingTestBase::OP_WRITE_SEQ:
        checkpointSharedGuard.lock();
        // fall through
    case PagingTestBase::OP_READ_SEQ:
        testSequentialOp(op);
        break;
    case PagingTestBase::OP_WRITE_RAND:
    case PagingTestBase::OP_WRITE_NOWAIT:
        checkpointSharedGuard.lock();
        // fall through
    case PagingTestBase::OP_READ_RAND:
    case PagingTestBase::OP_READ_NOWAIT:
        testRandomOp(op);
        break;
    case PagingTestBase::OP_WRITE_SKIP:
        checkpointSharedGuard.lock();
        testSkipOp(op, 5);
        break;
    case PagingTestBase::OP_SCRATCH:
        testScratch();
        break;
    case PagingTestBase::OP_PREFETCH:
        testPrefetch();
        break;
    case PagingTestBase::OP_PREFETCH_BATCH:
        testPrefetchBatch();
        break;
    case PagingTestBase::OP_ALLOCATE:
        checkpointSharedGuard.lock();
        testAllocate();
        break;
    case PagingTestBase::OP_DEALLOCATE:
        checkpointSharedGuard.lock();
        testDeallocate();
        break;
    case PagingTestBase::OP_CHECKPOINT:
        testCheckpointGuarded();
        break;
    case PagingTestBase::OP_RESIZE_CACHE:
        testCacheResize();
        return false;
    default:
        permAssert(false);
    }
    return true;
}

void PagingTestBase::testMultipleThreads()
{
    openStorage(DeviceMode::createNew);
    testAllocateAll();
    runThreadedTestCase();
}

// End PagingTestBase.cpp

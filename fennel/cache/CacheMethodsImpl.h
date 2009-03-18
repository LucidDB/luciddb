/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#ifndef Fennel_CacheMethodsImpl_Included
#define Fennel_CacheMethodsImpl_Included

#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/device/RandomAccessNullDevice.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/common/CompoundId.h"
#include "fennel/common/InvalidParamExcn.h"
#include "fennel/cache/VMAllocator.h"

// NOTE:  don't include this file directly; include CacheImpl.h instead

FENNEL_BEGIN_NAMESPACE

// TODO:  cache data structure consistency check

// ----------------------------------------------------------------------
// Public entry points
// ----------------------------------------------------------------------

template <class PageT,class VictimPolicyT>
CacheImpl<PageT,VictimPolicyT>
::CacheImpl(
    CacheParams const &params,
    CacheAllocator *pBufferAllocatorInit)
:
    deviceTable(CompoundId::getMaxDeviceCount()),
    pageTable(),
    bufferAllocator(
        pBufferAllocatorInit ?
        *pBufferAllocatorInit
        : *new VMAllocator(params.cbPage,0)),
    pBufferAllocator(pBufferAllocatorInit ? NULL : &bufferAllocator),
    victimPolicy(params),
    timerThread(*this)
{
    cbPage = params.cbPage;
    pDeviceAccessScheduler = NULL;
    inFlushMode = false;

    // TODO - parameterize
    dirtyHighWaterPercent = 25;
    dirtyLowWaterPercent = 5;

    initializeStats();

    allocatePages(params);

    // initialize page hash table
    // NOTE: this is the size of the page hash table; 2N is for a 50%
    // load factor, and +1 is to avoid picking an even number
    // TODO:  use a static table of primes to pick the least-upper-bound prime
    pageTable.resize(2*pages.size()+1);
    for (uint i = 0; i < pageTable.size(); i++) {
        pageTable[i] = new PageBucketT();
    }

    try {
        pDeviceAccessScheduler = DeviceAccessScheduler::newScheduler(
            params.schedParams);
    }
    catch (FennelExcn &ex) {
        close();
        throw ex;
    }

    // initialize null device
    registerDevice(
        NULL_DEVICE_ID,
        SharedRandomAccessDevice(
            new RandomAccessNullDevice()));

    idleFlushInterval = params.idleFlushInterval;
    if (idleFlushInterval) {
        timerThread.start();
    }

    prefetchPagesMax = params.prefetchPagesMax;
    prefetchThrottleRate = params.prefetchThrottleRate;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>::getPrefetchParams(
    uint &prefetchPagesMax,
    uint &prefetchThrottleRate)
{
    prefetchPagesMax = this->prefetchPagesMax;
    prefetchThrottleRate = this->prefetchThrottleRate;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>::initializeStats()
{
    // clear instantaneous counters too just to avoid confusion
    statsSinceInit.nHits = 0;
    statsSinceInit.nHitsSinceInit = 0;
    statsSinceInit.nRequests = 0;
    statsSinceInit.nRequestsSinceInit = 0;
    statsSinceInit.nVictimizations = 0;
    statsSinceInit.nVictimizationsSinceInit = 0;
    statsSinceInit.nDirtyPages = 0;
    statsSinceInit.nPageReads = 0;
    statsSinceInit.nPageReadsSinceInit = 0;
    statsSinceInit.nPageWrites = 0;
    statsSinceInit.nPageWritesSinceInit = 0;
    statsSinceInit.nRejectedPrefetches = 0;
    statsSinceInit.nRejectedPrefetchesSinceInit = 0;
    statsSinceInit.nIoRetries = 0;
    statsSinceInit.nIoRetriesSinceInit = 0;
    statsSinceInit.nSuccessfulPrefetches = 0;
    statsSinceInit.nSuccessfulPrefetchesSinceInit = 0;
    statsSinceInit.nLazyWrites = 0;
    statsSinceInit.nLazyWritesSinceInit = 0;
    statsSinceInit.nLazyWriteCalls = 0;
    statsSinceInit.nLazyWriteCallsSinceInit = 0;
    statsSinceInit.nVictimizationWrites = 0;
    statsSinceInit.nVictimizationWritesSinceInit = 0;
    statsSinceInit.nCheckpointWrites = 0;
    statsSinceInit.nCheckpointWritesSinceInit = 0;
    statsSinceInit.nMemPagesAllocated = 0;
    statsSinceInit.nMemPagesUnused = 0;
    statsSinceInit.nMemPagesMax = 0;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>::allocatePages(CacheParams const &params)
{
    static const int allocErrorMsgSize = 255;
    uint nPagesMax = 0;
    uint nPagesInit = 0;

    // Make two attempts: First, use the configured values.  If that fails,
    // try again with default nMemPagesMax.  If that fails, throw in the towel.
    for(int attempts = 0; attempts < 2; attempts++) {
        bool allocError = false;
        int allocErrorCode = 0;
        char allocErrorMsg[allocErrorMsgSize + 1] = { 0 };

        nPagesMax = params.nMemPagesMax;
        nPagesInit = params.nMemPagesInit;

        try {
            if (attempts != 0) {
                nPagesMax = CacheParams::defaultMemPagesMax;
                nPagesInit = CacheParams::defaultMemPagesInit;
            }

            pages.clear();
            if (pages.capacity() > nPagesMax) {
                // Reset capacity of pages to a smaller value by swapping pages
                // with a temporary vector that has no capacity.
                std::vector<PageT *>(0).swap(pages);
            }
            pages.reserve(nPagesMax);
            pages.assign(nPagesMax, NULL);

            // allocate pages, but defer adding all of them onto the free list
            for (uint i = 0; i < nPagesMax; i++) {
                PBuffer pBuffer = NULL;
                if (i < nPagesInit) {
                    pBuffer = static_cast<PBuffer>(
                        bufferAllocator.allocate(&allocErrorCode));
                    if (pBuffer == NULL) {
                        allocError = true;
                        strncpy(
                            allocErrorMsg, "mmap failed", allocErrorMsgSize);
                        break;
                    }
                }
                PageT &page = *new PageT(*this,pBuffer);
                pages[i] = &page;
            }
        } catch(std::exception &excn) {
            allocError = true;
            allocErrorCode = 0;
            if (dynamic_cast<std::bad_alloc *>(&excn) != NULL) {
                strncpy(allocErrorMsg, "malloc failed", allocErrorMsgSize);
            } else {
                strncpy(allocErrorMsg, excn.what(), allocErrorMsgSize);
            }
        }

        if (!allocError) {
            // successful allocation
            break;
        }

        // Free the allocated pages
        for (uint i = 0; i < pages.size(); i++) {
            if (!pages[i]) {
                break;
            }
            PBuffer pBuffer = pages[i]->pBuffer;
            deleteAndNullify(pages[i]);
            if (pBuffer) {
                // Ignore any error. We are sometimes unable to deallocate
                // pages when trying to recover from initial failure.  Likely
                // the second attempt will fail as well.  This leads to a
                // failed assertion in the VMAllocator destructor.  See the
                // comment there.
                bufferAllocator.deallocate(pBuffer);
            }
        }

        if (attempts != 0) {
            // Reduced page count still failed.  Give up.
            close();
            throw SysCallExcn(std::string(allocErrorMsg), allocErrorCode);
        }
    }

    // Go back and add the pages to the free list and register them with
    // victimPolicy (requires no further memory allocation as the free lists
    // and victim policy use IntrusiveList and IntrusiveDList).
    for (uint i = 0; i < pages.size(); i++) {
        PageT *page = pages[i];
        PBuffer pBuffer = page->pBuffer;
        if (pBuffer) {
            unmappedBucket.pageList.push_back(*page);
            victimPolicy.registerPage(*page);
        } else {
            unallocatedBucket.pageList.push_back(*page);
        }
    }

    uint nPages = std::min(nPagesInit, nPagesMax);
    calcDirtyThreshholds(nPages);
    victimPolicy.setAllocatedPageCount(nPages);
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>::calcDirtyThreshholds(uint nCachePages)
{
    dirtyHighWaterMark = nCachePages * dirtyHighWaterPercent / 100;
    dirtyLowWaterMark = nCachePages * dirtyLowWaterPercent / 100;
}

template <class PageT,class VictimPolicyT>
uint CacheImpl<PageT,VictimPolicyT>::getAllocatedPageCount()
{
    SXMutexSharedGuard guard(unallocatedBucket.mutex);
    return pages.size() - unallocatedBucket.pageList.size();
}

template <class PageT,class VictimPolicyT>
uint CacheImpl<PageT,VictimPolicyT>::getMaxAllocatedPageCount()
{
    return pages.size();
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>::setAllocatedPageCount(
    uint nMemPagesDesired)
{
    assert(nMemPagesDesired <= pages.size());
    // exclusive lock unallocatedBucket in case someone is crazy enough to call
    // this method from multiple threads
    SXMutexExclusiveGuard unallocatedBucketGuard(unallocatedBucket.mutex);
    uint nMemPages =
        pages.size() - unallocatedBucket.pageList.size();
    if (nMemPages < nMemPagesDesired) {
        // allocate some more

        // LER-5976: Allocate all pBuffers ahead of time so we can revert to
        // the old cache size if there's an allocation error.
        int nMemPagesToAllocate = nMemPagesDesired - nMemPages;
        std::vector<PBuffer> buffers(nMemPagesToAllocate);

        for (int i = 0; i < nMemPagesToAllocate; ++i) {
            int errorCode;
            PBuffer pBuffer = static_cast<PBuffer>(
                bufferAllocator.allocate(&errorCode));

            if (pBuffer == NULL) {
                // Release each allocated buffer and re-throw
                for(int i = 0; i < nMemPagesToAllocate; i++) {
                    if (buffers[i] == NULL) {
                        break;
                    }

                    // Ignore any errors and try to deallocate as many of the
                    // buffers as possible.  Ignoring errors leads to a failed
                    // assertion in the VMAllocator destructor on shutdown. See
                    // the comment there.
                    bufferAllocator.deallocate(buffers[i]);
                }
                buffers.clear();
                std::vector<PBuffer>(0).swap(buffers); // dealloc vector

                throw SysCallExcn("mmap failed", errorCode);
            }

            buffers[i] = pBuffer;
        }

        PageBucketMutator mutator(unallocatedBucket.pageList);
        for (int i = 0; i < nMemPagesToAllocate; i++) {
            PBuffer pBuffer = buffers[i];
            PageT *page = mutator.detach();
            assert(!page->pBuffer);
            page->pBuffer = pBuffer;
            victimPolicy.registerPage(*page);
            // move to unmappedBucket
            freePage(*page);
        }
    } else {
        // deallocate some
        for (; nMemPages > nMemPagesDesired; --nMemPages) {
            PageT *page;
            do {
                page = findFreePage();
            } while (!page);

            int errorCode;
            if (bufferAllocator.deallocate(page->pBuffer, &errorCode)) {
                // If the page buffer couldn't be deallocated, put it back
                // before reporting the error
                freePage(*page);
                throw SysCallExcn("munmap failed", errorCode);
            }
            page->pBuffer = NULL;
            victimPolicy.unregisterPage(*page);
            // move to unallocatedBucket
            unallocatedBucket.pageList.push_back(*page);
        }
    }

    calcDirtyThreshholds(nMemPagesDesired);
    // Notify the policy of the new cache size
    victimPolicy.setAllocatedPageCount(nMemPagesDesired);
}

template <class PageT,class VictimPolicyT>
PageT *CacheImpl<PageT,VictimPolicyT>
::lockPage(
    BlockId blockId,LockMode lockMode,bool readIfUnmapped,
    MappedPageListener *pMappedPageListener,TxnId txnId)
{
    // first find the page and increment its reference count

    assert(blockId != NULL_BLOCK_ID);
    assert(CompoundId::getDeviceId(blockId) != NULL_DEVICE_ID);
    PageBucketT &bucket = getHashBucket(blockId);
    PageT *page = lookupPage(bucket,blockId,true);
    if (page) {
        assert(page->pMappedPageListener == pMappedPageListener);
        // note that lookupPage incremented page's reference count for us, so
        // it's safe from victimization from here on
        incrementStatsCounter(nCacheHits);
    } else {
        do {
            page = findFreePage();
        } while (!page);

        // note that findFreePage returns an unmapped page, making it safe from
        // victimization at this point; mapPage will increment the reference
        // count

        PageT &mappedPage = mapPage(
            bucket,*page,blockId,pMappedPageListener,readIfUnmapped);
        if (&mappedPage == page) {
            // mapPage found no existing mapping, so initiate read from disk if
            // necessary
            if (readIfUnmapped) {
                readPageAsync(*page);
            }
        } else {
            // mapPage found an existing mapping, so forget unused free page,
            // and no need to initiate read from disk
            page = &mappedPage;
        }
        if (readIfUnmapped) {
            // whether or not an existing mapping was found, need
            // to wait for any pending read to complete (either our own started
            // above or someone else's)
            StrictMutexGuard pageGuard(page->mutex);
            while (page->dataStatus == CachePage::DATA_READ) {
                page->waitForPendingIO(pageGuard);
            }
        }
    }

    incrementStatsCounter(nCacheRequests);

    // now acquire the requested lock

    if (!page->lock.waitFor(lockMode,ETERNITY,txnId)) {
        // NoWait failed; release reference
        assert((lockMode == LOCKMODE_S_NOWAIT) ||
               (lockMode == LOCKMODE_X_NOWAIT));
        StrictMutexGuard pageGuard(page->mutex);
        page->nReferences--;
        if (!page->nReferences) {
            victimPolicy.notifyPageUnpin(*page);
        }
        return NULL;
    }
    if ((lockMode == LOCKMODE_X) || (lockMode == LOCKMODE_X_NOWAIT)) {
        // if we're locking the page for write, then need to make sure
        // that any pending write completes before this thread starts
        // changing the contents

        // REVIEW: can we use double-checked idiom here?
        StrictMutexGuard pageGuard(page->mutex);
        while (page->dataStatus == CachePage::DATA_WRITE) {
            page->waitForPendingIO(pageGuard);
        }
#ifdef DEBUG
        int errorCode;
        if (bufferAllocator.setProtection(
                page->pBuffer, cbPage, false, &errorCode))
        {
            throw new SysCallExcn("memory protection failed", errorCode);
        }
#endif
    } else {
        // TODO jvs 7-Feb-2006:  protection for other cases
#ifdef DEBUG
        StrictMutexGuard pageGuard(page->mutex);
        if (page->nReferences == 1) {
            int errorCode;
            if (bufferAllocator.setProtection(
                    page->pBuffer, cbPage, true, &errorCode))
            {
                throw new SysCallExcn("memory protection failed", errorCode);
            }
        }
#endif
    }
    return page;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::unlockPage(
    CachePage &vPage,LockMode lockMode,TxnId txnId)
{
    assert(lockMode < LOCKMODE_S_NOWAIT);
    PageT &page = static_cast<PageT &>(vPage);
    StrictMutexGuard pageGuard(page.mutex);
    assert(page.nReferences);
    bool bFree = false;
    assert(page.hasBlockId());
    if (CompoundId::getDeviceId(page.getBlockId()) == NULL_DEVICE_ID) {
        // originated from lockScratchPage()
        bFree = true;
    } else {
        int errorCode;
        if (bufferAllocator.setProtection(
                page.pBuffer, cbPage, false, &errorCode))
        {
            throw new SysCallExcn("memory protection failed", errorCode);
        }

        page.lock.release(lockMode,txnId);
    }
    page.nReferences--;
    if (!page.nReferences) {

        if (bFree) {
            // The page lock was acquired via lockScratch, so return it to
            // the free list.  No need to notify the victimPolicy since
            // the policy wasn't notified when the page was locked.
            page.dataStatus = CachePage::DATA_INVALID;
            page.blockId = NULL_BLOCK_ID;
            freePage(page);
        } else {
            // notify the victim policy that the page is no longer
            // being referenced
            victimPolicy.notifyPageUnpin(page);
        }

        // let waiting threads know that a page has become available
        // (either on the free list or as a victimization candidate)
        freePageCondition.notify_all();
    }
}

template <class PageT,class VictimPolicyT>
bool CacheImpl<PageT,VictimPolicyT>
::isPageMapped(BlockId blockId)
{
    PageBucketT &bucket = getHashBucket(blockId);
    SXMutexSharedGuard bucketGuard(bucket.mutex);
    for (PageBucketIter iter(bucket.pageList); iter; ++iter) {
        StrictMutexGuard pageGuard(iter->mutex);
        if (iter->getBlockId() == blockId) {
            bucketGuard.unlock();
            victimPolicy.notifyPageAccess(*iter, false);
            return true;
        }
    }
    return false;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::discardPage(BlockId blockId)
{
    assert(blockId != NULL_BLOCK_ID);
    PageBucketT &bucket = getHashBucket(blockId);
    PageT *page = lookupPage(bucket,blockId,false);
    if (!page) {
        // page is not mapped, so nothing to discard, but still need to
        // notify the policy
        victimPolicy.notifyPageDiscard(blockId);
        return;
    }
    StrictMutexTryGuard pageGuard(page->mutex,true);
    // lookupPage already waited for pending reads, but also need to wait for
    // pending writes
    // REVIEW:  isn't this redundant with code in unmapAndFreeDiscardedPage?
    while (page->dataStatus == CachePage::DATA_WRITE) {
        page->waitForPendingIO(pageGuard);
    }
    // our own lookupPage adds 1 reference; it should be the only one left
    assert(page->nReferences == 1);
    page->nReferences = 0;
    unmapAndFreeDiscardedPage(*page,pageGuard);
}

template <class PageT,class VictimPolicyT>
PageT &CacheImpl<PageT,VictimPolicyT>
::lockScratchPage(BlockNum blockNum)
{
    PageT *page;
    do {
        page = findFreePage();
    } while (!page);

    StrictMutexGuard pageGuard(page->mutex);
    page->nReferences = 1;
    // Set dirty early to avoid work on first call to getWritableData.
    // No need to notify the victimPolicy that the page is dirty because
    // scratch pages are locked for the duration of their use so they're
    // never candidates for victimization or flushing.
    page->dataStatus = CachePage::DATA_DIRTY;
    CompoundId::setDeviceId(page->blockId,NULL_DEVICE_ID);
    CompoundId::setBlockNum(page->blockId,blockNum);

    return *page;
}

template <class PageT,class VictimPolicyT>
bool CacheImpl<PageT,VictimPolicyT>
::prefetchPage(BlockId blockId,MappedPageListener *pMappedPageListener)
{
    assert(blockId != NULL_BLOCK_ID);
    if (isPageMapped(blockId)) {
        // already mapped:  either it's already fully read or someone
        // else has initiated a read; either way, nothing for us to do
        successfulPrefetch();
        return true;
    }
    PageT *page = findFreePage();
    if (!page) {
        // cache is low on free pages:  ignore prefetch hint
        rejectedPrefetch();
        return false;
    }

    PageBucketT &bucket = getHashBucket(blockId);
    bool bPendingRead = true;
    // don't need to increment the page reference count since the pending
    // read will protect the page from victimization, and the calling thread
    // doesn't actually want a reference until it locks the page later
    bool bIncRef = false;
    PageT &mappedPage = mapPage(
        bucket,*page,blockId,pMappedPageListener,bPendingRead,bIncRef);
    if (&mappedPage == page) {
        if (readPageAsync(*page)) {
            successfulPrefetch();
        } else {
            rejectedPrefetch();
            return false;
        }
    } else {
        // forget unused free page, and don't bother with read since someone
        // else must already have kicked it off
        page = &mappedPage;
    }
    return true;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::prefetchBatch(
    BlockId blockId,uint nPagesPerBatch,
    MappedPageListener *pMappedPageListener)
{
    assert(blockId != NULL_BLOCK_ID);
    assert(nPagesPerBatch > 1);

    SharedRandomAccessDevice &pDevice = getDevice(
        CompoundId::getDeviceId(blockId));
    DeviceAccessScheduler &scheduler = getDeviceAccessScheduler(*pDevice);
    RandomAccessRequest request;
    request.pDevice = pDevice.get();
    request.cbOffset = getPageOffset(blockId);
    request.cbTransfer = 0;
    request.type = RandomAccessRequest::READ;

    BlockId blockIdi = blockId;
    for (uint i = 0; i < nPagesPerBatch; i++) {
        PageT *page;
        do {
            page = findFreePage();
        } while (!page);

        PageBucketT &bucket = getHashBucket(blockIdi);
        bool bPendingRead = true;
        bool bIncRef = false;
        PageT &mappedPage = mapPage(
            bucket,*page,blockIdi,pMappedPageListener,bPendingRead,bIncRef);
        if (&mappedPage != page) {
            // This page already mapped; can't do batch prefetch.  For the
            // pages which we've already mapped, initiate transfer.
            // For this page, skip altogether since it's already been read
            // (or has a read in progress).  For remaining pages, continue
            // building new request.
            if (request.cbTransfer) {
                if (scheduler.schedule(request)) {
                    successfulPrefetch();
                } else {
                    ioRetry();
                    rejectedPrefetch();
                }
            }
            // adjust start past transfer just initiated plus already mapped
            // page
            request.cbOffset += request.cbTransfer;
            request.cbOffset += getPageSize();
            request.cbTransfer = 0;
            request.bindingList.clear(false);
        } else {
            // add this page to the request
            request.cbTransfer += getPageSize();
            request.bindingList.push_back(*page);
        }
        CompoundId::incBlockNum(blockIdi);
    }
    // deal with leftovers
    if (request.cbTransfer) {
        if (scheduler.schedule(request)) {
            successfulPrefetch();
        } else {
            ioRetry();
            rejectedPrefetch();
        }
    }
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::successfulPrefetch()
{
    incrementStatsCounter(nSuccessfulCachePrefetches);
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::rejectedPrefetch()
{
    incrementStatsCounter(nRejectedCachePrefetches);
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::ioRetry()
{
    incrementStatsCounter(nIoRetries);
}

template <class PageT,class VictimPolicyT>
uint CacheImpl<PageT,VictimPolicyT>
::checkpointPages(
    PagePredicate &pagePredicate,CheckpointType checkpointType)
{
    // TODO:  change RandomAccessRequest interface so that we can gang
    // these all up into one big discontiguous request

    uint nPages = 0;
    bool countPages = true;

    FlushPhase flushPhase;
    if (checkpointType >= CHECKPOINT_FLUSH_AND_UNMAP) {
        flushPhase = phaseInitiate;
    } else {
        flushPhase = phaseSkip;
    }
    for (;;) {
        for (uint i = 0; i < pages.size(); i++) {
            PageT &page = *(pages[i]);
            StrictMutexTryGuard pageGuard(page.mutex,true);
            // restrict view to just mapped pages of interest
            if (!page.hasBlockId()) {
                continue;
            }
            if (!pagePredicate(page)) {
                continue;
            }
            if (countPages) {
                ++nPages;
            }
            if (flushPhase == phaseInitiate) {
                if (page.isDirty()) {
                    // shouldn't be flushing a page if someone is currently
                    // scribbling on it
                    assert(!page.isExclusiveLockHeld());
                    incrementStatsCounter(nCheckpointWrites);
                    // initiate a flush
                    writePageAsync(page);
                }
            } else if (flushPhase == phaseWait) {
                BlockId origBlockId = page.getBlockId();
                MappedPageListener *origListener = page.pMappedPageListener;
                while (page.dataStatus == CachePage::DATA_WRITE) {
                    page.waitForPendingIO(pageGuard);
                }

                // If this page has been remapped during sleeps that occurred
                // while waiting for the page I/O to complete, then there's
                // no need to reset the listener, since the remap has
                // effectively reset the listener.  (TODO: zfong 6/23/08 -
                // Add a unit testcase for this.)
                //
                // Otherwise, reset the listener, if called for by the original
                // listener.  Note that by doing so, during the next iteration
                // in the outermost for loop in this method when we're
                // unmapping cache entries, we will not unmap this page
                // because we've changed the listener.
                if (page.pMappedPageListener &&
                    page.pMappedPageListener == origListener &&
                    page.getBlockId() == origBlockId)
                {
                    MappedPageListener *newListener =
                    page.pMappedPageListener->notifyAfterPageCheckpointFlush(
                        page);
                    if (newListener != NULL) {
                        page.pMappedPageListener = newListener;
                    }
                }
            } else {
                if (checkpointType <= CHECKPOINT_FLUSH_AND_UNMAP) {
                    unmapAndFreeDiscardedPage(page,pageGuard);
                }
            }
        }
        countPages = false;
        if (flushPhase == phaseInitiate) {
            flushPhase = phaseWait;
            continue;
        }
        if (flushPhase == phaseWait) {
            if (checkpointType <= CHECKPOINT_FLUSH_AND_UNMAP) {
                flushPhase = phaseSkip;
                continue;
            }
        }
        return nPages;
    }
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::flushPage(CachePage &page,bool async)
{
    StrictMutexGuard pageGuard(page.mutex);
    assert(page.isExclusiveLockHeld());
    if (page.pMappedPageListener) {
        if (!page.pMappedPageListener->canFlushPage(page)) {
            if (async) {
                // TODO jvs 21-Jan-2006: instead of ignoring request, fail; we
                // should be using Segment-level logic to avoid ever getting
                // here
                return;
            }
            permFail("attempt to flush page out of order");
        }
    }
    if (page.dataStatus != CachePage::DATA_WRITE) {
        // no flush already in progress, so request one
        writePageAsync(static_cast<PageT &>(page));
    }
    if (async) {
        return;
    }
    // wait for flush to complete
    while (page.dataStatus == CachePage::DATA_WRITE) {
        page.waitForPendingIO(pageGuard);
    }
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::nicePage(CachePage &page)
{
    victimPolicy.notifyPageNice(static_cast<PageT &>(page));
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::registerDevice(DeviceId deviceId,SharedRandomAccessDevice pDevice)
{
    assert(deviceTable[opaqueToInt(deviceId)] == NULL);
    deviceTable[opaqueToInt(deviceId)] = pDevice;
    pDeviceAccessScheduler->registerDevice(pDevice);
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::unregisterDevice(DeviceId deviceId)
{
    SharedRandomAccessDevice &pDevice = getDevice(deviceId);
    assert(pDevice);
    DeviceIdPagePredicate pagePredicate(deviceId);
    uint nPages = checkpointPages(pagePredicate,CHECKPOINT_DISCARD);
    assert(!nPages);
    pDeviceAccessScheduler->unregisterDevice(pDevice);
    pDevice.reset();
}

template <class PageT,class VictimPolicyT>
SharedRandomAccessDevice &CacheImpl<PageT,VictimPolicyT>
::getDevice(DeviceId deviceId)
{
    return deviceTable[opaqueToInt(deviceId)];
}

// ----------------------------------------------------------------------
// Notification methods called from friend Page
// ----------------------------------------------------------------------

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::notifyTransferCompletion(CachePage &page,bool bSuccess)
{
    StrictMutexGuard pageGuard(page.mutex);
    // NOTE: A write failure is always a panic, because there's nothing we
    // can do to recover from it.  However, read failures may be expected under
    // some recovery conditions, and will be detected as an assertion when the
    // caller invokes readablePage() on the locked page.  Callers in recovery
    // can use isDataValid() to avoid the assertion.
    switch(page.dataStatus) {
    case CachePage::DATA_WRITE:
        {
            if (!bSuccess) {
                std::cerr << "Write failed for page 0x" << std::hex <<
                    opaqueToInt(page.getBlockId());
                ::abort();
            }
            decrementCounter(nDirtyPages);
            victimPolicy.notifyPageClean(static_cast<PageT &>(page));
            // let waiting threads know that this page may now be available
            // for victimization
            freePageCondition.notify_all();
        }
        break;
    case CachePage::DATA_READ:
        break;
    default:
        permAssert(false);
        break;
    }
    if (bSuccess) {
        CachePage::DataStatus oldStatus = page.dataStatus;
        page.dataStatus = CachePage::DATA_CLEAN;
        if (page.pMappedPageListener) {
            if (oldStatus == CachePage::DATA_READ) {
                page.pMappedPageListener->notifyAfterPageRead(page);
            } else {
                page.pMappedPageListener->notifyAfterPageFlush(page);
            }
        }
    } else {
        page.dataStatus = CachePage::DATA_ERROR;
    }
    page.ioCompletionCondition.notify_all();
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::markPageDirty(CachePage &page)
{
    StrictMutexGuard pageGuard(page.mutex);
    incrementCounter(nDirtyPages);
    bool bValid = page.isDataValid();
    page.dataStatus = CachePage::DATA_DIRTY;
    victimPolicy.notifyPageDirty(static_cast<PageT &>(page));

    // No synchronization required during notification because caller already
    // holds exclusive lock on page.  The notification is called AFTER the page
    // has already been marked dirty in case the listener needs to write to
    // the page (otherwise an infinite loop would occur).
    pageGuard.unlock();
    if (page.pMappedPageListener){
        page.pMappedPageListener->notifyPageDirty(page,bValid);
    }
}

// ----------------------------------------------------------------------
// Implementation of TimerThreadClient interface
// ----------------------------------------------------------------------

template <class PageT,class VictimPolicyT>
uint CacheImpl<PageT,VictimPolicyT>
::getTimerIntervalMillis()
{
    return idleFlushInterval;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::onTimerInterval()
{
    flushSomePages();
}

// ----------------------------------------------------------------------
// Private implementation methods
// ----------------------------------------------------------------------

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>::closeImpl()
{
    if (timerThread.isStarted()) {
        timerThread.stop();
    }

    if (pDeviceAccessScheduler) {
        pDeviceAccessScheduler->stop();
    }

    // unregister the null device
    if (getDevice(NULL_DEVICE_ID)) {
        unregisterDevice(NULL_DEVICE_ID);
    }

    // make sure all devices got unregistered
    for (uint i = 0; i < deviceTable.size(); i++) {
        assert(!deviceTable[i]);
    }

    deleteAndNullify(pDeviceAccessScheduler);

    // clean up page hash table
    for (uint i = 0; i < pageTable.size(); i++) {
        // all pages should already have been unmapped
        assert(!pageTable[i]->pageList.size());
        deleteAndNullify(pageTable[i]);
    }

    unmappedBucket.pageList.clear();
    unallocatedBucket.pageList.clear();

    // deallocate all pages
    for (uint i = 0; i < pages.size(); i++) {
        if (!pages[i]) {
            continue;
        }
        victimPolicy.unregisterPage(*(pages[i]));
        PBuffer pBuffer = pages[i]->pBuffer;
        if (pBuffer) {
            int errorCode;
            if (bufferAllocator.deallocate(pBuffer, &errorCode)) {
                throw SysCallExcn("munmap failed", errorCode);
            }
        }
        deleteAndNullify(pages[i]);
    }
}

template <class PageT,class VictimPolicyT>
PageT *CacheImpl<PageT,VictimPolicyT>
::lookupPage(PageBucketT &bucket,BlockId blockId, bool pin)
{
    assertCorrectBucket(bucket,blockId);
    SXMutexSharedGuard bucketGuard(bucket.mutex);
    for (PageBucketIter iter(bucket.pageList); iter; ++iter) {
        StrictMutexGuard pageGuard(iter->mutex);
        if (iter->getBlockId() == blockId) {
            victimPolicy.notifyPageAccess(*iter, pin);
            iter->nReferences++;
            while (iter->dataStatus == CachePage::DATA_READ) {
                iter->waitForPendingIO(pageGuard);
            }
            return iter;
        }
    }
    return NULL;
}

template <class PageT,class VictimPolicyT>
PageT *CacheImpl<PageT,VictimPolicyT>
::findFreePage()
{
    // Check unmappedBucket first.  Note the use of the double-checked locking
    // idiom here; it's OK because perfect accuracy is not required.  Under
    // steady-state conditions, unmappedBucket will be empty, so avoiding
    // unnecessary locking is a worthwhile optimization.
    if (unmappedBucket.pageList.size()) {
        SXMutexExclusiveGuard unmappedBucketGuard(unmappedBucket.mutex);
        PageBucketMutator mutator(unmappedBucket.pageList);
        if (mutator) {
            assert(!mutator->hasBlockId());
            return mutator.detach();
        }
    }
    // search for a victimizable page, trying pages in the order recommended
    // by victimPolicy
    uint nToFlush = 10;

    VictimSharedGuard victimSharedGuard(victimPolicy.getMutex());
    std::pair<VictimPageIterator,VictimPageIterator> victimRange(
        victimPolicy.getVictimRange());
    for (; victimRange.first != victimRange.second; ++(victimRange.first)) {
        PageT &page = *(victimRange.first);
        // if page mutex is unavailable, just skip it
        StrictMutexTryGuard pageGuard(page.mutex);
        if (!pageGuard.locked()) {
            continue;
        }
        if (canVictimizePage(page)) {
            if (page.isDirty()) {
                // can't victimize a dirty page; kick off an async write
                // and maybe later when we come back to try again it will
                // be available
                if (!nToFlush) {
                    continue;
                }
                if (page.pMappedPageListener &&
                    !page.pMappedPageListener->canFlushPage(page))
                {
                    continue;
                }
                nToFlush--;
                incrementStatsCounter(nVictimizationWrites);
                // If the write request required retry, don't submit any
                // additional write requests in this loop
                if (!writePageAsync(page)) {
                    nToFlush = 0;
                }
                continue;
            }
            // NOTE:  have to do this early since unmapPage will
            // call back into victimPolicy, which could deadlock
            victimSharedGuard.unlock();
            unmapPage(page,pageGuard,false);
            incrementStatsCounter(nVictimizations);
            return &page;
        }
    }
    victimSharedGuard.unlock();

    // no free pages, so wait for one (with timeout just in case)
    StrictMutexGuard freePageGuard(freePageMutex);
    boost::xtime atv;
    convertTimeout(100,atv);
    freePageCondition.timed_wait(freePageGuard,atv);
    return NULL;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::collectStats(CacheStats &stats)
{
    stats.nHits = nCacheHits;
    stats.nRequests = nCacheRequests;
    stats.nVictimizations = nVictimizations;
    stats.nDirtyPages = nDirtyPages;
    stats.nPageReads = nPageReads;
    stats.nPageWrites = nPageWrites;
    stats.nRejectedPrefetches = nRejectedCachePrefetches;
    stats.nIoRetries = nIoRetries;
    stats.nSuccessfulPrefetches = nSuccessfulCachePrefetches;
    stats.nLazyWrites = nLazyWrites;
    stats.nLazyWriteCalls = nLazyWriteCalls;
    stats.nVictimizationWrites = nVictimizationWrites;
    stats.nCheckpointWrites = nCheckpointWrites;
    stats.nMemPagesAllocated = getAllocatedPageCount();
    stats.nMemPagesUnused = unmappedBucket.pageList.size();
    stats.nMemPagesMax = getMaxAllocatedPageCount();

    // NOTE:  nDirtyPages is not cumulative; don't clear it!
    nCacheHits.clear();
    nCacheRequests.clear();
    nVictimizations.clear();
    nPageReads.clear();
    nPageWrites.clear();
    nRejectedCachePrefetches.clear();
    nIoRetries.clear();
    nSuccessfulCachePrefetches.clear();
    nLazyWrites.clear();
    nLazyWriteCalls.clear();
    nVictimizationWrites.clear();
    nCheckpointWrites.clear();

    statsSinceInit.nHitsSinceInit += stats.nHits;
    statsSinceInit.nRequestsSinceInit += stats.nRequests;
    statsSinceInit.nVictimizationsSinceInit += stats.nVictimizations;
    statsSinceInit.nPageReadsSinceInit += stats.nPageReads;
    statsSinceInit.nPageWritesSinceInit += stats.nPageWrites;
    statsSinceInit.nRejectedPrefetchesSinceInit += stats.nRejectedPrefetches;
    statsSinceInit.nIoRetriesSinceInit += stats.nIoRetries;
    statsSinceInit.nSuccessfulPrefetchesSinceInit +=
        stats.nSuccessfulPrefetches;
    statsSinceInit.nLazyWritesSinceInit += stats.nLazyWrites;
    statsSinceInit.nLazyWriteCallsSinceInit += stats.nLazyWriteCalls;
    statsSinceInit.nVictimizationWritesSinceInit += stats.nVictimizationWrites;
    statsSinceInit.nCheckpointWritesSinceInit += stats.nCheckpointWrites;

    stats.nHitsSinceInit = statsSinceInit.nHitsSinceInit;
    stats.nRequestsSinceInit = statsSinceInit.nRequestsSinceInit;
    stats.nVictimizationsSinceInit = statsSinceInit.nVictimizationsSinceInit;
    stats.nPageReadsSinceInit = statsSinceInit.nPageReadsSinceInit;
    stats.nPageWritesSinceInit = statsSinceInit.nPageWritesSinceInit;
    stats.nRejectedPrefetchesSinceInit =
        statsSinceInit.nRejectedPrefetchesSinceInit;
    stats.nIoRetriesSinceInit =
        statsSinceInit.nIoRetriesSinceInit;
    stats.nSuccessfulPrefetchesSinceInit =
        statsSinceInit.nSuccessfulPrefetchesSinceInit;
    stats.nLazyWritesSinceInit = statsSinceInit.nLazyWritesSinceInit;
    stats.nLazyWriteCallsSinceInit = statsSinceInit.nLazyWriteCallsSinceInit;
    stats.nVictimizationWritesSinceInit =
        statsSinceInit.nVictimizationWritesSinceInit;
    stats.nCheckpointWritesSinceInit =
        statsSinceInit.nCheckpointWritesSinceInit;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::flushSomePages()
{
    // TODO:  parameterize
    uint nToFlush = std::min<uint>(5,nDirtyPages);
    if (!nToFlush) {
        // in case there aren't any dirty buffers to start with
        return;
    }

    // Only flush if we're within the dirty threshholds
    if (!inFlushMode) {
        if (nDirtyPages < dirtyHighWaterMark) {
            return;
        }
        inFlushMode = true;
    }
    if (nDirtyPages < dirtyLowWaterMark) {
        inFlushMode = false;
        return;
    }

    incrementStatsCounter(nLazyWriteCalls);
    uint nFlushed = 0;
    VictimSharedGuard victimSharedGuard(victimPolicy.getMutex());
    std::pair<DirtyVictimPageIterator,DirtyVictimPageIterator> victimRange(
        victimPolicy.getDirtyVictimRange());
    for (; victimRange.first != victimRange.second; ++(victimRange.first)) {
        PageT &page = *(victimRange.first);
        // if page mutex is unavailable, just skip it
        StrictMutexTryGuard pageGuard(page.mutex);
        if (!pageGuard.locked()) {
            continue;
        }
        if (!page.isDirty()) {
            continue;
        }
        if (page.isScratchLocked()) {
            // someone has the page scratch-locked
            continue;
        }
        if (!page.lock.waitFor(LOCKMODE_S_NOWAIT)) {
            // someone has the page write-locked
            continue;
        } else {
            // release our test lock just acquired
            page.lock.release(LOCKMODE_S);
        }
        if (page.pMappedPageListener &&
            !page.pMappedPageListener->canFlushPage(page))
        {
            continue;
        }
        incrementStatsCounter(nLazyWrites);
        // If the write request required retry, don't submit any
        // additional write requests
        if (!writePageAsync(page)) {
            break;
        }
        nFlushed++;
        if (nFlushed >= nToFlush) {
            break;
        }
    }
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::unmapPage(PageT &page,StrictMutexTryGuard &pageGuard, bool discard)
{
    assert(!page.nReferences);
    assert(pageGuard.locked());

    victimPolicy.notifyPageUnmap(page, discard);
    if (page.pMappedPageListener) {
        page.pMappedPageListener->notifyPageUnmap(page);
        page.pMappedPageListener = NULL;
    }
    if (page.isDirty()) {
        decrementCounter(nDirtyPages);
    }

    // NOTE:  to get the locking sequence safe for deadlock avoidance,
    // we're going to have to release the page mutex.  To indicate that the
    // page is being unmapped (so that no one else tries to lock it or
    // victimize it), we first clear the BlockId, saving it for our own use.
    BlockId blockId = page.getBlockId();
    page.blockId = NULL_BLOCK_ID;
    page.dataStatus = CachePage::DATA_INVALID;
    pageGuard.unlock();

    PageBucketT &bucket = getHashBucket(blockId);
    SXMutexExclusiveGuard bucketGuard(bucket.mutex);
    bool bFound = bucket.pageList.remove(page);
    assert(bFound);
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::unmapAndFreeDiscardedPage(PageT &page,StrictMutexTryGuard &pageGuard)
{
    while (page.isTransferInProgress()) {
        page.waitForPendingIO(pageGuard);
    }
    unmapPage(page,pageGuard,true);
    pageGuard.lock();
    assert(!page.nReferences);
    freePage(page);
}

template <class PageT,class VictimPolicyT>
PageT &CacheImpl<PageT,VictimPolicyT>
::mapPage(
    PageBucketT &bucket,PageT &page,BlockId blockId,
    MappedPageListener *pMappedPageListener,
    bool bPendingRead,bool bIncRef)
{
    assert(!page.hasBlockId());
    assert(!page.isDirty());
    assert(getDevice(CompoundId::getDeviceId(blockId)).get());
    assertCorrectBucket(bucket,blockId);

    // check existing pages in hash bucket in case someone else just mapped the
    // same page
    SXMutexExclusiveGuard bucketGuard(bucket.mutex);
    for (PageBucketIter iter(bucket.pageList); iter; ++iter) {
        StrictMutexGuard pageGuard(iter->mutex);
        if (iter->getBlockId() == blockId) {
            // blockId already mapped; discard new page and return existing page
            freePage(page);
            if (bIncRef) {
                iter->nReferences++;
            }
            bucketGuard.unlock();
            assert(pMappedPageListener == iter->pMappedPageListener);
            victimPolicy.notifyPageAccess(*iter, bIncRef);
            return *iter;
        }
    }

    // not found:  add new page instead
    StrictMutexGuard pageGuard(page.mutex);
    page.blockId = blockId;
    assert(!page.pMappedPageListener);
    page.pMappedPageListener = pMappedPageListener;
    if (bIncRef) {
        page.nReferences++;
    }
    if (bPendingRead) {
        page.dataStatus = CachePage::DATA_READ;
    }
    bucket.pageList.push_back(page);
    bucketGuard.unlock();
    victimPolicy.notifyPageMap(page, bIncRef);
    if (pMappedPageListener) {
        pMappedPageListener->notifyPageMap(page);
    }
    return page;
}

template <class PageT,class VictimPolicyT>
bool CacheImpl<PageT,VictimPolicyT>
::transferPageAsync(PageT &page)
{
    SharedRandomAccessDevice &pDevice =
        getDevice(CompoundId::getDeviceId(page.getBlockId()));
    RandomAccessRequest request;
    request.pDevice = pDevice.get();
    request.cbOffset = getPageOffset(page.getBlockId());
    request.cbTransfer = getPageSize();
    if (page.dataStatus == CachePage::DATA_WRITE) {
        request.type = RandomAccessRequest::WRITE;
    } else {
        assert(page.dataStatus == CachePage::DATA_READ);
        request.type = RandomAccessRequest::READ;
    }
    request.bindingList.push_back(page);
    bool rc = getDeviceAccessScheduler(*pDevice).schedule(request);
    if (!rc) {
        ioRetry();
    }
    return rc;
}

template <class PageT,class VictimPolicyT>
CacheAllocator &CacheImpl<PageT,VictimPolicyT>
::getAllocator() const
{
    return bufferAllocator;
}

template <class PageT,class VictimPolicyT>
inline bool CacheImpl<PageT,VictimPolicyT>
::readPageAsync(PageT &page)
{
    page.dataStatus = CachePage::DATA_READ;
    incrementStatsCounter(nPageReads);
    return transferPageAsync(page);
}

template <class PageT,class VictimPolicyT>
inline bool CacheImpl<PageT,VictimPolicyT>
::writePageAsync(PageT &page)
{
    assert(page.isDirty());
    if (page.pMappedPageListener) {
        assert(page.pMappedPageListener->canFlushPage(page));
        page.pMappedPageListener->notifyBeforePageFlush(page);
    }
    page.dataStatus = CachePage::DATA_WRITE;
    incrementStatsCounter(nPageWrites);
    if (!transferPageAsync(page)) {
        return false;
    } else {
        return true;
    }
}

template <class PageT,class VictimPolicyT>
inline FileSize CacheImpl<PageT,VictimPolicyT>
::getPageOffset(BlockId const &blockId)
{
    return ((FileSize) CompoundId::getBlockNum(blockId))
        * (FileSize) cbPage;
}

template <class PageT,class VictimPolicyT>
inline PageBucket<PageT> &CacheImpl<PageT,VictimPolicyT>
::getHashBucket(BlockId const &blockId)
{
    std::hash<BlockId> hasher;
    size_t hashCode = hasher(blockId);
    return *(pageTable[hashCode%pageTable.size()]);
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::assertCorrectBucket(PageBucketT &bucket,BlockId const &blockId)
{
    assert(&bucket == &(getHashBucket(blockId)));
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::freePage(PageT &page)
{
    SXMutexExclusiveGuard unmappedBucketGuard(unmappedBucket.mutex);
    unmappedBucket.pageList.push_back(page);
}

template <class PageT,class VictimPolicyT>
inline bool CacheImpl<PageT,VictimPolicyT>
::canVictimizePage(PageT &page)
{
    // NOTE:  the hasBlockId() check is to prevent us from trying to
    // victimize a page that is in transit between the free list and
    // a mapping; maybe such pages should have nReferences
    // non-zero instead?
    return page.hasBlockId()
        && !page.nReferences
        && !page.isTransferInProgress();
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::incrementCounter(AtomicCounter &x)
{
    ++x;
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::decrementCounter(AtomicCounter &x)
{
    --x;
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::incrementStatsCounter(AtomicCounter &x)
{
    incrementCounter(x);
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::decrementStatsCounter(AtomicCounter &x)
{
    decrementCounter(x);
}

FENNEL_END_NAMESPACE

#endif

// End CacheMethodsImpl.h

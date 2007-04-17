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
    pages(params.nMemPagesMax),
    bufferAllocator(
        pBufferAllocatorInit ?
        *pBufferAllocatorInit
        : *new VMAllocator(params.cbPage,0)),
    pBufferAllocator(pBufferAllocatorInit ? NULL : &bufferAllocator),
    timerThread(*this)
{
    cbPage = params.cbPage;
    pDeviceAccessScheduler = NULL;

    initializeStats();

    // allocate pages, adding all of them onto the free list and registering
    // them with victimPolicy
    for (uint i = 0; i < params.nMemPagesMax; i++) {
        PBuffer pBuffer = NULL;
        if (i < params.nMemPagesInit) {
            pBuffer = static_cast<PBuffer>(
                bufferAllocator.allocate());
        }
        PageT &page = *new PageT(*this,pBuffer);
        pages[i] = &page;
        if (pBuffer) {
            unmappedBucket.pageList.push_back(page);
        } else {
            unallocatedBucket.pageList.push_back(page);
        }
        victimPolicy.registerPage(page);
    }

    // initialize page hash table
    // NOTE: this is the size of the page hash table; 2N is for a 50%
    // load factor, and +1 is to avoid picking an even number
    // TODO:  use a static table of primes to pick the least-upper-bound prime
    pageTable.resize(2*pages.size()+1);
    for (uint i = 0; i < pageTable.size(); i++) {
        pageTable[i] = new PageBucketT();
    }

    pDeviceAccessScheduler = DeviceAccessScheduler::newScheduler(
        params.schedParams);
    
    // initialize null device
    registerDevice(
        NULL_DEVICE_ID,
        SharedRandomAccessDevice(
            new RandomAccessNullDevice()));

    idleFlushInterval = params.idleFlushInterval;
    if (idleFlushInterval) {
        timerThread.start();
    }
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
    statsSinceInit.nMemPagesAllocated = 0;
    statsSinceInit.nMemPagesUnused = 0;
    statsSinceInit.nMemPagesMax = 0;
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
        PageBucketMutator mutator(unallocatedBucket.pageList);
        for (; nMemPages < nMemPagesDesired; ++nMemPages) {
            PBuffer pBuffer = static_cast<PBuffer>(
                bufferAllocator.allocate());
            PageT *page = mutator.detach();
            assert(!page->pBuffer);
            page->pBuffer = pBuffer;
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
            try {
                bufferAllocator.deallocate(page->pBuffer);
            } catch (...) {
                // if the page buffer couldn't be deallocated, put it back
                // before reporting the error
                freePage(*page);
                throw;
            }
            page->pBuffer = NULL;
            // move to unallocatedBucket
            unallocatedBucket.pageList.push_back(*page);
        }
    }
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
    PageT *page = lookupPage(bucket,blockId);
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
        bufferAllocator.setProtection(page->pBuffer, cbPage, false);
#endif
    } else {
        // TODO jvs 7-Feb-2006:  protection for other cases
#ifdef DEBUG
        StrictMutexGuard pageGuard(page->mutex);
        if (page->nReferences == 1) {
            bufferAllocator.setProtection(page->pBuffer, cbPage, true);
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
        bufferAllocator.setProtection(page.pBuffer, cbPage, false);
        page.lock.release(lockMode,txnId);
    }
    page.nReferences--;
    if (!page.nReferences) {
        if (bFree) {
            // the page lock was acquired via lockScratch, so return the page
            // to the free list
            page.dataStatus = CachePage::DATA_INVALID;
            page.blockId = NULL_BLOCK_ID;
            freePage(page);
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
            victimPolicy.notifyPageAccess(*iter);
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
    PageT *page = lookupPage(bucket,blockId);
    if (!page) {
        // page is not mapped, so nothing to discard
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
    // set dirty early to avoid work on first call to getWritableData
    page->dataStatus = CachePage::DATA_DIRTY;
    CompoundId::setDeviceId(page->blockId,NULL_DEVICE_ID);
    CompoundId::setBlockNum(page->blockId,blockNum);
    
    return *page;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::prefetchPage(BlockId blockId,MappedPageListener *pMappedPageListener)
{
    assert(blockId != NULL_BLOCK_ID);
    if (isPageMapped(blockId)) {
        // already mapped:  either it's already fully read or someone
        // else has initiated a read; either way, nothing for us to do
        return;
    }
    PageT *page = findFreePage();
    if (!page) {
        // cache is low on free pages:  ignore prefetch hint
        return;
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
        readPageAsync(*page);
    } else {
        // forget unused free page, and don't bother with read since someone
        // else must already have kicked it off
        page = &mappedPage;
    }
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
                scheduler.schedule(request);
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
        scheduler.schedule(request);
    }
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
                    // initiate a flush
                    writePageAsync(page);
                }
            } else if (flushPhase == phaseWait) {
                while (page.dataStatus == CachePage::DATA_WRITE) {
                    page.waitForPendingIO(pageGuard);
                }

                // Reset the listener if called for by the original listener.
                // Note that by doing so, if we're later going to be unmapping
                // cache entries, we will not unmap this page because its
                // listener has changed.
                if (page.pMappedPageListener) {
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
            bufferAllocator.deallocate(pBuffer);
        }
        deleteAndNullify(pages[i]);
    }
}

template <class PageT,class VictimPolicyT>
PageT *CacheImpl<PageT,VictimPolicyT>
::lookupPage(PageBucketT &bucket,BlockId blockId)
{
    assertCorrectBucket(bucket,blockId);
    SXMutexSharedGuard bucketGuard(bucket.mutex);
    for (PageBucketIter iter(bucket.pageList); iter; ++iter) {
        StrictMutexGuard pageGuard(iter->mutex);
        if (iter->getBlockId() == blockId) {
            victimPolicy.notifyPageAccess(*iter);
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
                writePageAsync(page);
                continue;
            }
            // NOTE:  have to do this early since unmapPage will
            // call back into victimPolicy, which could deadlock
            victimSharedGuard.unlock();
            unmapPage(page,pageGuard);
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
    stats.nMemPagesAllocated = getAllocatedPageCount();
    stats.nMemPagesUnused = unmappedBucket.pageList.size();
    stats.nMemPagesMax = getMaxAllocatedPageCount();

    // NOTE:  nDirtyPages is not cumulative; don't clear it!
    nCacheHits.clear();
    nCacheRequests.clear();
    nVictimizations.clear();
    nPageReads.clear();
    nPageWrites.clear();

    statsSinceInit.nHitsSinceInit += stats.nHits;
    statsSinceInit.nRequestsSinceInit += stats.nRequests;
    statsSinceInit.nVictimizationsSinceInit += stats.nVictimizations;
    statsSinceInit.nPageReadsSinceInit += stats.nPageReads;
    statsSinceInit.nPageWritesSinceInit += stats.nPageWrites;

    stats.nHitsSinceInit = statsSinceInit.nHitsSinceInit;
    stats.nRequestsSinceInit = statsSinceInit.nRequestsSinceInit;
    stats.nVictimizationsSinceInit = statsSinceInit.nVictimizationsSinceInit;
    stats.nPageReadsSinceInit = statsSinceInit.nPageReadsSinceInit;
    stats.nPageWritesSinceInit = statsSinceInit.nPageWritesSinceInit;
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
    uint nFlushed = 0;
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
        writePageAsync(page);
        nFlushed++;
        if (nFlushed >= nToFlush) {
            break;
        }
    }
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
::unmapPage(PageT &page,StrictMutexTryGuard &pageGuard)
{
    assert(!page.nReferences);
    assert(pageGuard.locked());

    victimPolicy.notifyPageUnmap(page);
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
    unmapPage(page,pageGuard);
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
            victimPolicy.notifyPageAccess(*iter);
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
    victimPolicy.notifyPageMap(page);
    if (pMappedPageListener) {
        pMappedPageListener->notifyPageMap(page);
    }
    return page;
}

template <class PageT,class VictimPolicyT>
void CacheImpl<PageT,VictimPolicyT>
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
    getDeviceAccessScheduler(*pDevice).schedule(request);
}

template <class PageT,class VictimPolicyT>
CacheAllocator &CacheImpl<PageT,VictimPolicyT>
::getAllocator() const
{
    return bufferAllocator;
}

template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::readPageAsync(PageT &page)
{
    page.dataStatus = CachePage::DATA_READ;
    incrementStatsCounter(nPageReads);
    transferPageAsync(page);
}


template <class PageT,class VictimPolicyT>
inline void CacheImpl<PageT,VictimPolicyT>
::writePageAsync(PageT &page)
{
    assert(page.isDirty());
    if (page.pMappedPageListener) {
        assert(page.pMappedPageListener->canFlushPage(page));
        page.pMappedPageListener->notifyBeforePageFlush(page);
    }
    page.dataStatus = CachePage::DATA_WRITE;
    incrementStatsCounter(nPageWrites);
    transferPageAsync(page);
}

template <class PageT,class VictimPolicyT>
inline FileSize CacheImpl<PageT,VictimPolicyT>
::getPageOffset(BlockId const &blockId)
{
    return CompoundId::getBlockNum(blockId)*cbPage;
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

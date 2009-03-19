/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

#ifndef Fennel_CacheImpl_Included
#define Fennel_CacheImpl_Included

#include "fennel/cache/Cache.h"
#include "fennel/cache/CacheStats.h"
#include "fennel/common/IntrusiveList.h"
#include "fennel/common/AtomicCounter.h"
#include "fennel/synch/SXMutex.h"
#include "fennel/synch/TimerThread.h"
#include "fennel/cache/CacheAllocator.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/cache/CachePage.h"
#include "fennel/cache/PageBucket.h"
#include "fennel/cache/MappedPageListener.h"
#include "fennel/cache/PagePredicate.h"
#include "fennel/common/FileStatsTarget.h"

#include <vector>
#include <algorithm>
#include <boost/scoped_ptr.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * CacheImpl is a template implementation of the Cache interface.  It
 * collaborates with the types of its template parameters (PageT,
 * VictimPolicyT).  The realization of PageT must always
 * be derived from Page; it may have other inheritance requirements depending
 * on the realization for VictimPolicyT.
 *
 *<p>
 *
 * Synchronization within the cache accomplishes three distinct goals:
 *
 *<ul>
 * <li> protect the internal integrity of cache data structures
 * <li> implement the shared and exclusive page locking requested
 * externally by callers
 * <li> block callers while relevant pending I/O completes
 *</ul>
 *
 *<p>
 *
 * Synchronization occurs at several different levels:
 *<ul>
 *<li>Top-level state variables of the cache are protected by
 * cache-level mutexes, or by interlocked bus access on architectures which
 * support it.  Global waits (e.g. for free pages) are accomplished
 * via cache-level condition variables.
 *<li>Mapped pages are hashed into buckets based on their BlockIds.
 * Each bucket is protected by a bucket-level SXMutex.
 *<li>The internal state variables of each page are protected by a
 * page-level mutex.  Page-level waits (e.g. for I/O completion) are
 * accomplished via page-level condition variables.
 *<li>Each page also has an associated SXMutex which is used
 * to implement the shared and exclusive page locks advertised
 * in the Cache interface.
 *<li>VictimPolicyT may require synchronization on
 * its data structures.
 *<li>See also notes on class DeviceAccessScheduler.
 *</ul>
 *
 *<p>
 *
 * To avoid deadlock, lock acquisition order must follow these rules:
 *<ul>
 *<li>No other locks may be acquired while cache-level mutexes are held.
 *<li>Bucket locks may not be acquired when a page-level mutex is held.
 *<li>A page-level mutex may not be acquired when a VictimPolicyT lock is
 * held.
 *<li>The exception to the above rules is when tryacquire is called;
 * in this case, lock order may be violated since tryacquire cannot deadlock.
 *</ul>
 *
 *<p>
 *
 * Note that some of the code in CacheImpl (e.g. the methods lookupPage or
 * markPageDirty) could be moved to other classes such as PageBucket or
 * CachePage in standard object-oriented design.  However, this IS not and
 * SHOULD not be done.  Why?  Because the cache synchronization logic is
 * complex, so every effort should be made to keep it easy to understand,
 * verify, and debug.  Keeping method implementations as units rather than
 * distributed over a number of objects helps with this.
 *
 *<p>
 *
 * CacheImpl implements TimerThreadClient for taking repeated action on timer
 * callbacks (e.g. idle flush).
 *
 */
template <class PageT,class VictimPolicyT>
class CacheImpl : public Cache, private TimerThreadClient
{
    // convenience typedef
    typedef PageBucket<PageT> PageBucketT;
    typedef typename PageBucketT::PageListIter PageBucketIter;
    typedef typename PageBucketT::PageListMutator PageBucketMutator;

    /**
     * Collection of registered devices indexed by DeviceId; this
     * array is of fixed size, with a NULL slot indicating that the given
     * device ID has not been registered; this permits synchronization-free
     * access to the collection.
     */
    std::vector<SharedRandomAccessDevice> deviceTable;

    /**
     * Bucket of free unmapped pages whose buffers are still allocated.
     */
    PageBucketT unmappedBucket;

    /**
     * Bucket of free pages whose buffers are not allocated.
     */
    PageBucketT unallocatedBucket;

    /**
     * Array of PageBuckets indexed by BlockId hash code.  This hash table is
     * of fixed size, permitting synchronization-free access.
     */
    std::vector<PageBucketT *> pageTable;

    /**
     * Percentage of pages in the cache that must be dirty for lazy writes to
     * be initiated
     */
    uint dirtyHighWaterPercent;

    /**
     * Percentage of pages in the cache that must be dirty for lazy writes to
     * be suspended
     */
    uint dirtyLowWaterPercent;

    /**
     * Number of dirty pages in the cache corresponding to the high-water
     * percentage
     */
    uint dirtyHighWaterMark;

    /**
     * Number of dirty pages in the cache corresponding to the low-water
     * percentage
     */
    uint dirtyLowWaterMark;

    /**
     * Used by the lazy writer thread to indicate that the high-water dirty
     * threshhold has been reached and page flushes should continue until
     * the low-water threshhold is reached
     */
    bool inFlushMode;

    /**
     * See CacheStats::nHits.
     */
    AtomicCounter nCacheHits;

    /**
     * See CacheStats::nRequests.
     */
    AtomicCounter nCacheRequests;

    /**
     * See CacheStats::nVictimizations.
     */
    AtomicCounter nVictimizations;

    /**
     * See CacheStats::nDirtyPages.  This is actually used for more than
     * just statistics; the idle flush thread uses this in determining its
     * activity.
     */
    AtomicCounter nDirtyPages;

    /**
     * See CacheStats::nPageReads.
     */
    AtomicCounter nPageReads;

    /**
     * See CacheStats::nPageWrites.
     */
    AtomicCounter nPageWrites;

    /**
     * See CacheStats::nRejectedPrefetches.
     */
    AtomicCounter nRejectedCachePrefetches;

    /**
     * See CacheStats::nIoRetries.
     */
    AtomicCounter nIoRetries;

    /**
     * See CacheStats::nSuccessfulPrefetches.
     */
    AtomicCounter nSuccessfulCachePrefetches;

    /**
     * See CacheStats::nLazyWrites.
     */
    AtomicCounter nLazyWrites;

    /**
     * See CacheStats::nLazyWriteCalls.
     */
    AtomicCounter nLazyWriteCalls;

    /**
     * See CacheStats::nVictimizationWrites.
     */
    AtomicCounter nVictimizationWrites;

    /**
     * See CacheStats::nCheckpointWrites.
     */
    AtomicCounter nCheckpointWrites;

    /**
     * Accumulated state for all counters which are tracked since cache
     * initialization.  Other fields are unused.
     */
    CacheStats statsSinceInit;

    /**
     * Mutex coupled with freePageCondition.
     */
    StrictMutex freePageMutex;

    /**
     * Condition variable used for notification of free page availability.
     */
    LocalCondition freePageCondition;

    /**
     * A fixed-size vector of pointers to cache pages; we can get away with
     * this because currently the number of pages is fixed at initialization.
     * This permits synchronization-free access to the collection.
     */
    std::vector<PageT *> pages;

    /**
     * Scheduler for asynchronous I/O.
     */
    DeviceAccessScheduler *pDeviceAccessScheduler;

    /**
     * Source of buffer memory.
     */
    CacheAllocator &bufferAllocator;

    /**
     * Set only if bufferAllocator is owned by this cache.
     */
    boost::scoped_ptr<CacheAllocator> pBufferAllocator;

    /**
     * The realization for the VictimPolicy model.  See
     * LRUVictimPolicy for general information on the collaboration between
     * CacheImpl and victimPolicy.
     */
    VictimPolicyT victimPolicy;

    /**
     * Thread for running idle flush.
     */
    TimerThread timerThread;

    /**
     * @see CacheParams
     */
    uint idleFlushInterval;

    /**
     * Maximum number of outstanding pre-fetch requests
     */
    uint prefetchPagesMax;

    /**
     * The number of successful pre-fetches that have to occur before the
     * pre-fetch rate is throttled back up, in the event that it has been
     * throttled down due to rejected requests
     */
    uint prefetchThrottleRate;

    /**
     * Flush state used inside of checkpointPages.
     */
    enum FlushPhase {
        phaseSkip, phaseInitiate, phaseWait
    };

    // A class used as VictimPolicyT is required to define nested types
    // PageIterator and SharedGuard via which CacheImpl can safely iterate
    // over victim candidates.  Within CacheImpl code, these are referred to
    // via private typedefs for brevity.
    typedef typename VictimPolicyT::PageIterator VictimPageIterator;
    typedef typename VictimPolicyT::DirtyPageIterator DirtyVictimPageIterator;
    typedef typename VictimPolicyT::SharedGuard VictimSharedGuard;
    typedef typename VictimPolicyT::ExclusiveGuard VictimExclusiveGuard;

// ----------------------------------------------------------------------
// CacheImpl internal helper methods
// ----------------------------------------------------------------------

    /**
     * Finds a page by BlockId within a particular bucket.  If found,
     * waits for any pending read and then increments the page reference count;
     * also notifies victimPolicy of the page access.
     *
     * @param bucket the bucket to search; must be the same as
     * getHashBucket(blockId)
     *
     * @param blockId the BlockId of the page to look for
     * @param pin if true, the page will be pinned in the cache
     *
     * @return the page found, or NULL if not present
     */
    PageT *lookupPage(PageBucketT &bucket,BlockId blockId,bool pin);

    /**
     * Obtains a free page (either from the free queue or by victimizing
     * a mapped page); if none is available, suspends for
     * a little while to help reduce cache load.  The returned page
     * is clean, unmapped, and ready to be remapped.
     *
     * @return the page obtained, or NULL if none available
     */
    PageT *findFreePage();

    /**
     * Initiates asynchronous writes for a few dirty pages which are the best
     * victimization candidates.
     */
    void flushSomePages();

    /**
     * Performs an asynchronous I/O operation on the given page.  The page's ID
     * and dataStatus should already be defined.
     *
     * @param page page to transfer
     *
     * @return true if the aynch I/O operation did not require retry
     */
    bool transferPageAsync(PageT &page);

    /**
     * Reads the given page asynchronously.
     *
     * @param page page to read
     *
     * @return true if the asynch page request did not require retry
     */
    bool readPageAsync(PageT &page);

    /**
     * Writes the given page asynchronously.
     *
     * @param page page to write
     *
     * @return true if the asynch page request did not require retry
     */
    bool writePageAsync(PageT &page);

    /**
     * Translates a BlockId into the byte offset of the corresponding device
     * block.
     *
     * @param blockId the BlockId to translate
     *
     * @return byte offset within device
     */
    FileSize getPageOffset(BlockId const &blockId);

    /**
     * Gets the hash bucket containing the given BlockId.
     *
     * @param blockId the BlockId being sought
     *
     * @return the containing bucket
     */
    PageBucketT &getHashBucket(BlockId const &blockId);

    /**
     * Verifies the match between a PageBucket and BlockId.  Some methods
     * (e.g. lockPage) precompute the correct bucket for a BlockId parameter
     * and then make calls to helper methods (e.g. lookupPage, mapPage)
     * which can skip the bucket lookup.  This assertion
     * allows the helper methods to verify the calling logic.
     */
    void assertCorrectBucket(PageBucketT &bucket,BlockId const &blockId);

    /**
     * Unmaps a currently mapped page, but does not add it to the free list.
     * Also notifies victimPolicy of the unmapping.  The page must have
     * no outstanding references.
     *
     * @param page a currently mapped page to be unmapped
     *
     * @param guard a guard on page.mutex, which must already be held by the
     * calling thread when this method is invoked, and which will be released
     * when the method returns
     *
     * @param discard if true, the page is being discarded from the cache
     */
    void unmapPage(PageT &page,StrictMutexTryGuard &guard,bool discard);

    /**
     * Unmaps a page being discarded and adds it to unmappedBucket.
     * Also notifies victimPolicy of the unmapping.  If the page is
     * dirty, it is not flushed.  However, any pending I/O is allowed to
     * complete before discard.
     *
     * @param page a currently mapped page to be unmapped
     *
     * @param guard a guard on page.mutex, which must already be held by the
     * calling thread when this method is invoked, and which will be released
     * and reacquired during the method's execution
     */
    void unmapAndFreeDiscardedPage(
        PageT &page,
        StrictMutexTryGuard &guard);

    /**
     * Maps a page if it is not already mapped (and notifies victimPolicy of the
     * page mapping); otherwise, finds the existing mapping (and notifies
     * victimPolicy of the page access).
     *
     * @param bucket the bucket to contain the Page; must be
     * the same as getHashBucket(blockId)
     *
     * @param newPage a free page to map (previously obtained from
     * findFreePage); if mapPage finds an existing mapping, newPage is
     * automatically freed
     *
     * @param blockId the BlockId to be mapped
     *
     * @param pMappedPageListener the MappedPageListener to be associated with
     * the mapped page
     *
     * @param bPendingRead true if a newly mapped page should be marked
     * for a pending read (this is ignored if the page was already mapped)
     *
     * @param bIncRef true if the returned page should have its
     * reference count incremented as a side effect
     *
     * @return newPage if no mapping already exists for blockId; otherwise,
     * the existing mapped page
     */
    PageT &mapPage(
        PageBucketT &bucket,PageT &newPage,BlockId blockId,
        MappedPageListener *pMappedPageListener,
        bool bPendingRead = true,bool bIncRef = true);

    /**
     * Places an unmapped page in unmappedBucket, making it available for
     * the next call to findFreePage.
     *
     * @param page the page to be freed
     */
    void freePage(PageT &page);

    /**
     * Decides whether a page can be victimized.  The page must be mapped,
     * have no references, and no pending I/O.
     *
     * @param page the page to test; the page's mutex must already
     * be held by the calling thread
     *
     * @return true if the page can be victimized
     */
    bool canVictimizePage(PageT &page);

    /**
     * Increments a counter variable safely.
     *
     * @param x reference to counter to be updated
     */
    void incrementCounter(AtomicCounter &x);

    /**
     * Decrements a counter variable safely.
     *
     * @param x reference to counter to be updated
     */
    void decrementCounter(AtomicCounter &x);

    /**
     * Increments a statistical counter.  Can be defined to NOP to increase
     * cache performance if statistics aren't important.
     *
     * @param x reference to counter to be updated
     */
    void incrementStatsCounter(AtomicCounter &x);

    /**
     * Decrements a statistical counter.  Can be defined to NOP to increase
     * cache performance if statistics aren't important.
     *
     * @param x reference to counter to be updated
     */
    void decrementStatsCounter(AtomicCounter &x);

    /**
     * Clears stats which are tracked since initialization.
     */
    void initializeStats();

    /**
     * Handles initial allocation of pages and attempts to handle any
     * associated out-of-memory errors.
     */
    void allocatePages(CacheParams const &params);

    /**
     * Updates counters indicating a successful pre-fetch has occurred.
     */
    void successfulPrefetch();

    /**
     * Updates counters indicating a pre-fetch was rejected.
     */
    void rejectedPrefetch();

    /**
     * Updates counters indicating I/O retry was required
     */
    void ioRetry();

    /**
     * Calculates the number of dirty pages corresponding to the high and low
     * water marks.
     *
     * @param nCachePages number of cache pages
     */
    void calcDirtyThreshholds(uint nCachePages);

// ----------------------------------------------------------------------
// Implementation of private Cache interface (q.v.)
// ----------------------------------------------------------------------

    void markPageDirty(CachePage &page);
    void notifyTransferCompletion(CachePage &,bool);

// ----------------------------------------------------------------------
// Implementation of TimerThreadClient interface (q.v.)
// ----------------------------------------------------------------------

    virtual uint getTimerIntervalMillis();
    virtual void onTimerInterval();

protected:
    void closeImpl();

public:
// ----------------------------------------------------------------------
// Implementation of public Cache interface (q.v.)
// ----------------------------------------------------------------------
    CacheImpl(
        CacheParams const &,
        CacheAllocator * = NULL);
    virtual void setAllocatedPageCount(uint nMemPages);
    virtual uint getAllocatedPageCount();
    virtual uint getMaxAllocatedPageCount();
    virtual PageT *lockPage(
        BlockId blockId,LockMode lockMode,bool readIfUnmapped,
        MappedPageListener *pMappedPageListener,TxnId txnId);
    virtual PageT &lockScratchPage(BlockNum blockNum);
    virtual void discardPage(BlockId blockId);
    virtual uint checkpointPages(
        PagePredicate &pagePredicate,CheckpointType checkpointType);
    virtual void collectStats(CacheStats &stats);
    virtual void registerDevice(
        DeviceId deviceId,SharedRandomAccessDevice pDevice);
    virtual void unregisterDevice(DeviceId deviceId);
    virtual SharedRandomAccessDevice &getDevice(DeviceId deviceId);
    virtual bool prefetchPage(
        BlockId blockId,MappedPageListener *pMappedPageListener);
    virtual void prefetchBatch(
        BlockId blockId,uint nPages,MappedPageListener *pMappedPageListener);
    virtual void flushPage(CachePage &page,bool async);
    virtual void unlockPage(CachePage &page,LockMode lockMode,TxnId txnId);
    virtual void nicePage(CachePage &page);
    virtual bool isPageMapped(BlockId blockId);
    virtual CacheAllocator &getAllocator() const;
    virtual void getPrefetchParams(
        uint &prefetchPagesMax,
        uint &prefetchThrottleRate);
    virtual DeviceAccessScheduler &getDeviceAccessScheduler(
        RandomAccessDevice &)
    {
        return *pDeviceAccessScheduler;
    }

};

FENNEL_END_NAMESPACE

#include "fennel/cache/CacheMethodsImpl.h"

#endif

// End CacheImpl.h

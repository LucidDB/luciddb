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

#ifndef Fennel_Cache_Included
#define Fennel_Cache_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/cache/CacheAccessor.h"
#include "fennel/common/StatsSource.h"

#include <boost/enable_shared_from_this.hpp>

FENNEL_BEGIN_NAMESPACE

class CacheParams;
class PagePredicate;
class CacheAllocator;
class CacheStats;

/**
 * Cache defines an abstract interface for caching pages of devices.  This
 * interface does not dictate how cache buffers are allocated, only
 * how they are accessed.  A Cache keeps track of a collection of registered
 * devices; only pages of registered devices may be accessed through
 * the cache.
 *
 *<p>
 *
 * Note that all concurrency control is performed within the implementations
 * of these methods, so there is never any need to acquire any kind of
 * lock object before calling them.  However, some methods are themselves
 * synchronization points, so callers should be aware of the potential
 * for deadlock.
 */
class Cache
    : public ClosableObject,
        public CacheAccessor, public StatsSource,
        public boost::enable_shared_from_this<Cache>
{
    friend class CachePage;

protected:
    uint cbPage;

public:
    /**
     * The DeviceId assigned to the instance of RandomAccessNullDevice
     * associated with every cache.  This device is automatically registered
     * when the cache is opened and unregistered when the cache is closed.
     * Scratch pages have this DeviceId in their BlockIds, but no real blocks
     * can ever be mapped to this device.
     */
    static const DeviceId NULL_DEVICE_ID;
    
    /**
     * Factory method.  This creates a cache which uses LRUVictimPolicy.
     * To create a cache with custom policies, include CacheImpl.h and
     * instantiate CacheImpl directly.
     *
     * @param cacheParams parameters to use to instantiate this cache
     *
     * @param bufferAllocator allocator to use for obtaining buffer memory;
     * NULL indicates use a private VMAllocator without mlocking
     *
     * @return new Cache
     */
    static SharedCache newCache(
        CacheParams const &cacheParams,
        CacheAllocator *bufferAllocator = NULL);

    /**
     * Destructor.  Should not be called directly; use close instead.  All
     * devices must already have been unregistered.
     */
    virtual ~Cache();

    /**
     * Resizes this cache.  If nMemPages is greater than the number of page
     * buffers currently allocated, allocates more.  If less, frees some
     * (victimizing as necessary).
     *
     * @param nMemPages desired buffer allocation count; must be less than the
     * nMemPagesMax specified when the cache was created, and also less than
     * the current clean page target
     */
    virtual void setAllocatedPageCount(uint nMemPages) = 0;

    /**
     * Gets a count of how many pages currently have allocated buffers.
     *
     * @return the current count
     */
    virtual uint getAllocatedPageCount() = 0;

    /**
     * Gets a snapshot of cache activity; as a side-effect, clears
     * cumulative performance counters.
     *
     * @param stats receives the snapshot
     */
    virtual void collectStats(CacheStats &stats) = 0;

// ----------------------------------------------------------------------
// Accessor Methods
// ----------------------------------------------------------------------
    
    /**
     * @return the size of cached pages (in bytes)
     */
    uint getPageSize() const
    {
        return cbPage;
    }

// ----------------------------------------------------------------------
// Device Registration and Operations
// ----------------------------------------------------------------------
    
    /**
     * Registers the given device with the Cache; must be called exactly
     * once before any other caching operations can be requested for pages of
     * this device.
     *
     * @param deviceId the ID of the device to be registered
     *
     * @param pDevice the device to be registered
     */
    virtual void registerDevice(
        DeviceId deviceId,SharedRandomAccessDevice pDevice) = 0;

    /**
     * Unregisters the given device from the Cache, asserting
     * that no pages remain mapped to the specified device.
     *
     * @param deviceId the ID of the device to be unregistered
     */
    virtual void unregisterDevice(
        DeviceId deviceId) = 0;

    /**
     * Dereferences a device ID to the registered object which represents it.
     *
     * @param deviceId the ID of the device of interest
     * @return the device, or NULL if no such device is registered with
     * this Cache
     */
    virtual SharedRandomAccessDevice &getDevice(DeviceId deviceId) = 0;

// ----------------------------------------------------------------------
// Global Operations on Pages; also see CacheAccessor methods
// ----------------------------------------------------------------------
    
    /**
     * Flushes and/or unmaps selected pages.
     *
     * @param pagePredicate caller-provided interface for deciding which pages
     * should be checkpointed; the given PagePredicate will be called for each
     * mapped page, and only those which satisfy the predicate will be affected
     * by the checkpoint (note that the page mutex is held for the duration of
     * the call, so implementations must take care to avoid deadlock)
     *
     * @param checkpointType type of checkpoint to execute
     *
     * @return number of pages for which pagePredicate returned true
     */
    virtual uint checkpointPages(
        PagePredicate &pagePredicate,
        CheckpointType checkpointType = CHECKPOINT_FLUSH_ALL) = 0;

    /**
     * Determines if a particular page is mapped.
     *
     * @param blockId BlockId of the page to test
     *
     * @return true if the page is currently mapped
     */
    virtual bool isPageMapped(BlockId blockId) = 0;

    /**
     * Allocates a free page buffer for scratch usage.  The returned page is
     * considered to be locked in exclusive mode but not mapped to any device.
     * To release the page, use unlock(LOCKMODE_X).  If no free pages are
     * available, blocks until one becomes free.
     *
     *<p>
     *
     * Although the page remains unmapped, its BlockId is set for the duration
     * of the lock.  The caller can supply a BlockNum, which need not be
     * unique.
     *
     * @param blockNum the block number to use when making up the Page's
     * BlockId; the device ID will be NULL_DEVICE_ID
     *
     * @return the locked page
     */
    virtual CachePage *lockScratchPage(BlockNum blockNum = 0) = 0;

    // partial implementation of CacheAccessor
    virtual SharedCache getCache();
    virtual uint getMaxLockedPages();
    virtual void setMaxLockedPages(uint nPages);
    virtual void setTxnId(TxnId); // ignored
    virtual TxnId getTxnId() const;

    // implementation of StatsSource
    virtual void writeStats(StatsTarget &target);
    
private:
    // notification methods called from friend Page
    virtual void notifyTransferCompletion(CachePage &,bool bSuccess) = 0;
    virtual void markPageDirty(CachePage &) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End Cache.h

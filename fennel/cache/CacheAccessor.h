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

#ifndef Fennel_CacheAccessor_Included
#define Fennel_CacheAccessor_Included

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class CachePage;
class MappedPageListener;

/**
 * CacheAccessor defines the subset of the Cache interface used for
 * accessing cache pages.  This allows page usage to be associated with
 * particular consumers such as connections, algorithms, etc.  The Cache
 * interface is derived from CacheAccessor, so any Cache implementation can
 * alway be used directly as a vanilla CacheAccessor.
 */
class CacheAccessor : public boost::noncopyable
{
public:
    virtual ~CacheAccessor();

    /**
     * Locks a page into memory with the specified concurrency mode.  When the
     * page contents are no longer needed, the caller must invoke the
     * unlockPage() method with the same concurrency mode to release it.  If
     * the desired page is already locked by another thread with an
     * incompatible concurrency mode, blocks until the page
     * becomes available (unless the lock mode is of the NoWait variety, in
     * which case returns NULL immediately).  Note that NoWait
     * locking only applies to lock contention, not I/O, so if an unmapped page
     * is locked in NoWait mode, blocks until the read completes.
     *
     *<p>
     *
     * The device referenced by the requested blockId must already be
     * registered with the cache and must remain registered for the duration of
     * the lock.
     *
     *<p>
     *
     * Notes on concurrency modes:
     *
     *<ul>
     *
     *<li>LOCKMODE_S: Acquire a shared lock on page.
     *
     *<li>LOCKMODE_X: Acquire an exclusive lock on page.  Note that this does
     * not mark the page dirty, although an exclusive lock is mandatory before a
     * page can be used for write access.  When an exclusive lock is already
     * held, the same thread may acquire a shared lock, but not vice versa (such
     * an upgrade would make deadlock possible).
     *
     *<li>LOCKMODE_S_NOWAIT: Attempt to acquire a shared lock on page, but fail
     * immediately if this would conflict with an exclusive lock already held by
     * another thread.
     *
     *<li>LOCKMODE_X_NOWAIT: Attempt to acquire an exclusive lock on page,
     * but fail immediately if this would conflict with a shared or exclusive
     * lock already held by another thread.
     *
     *</ul>
     *
     *
     * @param blockId the BlockId of the page to be locked
     *
     * @param lockMode the desired concurrency mode
     *
     * @param readIfUnmapped if true (the default) the page data is read as
     * part of mapping; if false, the page data is left invalid until first
     * write (used when allocating a new block with invalid contents)
     *
     * @param pMappedPageListener optional listener to receive notifications
     * when this page is written; if specified, it must match all prior and
     * subsequent lock requests for the same page mapping
     *
     * @param txnId optional TxnId to associate with lock;
     * default is IMPLICIT_TXN_ID, which uses current thread ID
     * as an implicit TxnId
     *
     * @return the locked CachePage, or NULL if a NoWait attempt failed
     */
    virtual CachePage *lockPage(
        BlockId blockId,
        LockMode lockMode,
        bool readIfUnmapped = true,
        MappedPageListener *pMappedPageListener = NULL,
        TxnId txnId = IMPLICIT_TXN_ID) = 0;

    /**
     * Releases lock held on page.
     *
     * @param page the page to be unlocked
     *
     * @param lockMode must correspond to value passed to Cache::lockPage;
     * however, for pages locked with NOWAIT, the equivalent unlock type
     * should be normal (e.g. LOCKMODE_S instead of LOCKMODE_S_NOWAIT)
     *
     * @param txnId must correspond to value passed to Cache::lockPage
     */
    virtual void unlockPage(
        CachePage &page,
        LockMode lockMode,
        TxnId txnId = IMPLICIT_TXN_ID) = 0;

    /**
     * Unmaps a page from the cache if already mapped, discarding its contents
     * if dirty.  The caller must ensure that no other thread has the page
     * locked.
     *
     * @param blockId the BlockId of the page to be discarded
     */
    virtual void discardPage(BlockId blockId) = 0;

    /**
     * Hints that a page should be prefetched in preparation for a
     * future lock request.
     *
     * @param blockId the BlockId of the page to be prefetched
     *
     * @param pMappedPageListener optional listener to receive notifications
     * when this page is written; if specified, it must match all prior and
     * subsequent lock requests for the same page mapping
     *
     * @return true if the pre-fetch request was successful
     */
    virtual bool prefetchPage(
        BlockId blockId,
        MappedPageListener *pMappedPageListener = NULL) = 0;

    /**
     * Hints that a contiguous run of pages should be prefetched.
     *
     * @param blockId the BlockId of the first page to be prefetched; more
     * will be prefetched depending on the configured batch size
     *
     * @param nPages number of pages in batch
     *
     * @param pMappedPageListener optional listener to receive notifications
     * when this page is written; if specified, it must match all prior and
     * subsequent lock requests for the same page mapping
     */
    virtual void prefetchBatch(
        BlockId blockId,uint nPages,
        MappedPageListener *pMappedPageListener = NULL) = 0;

    /**
     * Forces the contents of a dirty page to its mapped location.  Page must
     * already be locked in exclusive mode.  For an asynchronous flush,
     * the caller must ensure that the page contents remain unchanged
     * until the flush completes.
     *
     * @param page the page to be flushed
     *
     * @param async true to schedle async write and return immediately; false
     * to wait for write to complete
     */
    virtual void flushPage(CachePage &page,bool async) = 0;

    /**
     * Marks a page as nice, indicating that it is very unlikely the page's
     * mapping will be needed again any time soon, so it is a good candidate
     * for victimization.
     *
     * @param page the page to be marked
     */
    virtual void nicePage(CachePage &page) = 0;

    /**
     * @return the page lock quota on this accessor
     */
    virtual uint getMaxLockedPages() = 0;

    /**
     * Sets the page lock quota on this accessor.  Ignored
     * for accessor implementations that don't support quotas.
     *
     * @param nPages new quota
     */
    virtual void setMaxLockedPages(uint nPages) = 0;

    /**
     * @return the underlying Cache accessed by this CacheAccessor
     */
    virtual SharedCache getCache() = 0;

    /**
     * Sets a default TxnId to use for locking pages (to be used when
     * IMPLICIT_TXN_ID is specified).  Not all CacheAccessor implementations
     * support this behavior.
     *
     * @param txnId new default txn ID
     */
    virtual void setTxnId(TxnId txnId) = 0;

    /**
     * @return default TxnId associated with this accessor
     */
    virtual TxnId getTxnId() const = 0;

    /**
     * Retrieves the current pre-fetch caching parameters that determine how
     * many pages should be pre-fetched and how often the pre-fetches should
     * occur.
     *
     * @param [out] prefetchPagesMax max number of outstanding pre-fetch pages
     *
     * @param [out] prefetchThrottleRate the number of successful pre-fetches
     * that have to occur before the pre-fetch rate is throttled back up,
     * in the event that it has been throttled down due to rejected requests
     */
    virtual void getPrefetchParams(
        uint &prefetchPagesMax,
        uint &prefetchThrottleRate) = 0;
};

FENNEL_END_NAMESPACE

#endif

// End CacheAccessor.h

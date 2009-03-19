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

#ifndef Fennel_CachePage_Included
#define Fennel_CachePage_Included

#include "fennel/synch/SynchObj.h"
#include "fennel/synch/SXMutex.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CacheAllocator.h"
#include "fennel/common/SysCallExcn.h"

FENNEL_BEGIN_NAMESPACE

class Cache;
class MappedPageListener;

template <class PageT,class VictimPolicyT>
class CacheImpl;

/**
 * Embedded link class for PageBucket lists.
 */
class PageBucketListNode : public IntrusiveListNode
{
};

/**
 * CachePage is a descriptor for the state of a page of cache memory.  Once a
 * CachePage has been mapped to some block of a Device and locked, its data can
 * be read or written with the getReadableData() and getWritableData() member
 * functions.
 */
class CachePage
    : public PageBucketListNode, private RandomAccessRequestBinding
{
public:
    /**
     * Enumeration of possible status of page data.  Order matters
     * (valid data states are grouped together, as are I/O states).
     */
    enum DataStatus {
        /**
         * The page contents are invalid (either unmapped or newly allocated).
         */
        DATA_INVALID,

        /**
         * The last transfer failed (so the page contents are invalid).
         */
        DATA_ERROR,

        /**
         * No transfer is in progress, and the page contains valid, unmodified
         * data.
         */
        DATA_CLEAN,

        /**
         * No transfer is in progress, and the page contains valid, dirty data.
         */
        DATA_DIRTY,

        /**
         * A disk write is in progress (so the page contains valid data).
         */
        DATA_WRITE,

        /**
         * A disk read is in progress (so the page does not yet contain valid
         * data).
         */
        DATA_READ
    };

private:
    template <class PageT,class VictimPolicyT>
    friend class CacheImpl;

// ----------------------------------------------------------------------
// State variables
// ----------------------------------------------------------------------
    /**
     * @see getCache()
     */
    Cache &cache;

    /**
     * Mutex protecting this Page's state variables.  This is only held
     * internally by CacheImpl for very short durations.
     */
    StrictMutex mutex;

    /**
     * Synchronization object used to implement shared and exclusive locks.
     * This is held for long durations (across cache lock/unlock calls).
     */
    SXMutex lock;

    /**
     * Allocated buffer memory, or NULL if page is currently unallocated.
     */
    PBuffer pBuffer;

    // NOTE: the data members below should only be accessed while the
    // page mutex is held.  This includes indirect access via accessors such as
    // getBlockId().

    /**
     * The BlockId to which this page is currently mapped,
     * or NULL_BLOCK_ID if unmapped.
     */
    BlockId blockId;

    /**
     * Reference count used to pin page so that it can't be victimized; this is
     * equal to at least the number of locks currently held on this page, but
     * may be higher due to the presence of threads which are still in the
     * process of locking the page or otherwise manipulating it.
     */
    uint nReferences;

    /**
     * Condition variable used for notification of pending I/O completion.
     * This is coupled with mutex.
     */
    LocalCondition ioCompletionCondition;

    /**
     * Listener to receive notifications of page writes, or NULL if no listener
     * defined.
     */
    MappedPageListener *pMappedPageListener;

    /**
     * Status of page data.
     */
    DataStatus dataStatus;

// ----------------------------------------------------------------------
// Methods internal to CacheImpl
// ----------------------------------------------------------------------
    /**
     * @return is the BlockId for this page currently set?
     */
    bool hasBlockId() const
    {
        return blockId != NULL_BLOCK_ID;
    }

    /**
     * Waits for pending I/O to complete.  Note that this wraps a condition
     * variable, which is subject to spurious wakeups; for this reason, the
     * caller MUST enclose calls to this method in a while loop.
     */
    void waitForPendingIO(StrictMutexGuard &guard)
    {
        ioCompletionCondition.wait(guard);
    }

    /**
     * Waits for pending I/O to complete while holding a try-mutex.
     */
    void waitForPendingIO(StrictMutexTryGuard &guard)
    {
        ioCompletionCondition.wait(guard);
    }

    /**
     * @return true if an exclusive lock is held on this page by some thread
     */
    bool isExclusiveLockHeld() const
    {
        // REVIEW:  should make sure lock is held by this thread?
        return isScratchLocked() || lock.isLocked(LOCKMODE_X);
    }

// ----------------------------------------------------------------------
// Implementation of RandomAccessRequestBinding interface (q.v.)
// ----------------------------------------------------------------------
    virtual void notifyTransferCompletion(bool bSuccess);
    virtual PBuffer getBuffer() const;
    virtual uint getBufferSize() const;

public:
    explicit CachePage(Cache &,PBuffer);
    virtual ~CachePage();

    /**
     * @return the cache which manages this page
     */
    Cache &getCache()
    {
        return cache;
    }

    /**
     * @return are the in-memory page contents different from those in
     * the corresponding block on disk?
     */
    bool isDirty() const
    {
        return dataStatus == DATA_DIRTY;
    }

    /**
     * @return is I/O currently in progress on this page?
     */
    bool isTransferInProgress() const
    {
        return (dataStatus >= DATA_WRITE);
    }

    /**
     * @return is the page data valid?
     */
    bool isDataValid() const
    {
        return (dataStatus >= DATA_CLEAN) && (dataStatus <= DATA_WRITE);
    }

    /**
     * @return did the read required for mapping fail?
     */
    bool isDataError() const
    {
        return (dataStatus == DATA_ERROR);
    }

    /**
     * @return the mapped BlockId
     */
    BlockId getBlockId() const
    {
        return blockId;
    }

    /**
     * Obtains a const pointer to the page contents.  The page must be locked in
     * shared or exclusive mode first.  The number of valid bytes returned
     * depends on the page size.
     *
     * @return pointer to the page contents (const to prevent accidental
     * modification)
     */
    PConstBuffer getReadableData() const
    {
        assert(isDataValid());
        assert(lock.isLocked(LOCKMODE_S) || isExclusiveLockHeld());
        return pBuffer;
    }

    /**
     * Obtains a writable pointer to the page contents, marking the page
     * dirty.  The page must be locked in exclusive mode first.  The number
     * of valid bytes returned depends on the page size.
     *
     * @return pointer to the page contents
     */
    PBuffer getWritableData()
    {
        assert(isExclusiveLockHeld());
        assert(!isDataError());
        assert(!isTransferInProgress());
        // REVIEW:  is thread-safety ever an issue here?  If so, it's also an
        // issue for the previous assertions.
        if (!isDirty()) {
            getCache().markPageDirty(*this);
        }
        return pBuffer;
    }

    /**
     * @return true if page is currently locked as scratch memory
     */
    bool isScratchLocked() const;

    /**
     * @return the MappedPageListener associated with this page
     */
    MappedPageListener *getMappedPageListener() const
    {
        assert(hasBlockId());
        return pMappedPageListener;
    }

    /**
     * Attempts to upgrade from LOCKMODE_S (which caller must already have
     * acquired) to LOCKMODE_X.  This is a NOWAIT operation; it fails
     * immediately if any other thread already holds a lock on the same page,
     * or when a transfer is already in progress.
     *
     * @return true if upgrade succeeded
     */
    bool tryUpgrade(TxnId txnId)
    {
        StrictMutexGuard pageGuard(mutex);
        if (isTransferInProgress()) {
            return false;
        }
#ifdef DEBUG
        int errorCode;
        if (getCache().getAllocator().setProtection(
                pBuffer, getCache().getPageSize(), false, &errorCode))
        {
            throw SysCallExcn("memory protection failed", errorCode);
        }
#endif
        return lock.tryUpgrade(txnId);
    }

    /**
     * Upgrades from LOCKMODE_S (which caller must already have
     * acquired) to LOCKMODE_X.  This is a WAIT operation if a page transfer
     * is in progress.  It's assumed that there are no other threads holding
     * a lock on the same page.
     */
    void upgrade(TxnId txnId)
    {
        StrictMutexGuard pageGuard(mutex);
        while (isTransferInProgress()) {
            waitForPendingIO(pageGuard);
        }
#ifdef DEBUG
        int errorCode;
        if (getCache().getAllocator().setProtection(
                pBuffer, getCache().getPageSize(), false, &errorCode))
        {
            throw SysCallExcn("memory protection failed", errorCode);
        }
#endif
        bool rc = lock.tryUpgrade(txnId);
        assert(rc);
    }

    /**
     * Swaps this page's buffer with another page.  You almost certainly
     * shouldn't be calling this directly (see SegPageLock::swapBuffers).
     *
     * @param other page to swap with
     */
    void swapBuffers(CachePage &other);
};

FENNEL_END_NAMESPACE

#endif

// End CachePage.h

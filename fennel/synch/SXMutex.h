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

#ifndef Fennel_SXMutex_Included
#define Fennel_SXMutex_Included

#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/synch/LockHolderId.h"
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

// NOTE jvs 24-Nov-2004:  It would be nice to replace SXMutex with
// boost::read_write_mutex.  However, it is a lot of work to
// get this right, because the boost design is intended for short-duration
// locks, whereas Fennel uses long-duration locks for cache pages.
// Also, the boost implementation does not yet take advantage
// of OS support for these primitives, so this custom implementation is
// at least as efficient.

// NOTE jvs 2-Jun-2007:  And it turns out that there were big problems
// with boost::read_write_mutex, so it has been removed from the Boost
// thread library, at least for now.

/**
 * An SXMutex implements a standard readers/writers exclusion scheme: any
 * number of shared-lock threads may hold the lock at one time, during which
 * time exclusive-lock threads are blocked; only one exclusive-lock thread may
 * hold the lock at a time, during which all other lock-requesting threads are
 * blocked.
 *
 *<p>
 *
 * Note on nomenclature: RWLock is a more standard name.  However, this
 * synchronization object is used in places where there's not a direct
 * correlation between shared/read and exclusive/write.  (For example, for a
 * checkpoint lock, writer threads take a shared lock and the checkpointing
 * thread takes an exclusive lock).  And "mutex" is more specific than "lock",
 * which is used in boost in the sense of a guard.
 */
class SXMutex : public SynchMonitoredObject
{
public:
    /**
     * Enumeration of available scheduling policies.
     */
    enum SchedulingPolicy
    {
        /**
         * Scheduling is determined by the OS with no preferences given.  This
         * may lead to starvation of exclusive lock requests.
         */
        SCHEDULE_DEFAULT,

        /**
         * Exclusive locks are always favored, so once a thread is waiting for
         * an exclusive lock, no new shared locks can be obtained.  This may
         * lead to starvation of shared lock requests.  Deadlock is also a
         * danger if multiple shared locks are requested by the same thread,
         * since an exclusive request may begin after the first request is
         * granted, blocking the second request.
         */
        SCHEDULE_FAVOR_EXCLUSIVE
    };

    explicit SXMutex();
    ~SXMutex();

    bool waitFor(
        LockMode lockMode,uint iTimeout = ETERNITY,
        TxnId txnId = IMPLICIT_TXN_ID);
    void release(LockMode lockMode, TxnId txnId = IMPLICIT_TXN_ID);
    bool tryUpgrade(TxnId txnId = IMPLICIT_TXN_ID);

    bool isLocked(LockMode lockdMode) const;
    void setSchedulingPolicy(SchedulingPolicy schedulingPolicy);

private:
    SchedulingPolicy schedulingPolicy;
    uint nShared,nExclusive,nExclusivePending;
    LockHolderId exclusiveHolderId;
};

/**
 * Guard class for acquisition of an SXMutex.  Models the
 * boost::ScopedLock concept.  Template parameter LockMode determines the
 * semantics of this lock; the NOWAIT modes may not be used.
 */
template<LockMode lockMode>
class SXMutexGuard : public boost::noncopyable
{
    SXMutex &rwLock;
    bool m_locked;

public:
    explicit SXMutexGuard(SXMutex& mx, bool initially_locked = true)
        : rwLock(mx), m_locked(false)
    {
        if (initially_locked) {
            lock();
        }
    }

    ~SXMutexGuard()
    {
        if (m_locked) {
            unlock();
        }
    }

    void lock()
    {
        assert(!m_locked);
        rwLock.waitFor(lockMode);
        m_locked = true;
    }

    void unlock()
    {
        assert(m_locked);
        rwLock.release(lockMode);
        m_locked = false;
    }

    bool locked() const
    {
        return m_locked;
    }

    operator const void*() const
    {
        return m_locked ? this : 0;
    }
};

typedef SXMutexGuard<LOCKMODE_S> SXMutexSharedGuard;
typedef SXMutexGuard<LOCKMODE_X> SXMutexExclusiveGuard;

FENNEL_END_NAMESPACE

#endif

// End SXMutex.h

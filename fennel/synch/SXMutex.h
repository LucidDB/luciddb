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

#ifndef Fennel_SXMutex_Included
#define Fennel_SXMutex_Included

#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/synch/LockHolderId.h"
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

// NOTE jvs 25-Oct-2008:  We can't replace this class with
// boost::shared_mutex, because lock ownership need to be transaction-scoped
// rather than thread-scoped.

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
class FENNEL_SYNCH_EXPORT SXMutex : public SynchMonitoredObject
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
        LockMode lockMode, uint iTimeout = ETERNITY,
        TxnId txnId = IMPLICIT_TXN_ID);
    void release(LockMode lockMode, TxnId txnId = IMPLICIT_TXN_ID);
    bool tryUpgrade(TxnId txnId = IMPLICIT_TXN_ID);

    bool isLocked(LockMode lockdMode) const;
    void setSchedulingPolicy(SchedulingPolicy schedulingPolicy);

private:
    SchedulingPolicy schedulingPolicy;
    uint nShared, nExclusive, nExclusivePending;
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

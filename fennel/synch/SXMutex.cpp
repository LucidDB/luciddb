/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/synch/SXMutex.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SXMutex::SXMutex()
{
    nShared = 0;
    nExclusive = 0;
    nExclusivePending = 0;
    exclusiveHolderId = -1;
    schedulingPolicy = SCHEDULE_DEFAULT;
}

SXMutex::~SXMutex()
{
    assert(!nShared);
    assert(!nExclusive);
    assert(!nExclusivePending);
    assert(exclusiveHolderId == -1);
}

void SXMutex::setSchedulingPolicy(SchedulingPolicy schedulingPolicyInit)
{
    StrictMutexGuard mutexGuard(mutex);
    assert(!nShared && !nExclusive && !nExclusivePending);
    schedulingPolicy = schedulingPolicyInit;
}

bool SXMutex::waitFor(LockMode lockMode,uint iTimeout)
{
    boost::xtime atv;
    if (iTimeout != ETERNITY) {
        convertTimeout(iTimeout,atv);
    }
    StrictMutexGuard mutexGuard(mutex);
    int currentThreadId = getCurrentThreadId();
    bool bExclusive = (lockMode == LOCKMODE_X || lockMode == LOCKMODE_X_NOWAIT);
    bool bExclusivePending = (lockMode == LOCKMODE_X)
        && (schedulingPolicy == SCHEDULE_FAVOR_EXCLUSIVE);
    if (bExclusivePending) {
        ++nExclusivePending;
    }
    for (;;) {
        if (exclusiveHolderId == currentThreadId) {
            break;
        }
        if (bExclusive) {
            if (!nExclusive && !nShared) {
                break;
            }
        } else {
            if (!nExclusive && !nExclusivePending) {
                break;
            }
        }
        if (lockMode >= LOCKMODE_S_NOWAIT) {
            // NOTE:  for LOCKMODE_X_NOWAIT, don't need to decrement
            // nExclusivePending since we didn't bother incrementing it
            return false;
        }
        if (iTimeout == ETERNITY) {
            condition.wait(mutexGuard);
        } else {
            if (!condition.timed_wait(mutexGuard,atv)) {
                if (bExclusivePending) {
                    assert(nExclusivePending > 0);
                    --nExclusivePending;
                }
                return false;
            }
        }
    }
    if (bExclusive) {
        ++nExclusive;
        exclusiveHolderId = currentThreadId;
        if (bExclusivePending) {
            assert(nExclusivePending > 0);
            --nExclusivePending;
        }
    } else {
        ++nShared;
    }
    return true;
}

void SXMutex::release(LockMode lockMode)
{
    StrictMutexGuard mutexGuard(mutex);
    if (lockMode == LOCKMODE_X) {
        assert(nExclusive);
        --nExclusive;
        if (!nExclusive) {
            exclusiveHolderId = -1;
            condition.notify_all();
        }
    } else {
        assert(lockMode == LOCKMODE_S);
        assert(nShared);
        --nShared;
        if (!nShared) {
            condition.notify_all();
        }
    }
}

bool SXMutex::isLocked(LockMode lockMode) const
{
    if (lockMode == LOCKMODE_X) {
        return nExclusive ? true : false;
    } else {
        assert(lockMode == LOCKMODE_S);
        return nShared ? true : false;
    }
}

bool SXMutex::tryUpgrade()
{
    StrictMutexGuard mutexGuard(mutex);
    assert(nShared);
    assert(!nExclusive);
    if (nShared > 1) {
        return false;
    }
    // otherwise assume caller holds the unique shared lock, and ignore
    // nExclusivePending
    nShared = 0;
    nExclusive = 1;
    exclusiveHolderId = getCurrentThreadId();
    return true;
}

int SXMutex::getCurrentThreadId()
{
    // NOTE jvs 24-Nov-2005:  it would be nice if boost threads would
    // abstract out the notion of thread ID!
#ifdef __MINGW32__
    return static_cast<int>(GetCurrentThreadId());
#else
    return static_cast<int>(pthread_self());
#endif
}

FENNEL_END_CPPFILE("$Id$");

// End SXMutex.cpp

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
#include "fennel/synch/SXMutex.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SXMutex::SXMutex()
{
    nShared = 0;
    nExclusive = 0;
    nExclusivePending = 0;
    schedulingPolicy = SCHEDULE_DEFAULT;
}

SXMutex::~SXMutex()
{
    assert(!nShared);
    assert(!nExclusive);
    assert(!nExclusivePending);
    assert(exclusiveHolderId.isNull());
}

void SXMutex::setSchedulingPolicy(SchedulingPolicy schedulingPolicyInit)
{
    StrictMutexGuard mutexGuard(mutex);
    assert(!nShared && !nExclusive && !nExclusivePending);
    schedulingPolicy = schedulingPolicyInit;
}

bool SXMutex::waitFor(LockMode lockMode, uint iTimeout, TxnId txnId)
{
    boost::xtime atv;
    if (iTimeout != ETERNITY) {
        convertTimeout(iTimeout, atv);
    }
    StrictMutexGuard mutexGuard(mutex);
    LockHolderId acquirerId(txnId);
    bool bExclusive = (lockMode == LOCKMODE_X || lockMode == LOCKMODE_X_NOWAIT);
    bool bExclusivePending = (lockMode == LOCKMODE_X)
        && (schedulingPolicy == SCHEDULE_FAVOR_EXCLUSIVE);
    if (bExclusivePending) {
        ++nExclusivePending;
    }
    for (;;) {
        if (exclusiveHolderId == acquirerId) {
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
            if (!condition.timed_wait(mutexGuard, atv)) {
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
        exclusiveHolderId = acquirerId;
        if (bExclusivePending) {
            assert(nExclusivePending > 0);
            --nExclusivePending;
        }
    } else {
        ++nShared;
    }
    return true;
}

void SXMutex::release(LockMode lockMode, TxnId txnId)
{
    StrictMutexGuard mutexGuard(mutex);
    if (lockMode == LOCKMODE_X) {
        assert(nExclusive);
        LockHolderId releaserId(txnId);
        assert(exclusiveHolderId == releaserId);
        --nExclusive;
        if (!nExclusive) {
            exclusiveHolderId.setNull();
            condition.notify_all();
        }
    } else {
        assert(lockMode == LOCKMODE_S);
        assert(nShared);
        // NOTE:  we can't assert(exclusiveHolderId.isNull()) here,
        // because a txn may take both a shared lock and an exclusive
        // lock simultaneously.
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

bool SXMutex::tryUpgrade(TxnId txnId)
{
    StrictMutexGuard mutexGuard(mutex);
    assert(nShared);
    assert(!nExclusive);
    assert(exclusiveHolderId.isNull());
    if (nShared > 1) {
        return false;
    }
    // otherwise assume caller holds the unique shared lock, and ignore
    // nExclusivePending
    nShared = 0;
    nExclusive = 1;
    LockHolderId holderId(txnId);
    exclusiveHolderId = holderId;
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End SXMutex.cpp

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/synch/SXMutex.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SXMutex::SXMutex()
{
    nShared = 0;
    nExclusive = 0;
    nExclusivePending = 0;
    pExclusiveHolder = NULL;
    schedulingPolicy = SCHEDULE_DEFAULT;
}

SXMutex::~SXMutex()
{
    assert(!nShared);
    assert(!nExclusive);
    assert(!nExclusivePending);
    assert(!pExclusiveHolder);
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
    ThreadData *pCurrThreadData = getThreadData();
    bool bExclusive = (lockMode == LOCKMODE_X || lockMode == LOCKMODE_X_NOWAIT);
    bool bExclusivePending = (lockMode == LOCKMODE_X)
        && (schedulingPolicy == SCHEDULE_FAVOR_EXCLUSIVE);
    if (bExclusivePending) {
        ++nExclusivePending;
    }
    for (;;) {
        if (pExclusiveHolder == pCurrThreadData) {
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
        pExclusiveHolder = pCurrThreadData;
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
            pExclusiveHolder = NULL;
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
    pExclusiveHolder = getThreadData();
    return true;
}

FENNEL_END_CPPFILE("$Id$");

// End SXMutex.cpp

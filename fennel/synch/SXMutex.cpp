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

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

#ifndef Fennel_DatabaseHeader_Included
#define Fennel_DatabaseHeader_Included

#include "fennel/segment/SegPageLock.h"
#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/common/PseudoUuid.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// DatabaseHeader

/**
 * Header stored in the first two pages of a Database data file.  (Two pages
 * because an identical copy is stored as part of a careful-write protocol when
 * checkpointing.)
 */
struct FENNEL_DB_EXPORT DatabaseHeader
    : public StoredNode
{
    // NOTE jvs 27-Apr-2007:  We use distinct magic numbers for incompatible
    // hardware/OS/compiler architectures.  This prevents accidents when
    // attempting to transport physical backup images across machines.
    // Currently, for 32-bit x86, Windows and Linux gcc are incompatible
    // (it may be possible to fix this via pragma/switches, but no
    // one has investigated it so far).  64-bit Linux is incompatible
    // with both.  On next bump-up, it would probably be a good idea
    // to rationalize the numbering scheme so that arch component
    // is one component and Fennel structural version is another;
    // the current scheme isn't scalable as we keep adding architectures!

    // Magic number history:
    // Original value:  0xb1b7b315d821d90aLL;
#ifndef __MSVC__
#if __WORDSIZE == 64
    // Added by jvs for amd64 on 27-May-2007
    static const MagicNumber MAGIC_NUMBER = 0xa513a9e27bc336acLL;
#else
    // Changed by zfong on 3/1/07 (for addition of nextTxnId to checkpoint
    // memento) from original value above to latest value:
    static const MagicNumber MAGIC_NUMBER = 0xb0941b203b81f718LL;
#endif
#else
    // Added by jvs for Windows-specific on 27-Apr-2007
    static const MagicNumber MAGIC_NUMBER = 0x8afe0241a2f7063eLL;
#endif

    /**
     * Data segment version number at last checkpoint.
     */
    SegVersionNum versionNumber;

    /**
     * Memento for state of txn log at last checkpoint.
     */
    LogicalTxnLogCheckpointMemento txnLogCheckpointMemento;

    /**
     * PageId of the oldest page in the shadow log needed for recovery.
     */
    PageId shadowRecoveryPageId;

    /**
     * Whenever a database is opened, except during recovery, a new UUID is
     * generated to represent the online lifetime of the database.  The
     * primary use is for marking log pages so that disk blocks written by
     * previous instances can't masquerade as usable log pages during
     * recovery.
     */
    PseudoUuid onlineUuid;
};

typedef SegNodeLock<DatabaseHeader> DatabaseHeaderPageLock;

FENNEL_END_NAMESPACE

#endif

// End DatabaseHeader.h

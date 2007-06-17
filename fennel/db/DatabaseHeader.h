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
struct DatabaseHeader : public StoredNode
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
#ifndef __MINGW32__
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

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
// Copyright (C) 2003-2004 Disruptive Tech
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

#ifndef Fennel_Types_Included
#define Fennel_Types_Included

#include "fennel/common/SharedTypes.h"

FENNEL_BEGIN_NAMESPACE

// use these symbols when you want to indicate that a variable points to
// a raw buffer of byte data
typedef uint8_t FixedBuffer;      // e.g. FixedBuffer buf[10];
typedef uint8_t *PBuffer;
typedef uint8_t const *PConstBuffer;

// use FileSize for all file sizes and offsets
typedef uint64_t FileSize;

// use TupleStorageByteLength for all tuple buffer length indicators
typedef uint TupleStorageByteLength;

// version number of a VersionedSegment
typedef uint64_t SegVersionNum;

// magic number used for verifying page type
typedef uint64_t MagicNumber;

template <class Node>
class SegNodeLock;

/**
 * See class IntrusiveList for details.
 */
struct IntrusiveListNode
{
    IntrusiveListNode *pNext;
    
#ifdef DEBUG
    IntrusiveListNode()
    {
        pNext = NULL;
    }
#endif
};

// When using unsigned types, it's often necessary to detect wrap-around
// (e.g. when decrementing in a for loop) or to have a special value other
// than 0.  The literal -1 cannot be used for this purpose, because the
// compiler knows that an unsigned number can't possibly be negative.
// Instead, use MAXU, defined here.

class MaxU {
public:
    MaxU(){}    
    operator uint8_t() const
    { return 0xFF; }
    operator uint16_t() const
    { return 0xFFFF; }
    operator uint32_t() const
    { return 0xFFFFFFFF; }
    operator uint64_t() const
    { return 0xFFFFFFFFFFFFFFFFLL; }
    // TODO:  something better
#ifdef __CYGWIN__
    operator uint() const
    { return 0xFFFFFFFFFFFFFFFFLL; }
#endif
};

static const MaxU MAXU;

// however, you can't compare some unsigned types against MAXU,
// so use isMAXU(u) instead
template <class tU>
inline bool isMAXU(tU u)
{
    return (u == tU(0xFFFFFFFFFFFFFFFFLL));
}

enum { ETERNITY = 0xFFFFFFFF };

/**
 * LockMode enumerates various common lock modes.
 */
enum LockMode
{
    /**
     * Shared lock.
     */
    LOCKMODE_S,

    /**
     * Exclusive lock.
     */
    LOCKMODE_X,

    /**
     * Shared lock; fail immediately rather than waiting.
     */
    LOCKMODE_S_NOWAIT,

    /**
     * Exclusive lock; fail immediately rather than waiting.
     */
    LOCKMODE_X_NOWAIT
    
// NOTE:  enumeration order is significant
};

/**
 * CheckpointType enumerations checkpoint types; precise meaning varies with
 * context.
 */
enum CheckpointType
{
    /**
     * Unmap all data from cache, without flushing dirty data.
     */
    CHECKPOINT_DISCARD,

    /**
     * Flush dirty data and then unmap it from cache.
     */
    CHECKPOINT_FLUSH_AND_UNMAP,

    /**
     * Flush all dirty data.
     */
    CHECKPOINT_FLUSH_ALL,

    /**
     * Flush some dirty data (criteria for flush is context-specific).
     */
    CHECKPOINT_FLUSH_FUZZY
    
// NOTE:  enumeration order is significant
};

/**
 * Options for how to deal with detection of a duplicate key while
 * searching.
 */
enum DuplicateSeek
{
    /**
     * Position to an arbitrary match for the duplicate key.
     */
    DUP_SEEK_ANY,

    /**
     * Position to the first match for a duplicate key.
     */
    DUP_SEEK_BEGIN,

    /**
     * Position to one past the last match for a duplicate key.
     */
    DUP_SEEK_END
};

/**
 * ParamName can be used to declare static string symbolic constants
 * serving as typo-safe parameter names.
 */
typedef char const * const ParamName;
    
/**
 * ParamName can be used to declare static string symbolic constants
 * serving as early-bound parameter values.
 */
typedef char const * const ParamVal;

// PageOwnerId is a 64-bit integer identifying the owner of a page allocated
// from a segment.
DEFINE_OPAQUE_INTEGER(PageOwnerId,uint64_t);

// DeviceID is an integer identifying a device.
DEFINE_OPAQUE_INTEGER(DeviceId,uint);

// SegmentId is an integer identifying a segment.
DEFINE_OPAQUE_INTEGER(SegmentId,uint);

// BlockId is a 64-bit identifier for a physical block on disk.
DEFINE_OPAQUE_INTEGER(BlockId,uint64_t);

// PageId is a 64-bit identifier for a logical page within the scope of a
// particular segment.
DEFINE_OPAQUE_INTEGER(PageId,uint64_t);

// SegByteId is the logical 64-bit address of a byte within the scope of a
// particular segment.
DEFINE_OPAQUE_INTEGER(SegByteId,uint64_t);

// TxnId is the 64-bit identifier for a transaction.
DEFINE_OPAQUE_INTEGER(TxnId,uint64_t);

// TxnId is an integer identifier for a txn-relative savepoint.
DEFINE_OPAQUE_INTEGER(SavepointId,uint);

// LogicalTxnClassId is a magic number identifying the type of
// a logged LogicalTxnParticipant.
DEFINE_OPAQUE_INTEGER(LogicalTxnClassId,uint64_t);

// LogicalActionType enumerates the possible actions in a LogicalTxn in a
// participant-defined manner.  Each participant class defines its own
// enumeration of positive integers, but the same integer may be used by
// different participant classes.  Negative integers are used for
// system-defined actions.
typedef int LogicalActionType;

/**
 * Sentinel value for an invalid PageId.
 */
static const PageId NULL_PAGE_ID = PageId(0xFFFFFFFFFFFFFFFFLL);

/**
 * Symbolic value for first PageId of a LINEAR_ALLOCATION Segment.
 */
static const PageId FIRST_LINEAR_PAGE_ID = PageId(0);

/**
 * Sentinel value for an invalid BlockId.
 */
static const BlockId NULL_BLOCK_ID = BlockId(0xFFFFFFFFFFFFFFFFLL);

/**
 * Symbolic value for an anonymous page owner; i.e. the page is allocated,
 * but without a particular owner.
 */
static const PageOwnerId ANON_PAGE_OWNER_ID = PageOwnerId(0xFFFFFFFFFFFFFFFFLL);

// The types below are called "Num" rather than "Id" because they are
// used to represent either a count or an offset.  We use plain
// typedef rather than DEFINE_OPAQUE_INTEGER because a lot of arithmetic
// is done on these.

/**
 * Record count or number.
 */
typedef uint64_t RecordNum;

/**
 * Extent count or number.
 */
typedef uint ExtentNum;

/**
 * Block count or number.
 */
typedef uint BlockNum;

/**
 * SeekPosition enumerates the two possible endpoints for a seek.
 */
enum SeekPosition
{
    SEEK_STREAM_BEGIN,
    SEEK_STREAM_END
};

FENNEL_END_NAMESPACE

#endif

// End Types.h

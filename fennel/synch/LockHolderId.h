/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2007-2009 The Eigenbase Project
// Copyright (C) 2007-2009 SQLstream, Inc.
// Copyright (C) 2007-2009 LucidEra, Inc.
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

#ifndef Fennel_LockHolderId_Included
#define Fennel_LockHolderId_Included

FENNEL_BEGIN_NAMESPACE

/**
 * LockHolderId encapsulates the identity of an entity which can
 * hold a lock, currently either a transaction or a thread.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class LockHolderId
{
    enum HolderType {
        TYPE_NULL,
        TYPE_THREAD,
        TYPE_TXN
    };

    TxnId holderId;
    HolderType holderType;

public:
    /**
     * Creates a new null holder.
     */
    explicit inline LockHolderId();

    /**
     * Creates a new ID corresponding to a transaction or thread ID.
     *
     * @param txnId ID of a transaction, or IMPLICIT_TXN_ID to
     * assign from the current thread ID
     */
    explicit inline LockHolderId(TxnId txnId);

    /**
     * Assigns identity from a transaction or thread ID.
     *
     * @param txnId ID of a transaction, or IMPLICIT_TXN_ID to
     * assign from the current thread ID
     */
    inline void assignFrom(TxnId txnId);

    /**
     * @return whether this holder is null (has not been assigned an identity)
     */
    inline bool isNull() const;

    /**
     * Sets this holder to null.
     */
    inline void setNull();

    /**
     * Compares this holder to another.
     *
     * @param other other holder to compare
     *
     * @return whether the two holders identify the same entity (or are both
     * null)
     */
    inline int operator == (LockHolderId const & other) const;
};

inline void LockHolderId::assignFrom(TxnId txnId)
{
    if (txnId == IMPLICIT_TXN_ID) {
        // NOTE jvs 2-Jun-2007:  Not exactly posixly correct,
        // but it will probably do for most environments.
        holderId = TxnId(uint(getCurrentThreadId()));
        holderType = TYPE_THREAD;
    } else {
        holderId = txnId;
        holderType = TYPE_TXN;
    }
}

void LockHolderId::setNull()
{
    holderId = NULL_TXN_ID;
    holderType = TYPE_NULL;
}

inline LockHolderId::LockHolderId()
{
    setNull();
}

inline LockHolderId::LockHolderId(TxnId txnId)
{
    assignFrom(txnId);
}

inline bool LockHolderId::isNull() const
{
    return holderType == TYPE_NULL;
}

inline int LockHolderId::operator == (LockHolderId const & other) const
{
    return (holderId == other.holderId)
        && (holderType == other.holderType);
}

FENNEL_END_NAMESPACE

#endif

// End LockHolderId.h

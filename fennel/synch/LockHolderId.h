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
class FENNEL_SYNCH_EXPORT LockHolderId
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
        holderId = TxnId(getCurrentThreadId());
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

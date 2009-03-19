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

#ifndef Fennel_LogicalTxn_Included
#define Fennel_LogicalTxn_Included

#include <boost/utility.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "fennel/txn/LogicalTxnStoredStructs.h"

#include <vector>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;

/**
 * LogicalTxn represents a transaction implemented via a logical
 * logging strategy.
 */
class LogicalTxn
    : public boost::noncopyable,
        public boost::enable_shared_from_this<LogicalTxn>
{
    friend class LogicalTxnLog;

    /**
     * Valid transaction states.
     */
    enum State
    {
        STATE_LOGGING_TXN,

        STATE_LOGGING_ACTION,

        STATE_ROLLING_BACK,

        STATE_ROLLED_BACK,

        STATE_COMMITTED
    };

    /**
     * Identifier for this transaction.  Note that this is assigned when the
     * txn is created, so it does not reflect commit order.
     */
    TxnId txnId;

    /**
     * Log containing this txn.
     */
    SharedLogicalTxnLog pLog;

    /**
     * CacheAccessor to use for writing to log.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Stream for writing to this transaction's log.
     */
    SharedSpillOutputStream pOutputStream;

    /**
     * Current txn state.
     */
    State state;

    /**
     * Was this txn active at the last checkpoint?
     */
    bool checkpointed;

    /**
     * Potential savepoint representing current log position.
     */
    LogicalTxnSavepoint svpt;

    /**
     * Savepoints previously returned via createSavepoint.  SavepointId is an
     * index into this vector.
     */
    std::vector<LogicalTxnSavepoint> savepoints;

    /**
     * Collection of LogicalTxnParticipants which have joined this txn.
     */
    std::vector<SharedLogicalTxnParticipant> participants;

    explicit LogicalTxn(
        TxnId txnId,
        SharedLogicalTxnLog pLog,
        SharedCacheAccessor pCacheAccessor);

    void describeParticipant(SharedLogicalTxnParticipant);
    void describeAllParticipants();
    void forgetAllParticipants();

    void rollbackToSavepoint(LogicalTxnSavepoint &oldSvpt);
    ByteOutputStream &beginLogicalAction(
        LogicalTxnParticipant *pParticipant,
        LogicalActionType actionType);

public:

    virtual ~LogicalTxn();

    /**
     * Registers a participant which is joining the transaction.  Must
     * be called before the participant can log any actions.
     *
     * @param pParticipant the participant to join; reference will be retained
     * for the duration of the txn
     */
    void addParticipant(SharedLogicalTxnParticipant pParticipant);

    /**
     * Begins an action description log entry.  After this, the
     * participant must write the action description to the txn's output
     * stream.
     *
     * @param participant the LogicalTxnParticipant initiating the action
     *
     * @param actionType participant-defined LogicalActionType
     *
     * @return output stream to use for logging action data
     */
    ByteOutputStream &beginLogicalAction(
        LogicalTxnParticipant &participant,
        LogicalActionType actionType);

    /**
     * Ends the log entry for an action description.  After this, the
     * ByteOutputStream returned from beginLogicalAction is no longer valid.
     */
    void endLogicalAction();

    /**
     * Creates a savepoint representing the current transaction state.
     *
     * @return ID of new savepoint
     */
    SavepointId createSavepoint();

    /**
     * Commits a given savepoint and any later savepoints.  Note that
     * committing a savepoint does not make its actions durable; it just
     * releases any information required to rollback to that savepoint.
     *
     * @param svptId savepoint to commit
     */
    void commitSavepoint(SavepointId svptId);

    /**
     * Aborts the transaction.
     *
     * @param pSvptId Id of the savepoint to which to rollback (or
     * default NULL to rollback and end the entire transaction)
     */
    void rollback(SavepointId const *pSvptId = NULL);

    /**
     * Commits the transaction.
     */
    void commit();

    /**
     * @return the log for this txn
     */
    SharedLogicalTxnLog getLog();

    /**
     * @return ID of this txn
     */
    TxnId getTxnId() const;
};

FENNEL_END_NAMESPACE

#endif

// End LogicalTxn.h

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
class FENNEL_TXN_EXPORT LogicalTxn
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

    /**
     * @return true if transaction has been committed or rolled back
     */
    bool isEnded() const;
};

FENNEL_END_NAMESPACE

#endif

// End LogicalTxn.h

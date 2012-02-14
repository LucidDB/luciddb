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

#ifndef Fennel_LogicalTxnStoredStructs_Included
#define Fennel_LogicalTxnStoredStructs_Included

#include "fennel/segment/SegStream.h"

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;

// NOTE:  read comments on struct StoredNode before modifying
// the structs below

/**
 * LogicalTxnSavepoint defines all state for a savepoint within a LogicalTxn.
 */
struct FENNEL_TXN_EXPORT LogicalTxnSavepoint
{
    /**
     * Log size up to end of last action logged.
     */
    FileSize cbLogged;

    /**
     * Entry size for last action logged.
     */
    uint cbActionPrev;
};

/**
 * Log record describing a transaction event.
 */
struct FENNEL_TXN_EXPORT LogicalTxnEventMemento
{
    enum Event {
        EVENT_ROLLBACK,
        EVENT_COMMIT,
        EVENT_CHECKPOINT
    };

    /**
     * Transaction event which caused this memento to be created.
     */
    Event event;

    /**
     * Unique identifier for committed transaction.
     */
    TxnId txnId;

    /**
     * Position of end of transaction log as of event occurrence.
     */
    SegStreamPosition logPosition;

    /**
     * If true, log data for this txn is stored separately.  Otherwise, for
     * EVENT_COMMIT, it's stored immediately after this header.
     */
    bool longLog;

    /**
     * Number of bytes in last action recorded.
     */
    uint cbActionLast;

    /**
     * Number of participants which have joined txn so far.
     */
    uint nParticipants;
};

/**
 * Logged header for a single logical action.
 */
struct FENNEL_TXN_EXPORT LogicalTxnActionHeader
{
    /**
     * Stored pointer to the participant initiating the action.  While
     * online, this is used for direct access to the participant.  In
     * recovery, it's used instead as an identifier which is swizzled
     * to the recovered participant.
     */
    LogicalTxnParticipant *pParticipant;

    /**
     * Participant-defined action type which allows the participant
     * to interpret the data portion of the action.
     */
    LogicalActionType actionType;

    /**
     * Number of bytes logged for previous action, or MAXU
     * for first action.
     */
    uint cbActionPrev;
};

/**
 * Global information recorded during LogicalTxnLog::checkpoint; this is all
 * the information needed to begin recovery.
 */
struct FENNEL_TXN_EXPORT LogicalTxnLogCheckpointMemento
{
    /**
     * Log stream position of first LogicalTxnCheckpointHeader.
     */
    SegStreamPosition logPosition;

    /**
     * Number of uncommitted transactions active at time of checkpoint.
     */
    uint nUncommittedTxns;

    /**
     * TxnId of the next new transaction
     */
    TxnId nextTxnId;
};

/**
 * LogicalTxn-defined LogicalActionType for the initial entry for a
 * particular participant.  The corresponding data is the result of calling
 * LogicalTxnParticipant::describeTxnParticipant().
 */
static const LogicalActionType ACTION_TXN_DESCRIBE_PARTICIPANT = -1;

/**
 * LogicalTxn-defined LogicalActionType for a rollback to savepoint.
 * particular participant.  The corresponding data is the LogicalTxnSavepoint
 * to which the txn was rolled back.
 */
static const LogicalActionType ACTION_TXN_ROLLBACK_TO_SAVEPOINT = -2;

FENNEL_END_NAMESPACE

#endif

// End LogicalTxnStoredStructs.h

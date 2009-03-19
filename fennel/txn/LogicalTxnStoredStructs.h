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
struct LogicalTxnSavepoint
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
struct LogicalTxnEventMemento
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
struct LogicalTxnActionHeader
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
struct LogicalTxnLogCheckpointMemento
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

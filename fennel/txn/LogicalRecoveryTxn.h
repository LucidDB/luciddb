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

#ifndef Fennel_LogicalRecoveryTxn_Included
#define Fennel_LogicalRecoveryTxn_Included

#include "fennel/common/VoidPtrHash.h"

#include <boost/utility.hpp>

#include <hash_map>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;
class LogicalTxnParticipantFactory;
struct LogicalTxnSavepoint;

/**
 * LogicalRecoveryTxn implements recovery for transactions previously logged
 * via LogicalTxn.
 */
class FENNEL_TXN_EXPORT LogicalRecoveryTxn
    : public boost::noncopyable
{
    typedef std::hash_map<
        LogicalTxnParticipant *,
        SharedLogicalTxnParticipant,
        VoidPtrHash> ParticipantMap;
    typedef ParticipantMap::iterator ParticipantMapIter;

    /**
     * Stream from which to read log data.
     */
    SharedByteInputStream pTxnInputStream;

    /**
     * Factory for recovering txn participants, or NULL if online recovery
     * being performed.
     */
    LogicalTxnParticipantFactory *pParticipantFactory;

    /**
     * Swizzling map from logged participant to recovered participant.
     */
    ParticipantMap participantMap;

    bool isOnline() const
    {
        return pParticipantFactory ? false : true;
    }

    void recoverParticipant(
        LogicalTxnParticipant *pLoggedParticipant);

    LogicalTxnParticipant *swizzleParticipant(
        LogicalTxnParticipant *pParticipant);

public:
    /**
     * Constructor.
     *
     * @param pTxnInputStream stream for reading transaction log
     *
     * @param pParticipantFactory if NULL, online recovery is being performed,
     * and logged participants can be accessed directly; if non-NULL, logged
     * participants must be recovered via this factory
     */
    explicit LogicalRecoveryTxn(
        SharedByteInputStream pTxnInputStream,
        LogicalTxnParticipantFactory *pParticipantFactory);

    virtual ~LogicalRecoveryTxn();

    /**
     * Performs redo for actions from the log in their original logged order.
     *
     * @param cbRedo number of log bytes to read; redo stops after this
     */
    void redoActions(uint cbRedo);

    /**
     * Performs undo for actions from the log in reverse order.  The undo may
     * be limited by nActionsMax or minActionOffset, but not both.
     *
     * @param svptEnd savepoint for log position just after first action
     * to be undone
     *
     * @param nActionsMax limit on number of actions to undo, or MAXU for no
     * limit
     *
     * @param minActionOffset log position at which to stop rolling back, or 0
     * for the entire log
     */
    void undoActions(
        LogicalTxnSavepoint const &svptEnd,
        uint nActionsMax = MAXU,
        FileSize minActionOffset = 0);
};

FENNEL_END_NAMESPACE

#endif

// End LogicalRecoveryTxn.h

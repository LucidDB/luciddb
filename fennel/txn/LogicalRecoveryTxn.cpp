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

#include "fennel/common/CommonPreamble.h"
#include "fennel/txn/LogicalRecoveryTxn.h"
#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/txn/LogicalTxnParticipant.h"
#include "fennel/txn/LogicalTxnParticipantFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LogicalRecoveryTxn::LogicalRecoveryTxn(
    SharedByteInputStream pTxnInputStreamInit,
    LogicalTxnParticipantFactory *pParticipantFactoryInit)
    : pTxnInputStream(pTxnInputStreamInit),
      pParticipantFactory(pParticipantFactoryInit)
{
}

LogicalRecoveryTxn::~LogicalRecoveryTxn()
{
}

void LogicalRecoveryTxn::redoActions(
    uint cbRedo)
{
    FileSize cbStart = pTxnInputStream->getOffset();
    while (pTxnInputStream->getOffset() < (cbStart + cbRedo)) {
        LogicalTxnActionHeader actionHeader;
        pTxnInputStream->readValue(actionHeader);

        switch (actionHeader.actionType) {
        case ACTION_TXN_DESCRIBE_PARTICIPANT:
            recoverParticipant(actionHeader.pParticipant);
            break;
        case ACTION_TXN_ROLLBACK_TO_SAVEPOINT:
            {
                LogicalTxnSavepoint oldSvpt;
                pTxnInputStream->readValue(oldSvpt);
                // remember current position
                FileSize offset = pTxnInputStream->getOffset();
                // rewind this action
                pTxnInputStream->seekBackward(
                    sizeof(oldSvpt) + sizeof(actionHeader));
                LogicalTxnSavepoint svptEnd;
                svptEnd.cbLogged = pTxnInputStream->getOffset();
                svptEnd.cbActionPrev = actionHeader.cbActionPrev;
                // redo rollback
                undoActions(svptEnd, MAXU, oldSvpt.cbLogged);
                // restore position
                pTxnInputStream->seekForward(
                    offset - pTxnInputStream->getOffset());
            }
            break;
        default:
            {
                LogicalTxnParticipant *pParticipant = swizzleParticipant(
                    actionHeader.pParticipant);
                pParticipant->redoLogicalAction(
                    actionHeader.actionType,
                    *pTxnInputStream);
            }
            break;
        }
    }
    assert(pTxnInputStream->getOffset() == (cbStart + cbRedo));
}

void LogicalRecoveryTxn::recoverParticipant(
    LogicalTxnParticipant *pLoggedParticipant)
{
    if (isOnline()) {
        return;
    }
    LogicalTxnClassId classId;
    pTxnInputStream->readValue(classId);
    SharedLogicalTxnParticipant pRecoveredParticipant =
        pParticipantFactory->loadParticipant(classId,*pTxnInputStream);
    participantMap[pLoggedParticipant] = pRecoveredParticipant;
}

void LogicalRecoveryTxn::undoActions(
    LogicalTxnSavepoint const &svptEnd,
    uint nActionsMax,
    FileSize minActionOffset)
{
    assert(isMAXU(nActionsMax) || !minActionOffset);

    uint nActions = 0;
    uint cbActionExpected = svptEnd.cbActionPrev;
    uint seekDist = 0;
    while (cbActionExpected && (nActions < nActionsMax)) {
        seekDist += cbActionExpected;
        pTxnInputStream->seekBackward(seekDist);
        FileSize actionOffset = pTxnInputStream->getOffset();
        if (actionOffset < minActionOffset) {
            break;
        }
        LogicalTxnActionHeader actionHeader;
        pTxnInputStream->readValue(actionHeader);

        switch (actionHeader.actionType) {
        case ACTION_TXN_DESCRIBE_PARTICIPANT:
            if (swizzleParticipant(actionHeader.pParticipant)) {
                // ignore log data since the participant is already available
                seekDist = sizeof(actionHeader);
            } else {
                recoverParticipant(actionHeader.pParticipant);
                assert(
                    pTxnInputStream->getOffset()
                    == actionOffset + cbActionExpected);
                seekDist = cbActionExpected;
            }
            break;
        case ACTION_TXN_ROLLBACK_TO_SAVEPOINT:
            {
                // skip everything back to savepoint, since it was already
                // undone
                LogicalTxnSavepoint oldSvpt;
                pTxnInputStream->readValue(oldSvpt);
                assert(oldSvpt.cbLogged < pTxnInputStream->getOffset());
                actionHeader.cbActionPrev = oldSvpt.cbActionPrev;
                seekDist = pTxnInputStream->getOffset() - oldSvpt.cbLogged;
            }
            break;
        default:
            {
                LogicalTxnParticipant *pParticipant = swizzleParticipant(
                    actionHeader.pParticipant);
                pParticipant->undoLogicalAction(
                    actionHeader.actionType,
                    *pTxnInputStream);
                assert(
                    pTxnInputStream->getOffset()
                    == actionOffset + cbActionExpected);
                seekDist = cbActionExpected;
            }
            break;
        }
        cbActionExpected = actionHeader.cbActionPrev;
        ++nActions;
    }
}

LogicalTxnParticipant *LogicalRecoveryTxn::swizzleParticipant(
    LogicalTxnParticipant *pParticipant)
{
    if (isOnline()) {
        return pParticipant;
    }
    ParticipantMapIter pParticipantEntry = participantMap.find(pParticipant);
    if (pParticipantEntry != participantMap.end()) {
        return pParticipantEntry->second.get();
    } else {
        return NULL;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End LogicalRecoveryTxn.cpp

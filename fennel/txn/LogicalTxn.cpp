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
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnParticipant.h"
#include "fennel/segment/SegOutputStream.h"
#include "fennel/segment/SegInputStream.h"
#include "fennel/segment/SpillOutputStream.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalRecoveryLog.h"
#include "fennel/txn/LogicalRecoveryTxn.h"

#include <boost/bind.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

LogicalTxn::LogicalTxn(
    TxnId txnIdInit,
    SharedLogicalTxnLog pLogInit,
    SharedCacheAccessor pCacheAccessorInit)
    : txnId(txnIdInit),
      pLog(pLogInit),
      pCacheAccessor(pCacheAccessorInit)
{
    pOutputStream = SpillOutputStream::newSpillOutputStream(
        pLog->pSegmentFactory,
        pCacheAccessor,
        LogicalRecoveryLog::getLongLogFileName(txnId));

    // REVIEW: We could use something like a WALSegment to keep track of page
    // states, eliminating the overhead of a full-cache checkpoint when log is
    // committed.
    pOutputStream->setWriteLatency(WRITE_EAGER_ASYNC);

    state = STATE_LOGGING_TXN;
    svpt.cbActionPrev = 0;
    svpt.cbLogged = 0;
    checkpointed = false;
}

LogicalTxn::~LogicalTxn()
{
    assert(isEnded());
    assert(participants.empty());
}

void LogicalTxn::addParticipant(SharedLogicalTxnParticipant pParticipant)
{
    if (pParticipant->pTxn) {
        assert(pParticipant->pTxn == this);
        return;
    }
    participants.push_back(pParticipant);
    pParticipant->pTxn = this;
    pParticipant->enableLogging(true);
    describeParticipant(pParticipant);
}

ByteOutputStream &LogicalTxn::beginLogicalAction(
    LogicalTxnParticipant &participant,
    LogicalActionType actionType)
{
    assert(participant.pTxn == this);
    return beginLogicalAction(&participant,actionType);
}

ByteOutputStream &LogicalTxn::beginLogicalAction(
    LogicalTxnParticipant *pParticipant,
    LogicalActionType actionType)
{
    assert(state == STATE_LOGGING_TXN);
    LogicalTxnActionHeader actionHeader;
    actionHeader.pParticipant = pParticipant;
    actionHeader.actionType = actionType;
    actionHeader.cbActionPrev = svpt.cbActionPrev;
    pOutputStream->writeValue(actionHeader);
    state = STATE_LOGGING_ACTION;
    return *pOutputStream;
}

void LogicalTxn::endLogicalAction()
{
    assert(state == STATE_LOGGING_ACTION);
    state = STATE_LOGGING_TXN;
    svpt.cbActionPrev =
        pOutputStream->getOffset() - svpt.cbLogged;
    svpt.cbLogged = pOutputStream->getOffset();
}

SavepointId LogicalTxn::createSavepoint()
{
    assert(state == STATE_LOGGING_TXN);
    SavepointId svptId = SavepointId(savepoints.size());
    savepoints.push_back(svpt);
    return svptId;
}

void LogicalTxn::commitSavepoint(SavepointId svptId)
{
    assert(state == STATE_LOGGING_TXN);
    uint iSvpt = opaqueToInt(svptId);
    assert(iSvpt < savepoints.size());
    savepoints.resize(iSvpt);
}

void LogicalTxn::rollback(SavepointId const *pSvptId)
{
    assert(state == STATE_LOGGING_TXN);
    if (pSvptId) {
        uint iSvpt = opaqueToInt(*pSvptId);
        assert(iSvpt < savepoints.size());
        savepoints.resize(iSvpt + 1);
        rollbackToSavepoint(savepoints[iSvpt]);
        return;
    }

    // NOTE:  this protects against implicit self-delete until end-of-method
    SharedLogicalTxn pThis = shared_from_this();
    std::vector<SharedLogicalTxnParticipant> toRollback(participants);
    // forget the participants so that participants don't try to write
    // to log during rollback, but we keep the references
    // until after we finish using them
    forgetAllParticipants();

    SharedSegment pLongLogSegment = pOutputStream->getSegment();
    SharedByteInputStream pInputStream =
        pOutputStream->getInputStream(SEEK_STREAM_END);
    pOutputStream->close();
    assert(svpt.cbLogged == pInputStream->getOffset());

    {
        state = STATE_ROLLING_BACK;
        LogicalRecoveryTxn recoveryTxn(pInputStream, NULL);
        recoveryTxn.undoActions(svpt);
    }
    // clear out the participants
    toRollback.clear();

    svpt.cbLogged = pInputStream->getOffset();
    state = STATE_ROLLED_BACK;
    pInputStream.reset();
    if (pLongLogSegment) {
        pLongLogSegment->checkpoint(CHECKPOINT_DISCARD);
        pLongLogSegment.reset();
    }
    pLog->rollbackTxn(pThis);
    pLog.reset();
    pOutputStream.reset();
}

void LogicalTxn::commit()
{
    // NOTE:  this protects against implicit self-delete until end-of-method
    SharedLogicalTxn pThis = shared_from_this();
    pLog->commitTxn(pThis);
    pLog.reset();
    state = STATE_COMMITTED;
    forgetAllParticipants();
}

void LogicalTxn::describeAllParticipants()
{
    std::for_each(
        participants.begin(),
        participants.end(),
        boost::bind(&LogicalTxn::describeParticipant,this,_1));
}

void LogicalTxn::describeParticipant(SharedLogicalTxnParticipant pParticipant)
{
    beginLogicalAction(*pParticipant,ACTION_TXN_DESCRIBE_PARTICIPANT);
    LogicalTxnClassId classId =
        pParticipant->getParticipantClassId();
    pOutputStream->writeValue(classId);
    pParticipant->describeParticipant(*pOutputStream);
    endLogicalAction();
}

void LogicalTxn::forgetAllParticipants()
{
    std::for_each(
        participants.begin(),
        participants.end(),
        boost::bind(&LogicalTxnParticipant::clearLogicalTxn,_1));
    participants.clear();
}

void LogicalTxn::rollbackToSavepoint(LogicalTxnSavepoint &oldSvpt)
{
    assert(oldSvpt.cbLogged <= svpt.cbLogged);
    // disable logging for all participants during rollback
    std::for_each(
        participants.begin(),
        participants.end(),
        boost::bind(&LogicalTxnParticipant::enableLogging,_1,false));

    // TODO:  for short logs, could just reuse memory

    SharedByteInputStream pInputStream =
        pOutputStream->getInputStream(SEEK_STREAM_END);
    assert(svpt.cbLogged == pInputStream->getOffset());
    {
        state = STATE_ROLLING_BACK;
        LogicalRecoveryTxn recoveryTxn(pInputStream, NULL);
        recoveryTxn.undoActions(svpt, MAXU, oldSvpt.cbLogged);
        state = STATE_LOGGING_TXN;
    }
    pInputStream.reset();

    // re-enable logging for all participants
    std::for_each(
        participants.begin(),
        participants.end(),
        boost::bind(&LogicalTxnParticipant::enableLogging, _1, true));

    // write log entry noting the partial rollback
    beginLogicalAction(NULL, ACTION_TXN_ROLLBACK_TO_SAVEPOINT);
    pOutputStream->writeValue(oldSvpt);
    endLogicalAction();
}

SharedLogicalTxnLog LogicalTxn::getLog()
{
    return pLog;
}

bool LogicalTxn::isEnded() const
{
    return state == STATE_ROLLED_BACK || state == STATE_COMMITTED;
}

TxnId LogicalTxn::getTxnId() const
{
    return txnId;
}

FENNEL_END_CPPFILE("$Id$");

// End LogicalTxn.cpp

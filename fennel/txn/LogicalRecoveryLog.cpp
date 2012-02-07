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
#include "fennel/txn/LogicalRecoveryLog.h"
#include "fennel/txn/LogicalRecoveryTxn.h"
#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/segment/CrcSegInputStream.h"
#include "fennel/device/DeviceMode.h"
#include "fennel/txn/LogicalTxnParticipantFactory.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

LogicalRecoveryLog::LogicalRecoveryLog(
    LogicalTxnParticipantFactory &participantFactoryInit,
    SegmentAccessor const &logSegmentAccessorInit,
    PseudoUuid const &onlineUuid,
    SharedSegmentFactory pSegmentFactoryInit)
    : participantFactory(participantFactoryInit),
      pSegmentFactory(pSegmentFactoryInit),
      logSegmentAccessor(logSegmentAccessorInit)
{
    pInputStream = CrcSegInputStream::newCrcSegInputStream(
        logSegmentAccessor, onlineUuid);
}

SharedLogicalRecoveryLog LogicalRecoveryLog::newLogicalRecoveryLog(
    LogicalTxnParticipantFactory &participantFactory,
    SegmentAccessor const &logSegmentAccessor,
    PseudoUuid const &onlineUuid,
    SharedSegmentFactory pSegmentFactory)
{
    return SharedLogicalRecoveryLog(
        new LogicalRecoveryLog(
            participantFactory, logSegmentAccessor, onlineUuid,
            pSegmentFactory));
}

LogicalRecoveryLog::~LogicalRecoveryLog()
{
}

void LogicalRecoveryLog::recover(
    LogicalTxnLogCheckpointMemento const &logMemento)
{
    LogicalTxnEventMemento txnMemento;
    pInputStream->seekSegPos(logMemento.logPosition);
    for (uint i = 0; i < logMemento.nUncommittedTxns; ++i) {
        uint cb = pInputStream->readValue(txnMemento);
        assert(cb == sizeof(txnMemento));
        assert(txnMemento.event == LogicalTxnEventMemento::EVENT_CHECKPOINT);
        checkpointTxnMap[txnMemento.txnId] = txnMemento;
    }
    for (;;) {
        uint cb = pInputStream->readValue(txnMemento);
        if (cb < sizeof(txnMemento)) {
            break;
        }
        TxnId txnId = txnMemento.txnId;
        TxnMapIter pTxnEntry = checkpointTxnMap.find(txnId);
        SharedSegInputStream pTxnInputStream;
        if (txnMemento.longLog) {
            pTxnInputStream = openLongLogStream(txnId);
        } else {
            // REVIEW:  Is there a chance that txn might be incompletely
            // logged?  If so, need to prevent it or detect it.
            pTxnInputStream = pInputStream;
        }
        switch (txnMemento.event) {
        case LogicalTxnEventMemento::EVENT_COMMIT:
            if (pTxnEntry == checkpointTxnMap.end()) {
                redoTxn(txnMemento, NULL, pTxnInputStream);
            } else {
                redoTxn(
                    txnMemento,
                    &(pTxnEntry->second),
                    pTxnInputStream);
                checkpointTxnMap.erase(pTxnEntry);
            }
            break;
        case LogicalTxnEventMemento::EVENT_ROLLBACK:
            assert(pTxnEntry != checkpointTxnMap.end());
            undoTxn(pTxnEntry->second, pTxnInputStream);
            checkpointTxnMap.erase(txnId);
            break;
        case LogicalTxnEventMemento::EVENT_CHECKPOINT:
            break;
        default:
            permAssert(false);
        }
    }
    for (TxnMapIter pTxnEntry = checkpointTxnMap.begin();
         pTxnEntry != checkpointTxnMap.end(); ++pTxnEntry)
    {
        SharedSegInputStream pTxnInputStream =
            openLongLogStream(pTxnEntry->first);
        undoTxn(pTxnEntry->second, pTxnInputStream);
    }
    checkpointTxnMap.clear();
}

SharedSegInputStream LogicalRecoveryLog::openLongLogStream(TxnId txnId)
{
    DeviceMode openMode = DeviceMode::load;
    openMode.readOnly = true;
    SharedSegment pTxnLogSegment =
        pSegmentFactory->newTempDeviceSegment(
            logSegmentAccessor.pCacheAccessor->getCache(),
            openMode,
            getLongLogFileName(txnId));
    SegmentAccessor txnSegmentAccessor(
        pTxnLogSegment,
        logSegmentAccessor.pCacheAccessor);
    return SegInputStream::newSegInputStream(txnSegmentAccessor);
}

void LogicalRecoveryLog::redoTxn(
    LogicalTxnEventMemento const &commitMemento,
    LogicalTxnEventMemento const *pCheckpointMemento,
    SharedSegInputStream pTxnInputStream)
{
    LogicalRecoveryTxn recoveryTxn(
        pTxnInputStream,
        &participantFactory);
    FileSize cbRedo;
    if (pCheckpointMemento) {
        pTxnInputStream->seekSegPos(pCheckpointMemento->logPosition);
        // first, recover checkpointed participants
        LogicalTxnSavepoint svpt;
        svpt.cbActionPrev = pCheckpointMemento->cbActionLast;
        svpt.cbLogged = pCheckpointMemento->logPosition.cbOffset;
        recoveryTxn.undoActions(
            svpt,
            pCheckpointMemento->nParticipants);
        // now, prepare for redo
        cbRedo = commitMemento.logPosition.cbOffset
            - pCheckpointMemento->logPosition.cbOffset;
        pTxnInputStream->seekSegPos(pCheckpointMemento->logPosition);
    } else {
        cbRedo = commitMemento.logPosition.cbOffset;
    }
    recoveryTxn.redoActions(cbRedo);
}

void LogicalRecoveryLog::undoTxn(
    LogicalTxnEventMemento const &checkpointMemento,
    SharedSegInputStream pTxnInputStream)
{
    LogicalTxnSavepoint svpt;
    svpt.cbActionPrev = checkpointMemento.cbActionLast;
    svpt.cbLogged = checkpointMemento.logPosition.cbOffset;
    pTxnInputStream->seekSegPos(checkpointMemento.logPosition);
    LogicalRecoveryTxn recoveryTxn(
        pTxnInputStream,
        &participantFactory);
    recoveryTxn.undoActions(svpt);
}

std::string LogicalRecoveryLog::getLongLogFileName(TxnId txnId)
{
    std::ostringstream oss;
    oss << "txn";
    oss << txnId;
    oss << ".dat";
    return oss.str();
}

FENNEL_END_CPPFILE("$Id$");

// End LogicalRecoveryLog.cpp

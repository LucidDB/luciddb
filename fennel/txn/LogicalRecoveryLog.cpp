/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
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
        logSegmentAccessor,onlineUuid);
}

SharedLogicalRecoveryLog LogicalRecoveryLog::newLogicalRecoveryLog(
    LogicalTxnParticipantFactory &participantFactory,
    SegmentAccessor const &logSegmentAccessor,
    PseudoUuid const &onlineUuid,
    SharedSegmentFactory pSegmentFactory)
{
    return SharedLogicalRecoveryLog(
        new LogicalRecoveryLog(
            participantFactory,logSegmentAccessor,onlineUuid,pSegmentFactory));
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
        switch(txnMemento.event) {
        case LogicalTxnEventMemento::EVENT_COMMIT:
            if (pTxnEntry == checkpointTxnMap.end()) {
                redoTxn(txnMemento,NULL,pTxnInputStream);
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
            undoTxn(pTxnEntry->second,pTxnInputStream);
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
        undoTxn(pTxnEntry->second,pTxnInputStream);
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

/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/segment/CrcSegOutputStream.h"
#include "fennel/segment/SpillOutputStream.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/cache/QuotaCacheAccessor.h"

#include <boost/bind.hpp>
#include <sstream>

FENNEL_BEGIN_CPPFILE("$Id$");

// TODO:  factor out the management of long log segments via a separate
// extensibility interface

LogicalTxnLog::LogicalTxnLog(
    SegmentAccessor const &logSegmentAccessorInit,
    PseudoUuid const &onlineUuid,
    SharedSegmentFactory pSegmentFactoryInit)
    : pSegmentFactory(pSegmentFactoryInit),
      logSegmentAccessor(logSegmentAccessorInit)
{
    // Set up cache accessor so that all page locks will be taken out
    // with a reserved TxnId.  Just for sanity-checking, set up a quota to make
    // sure logging never locks more than two pages at a time.
    logSegmentAccessor.pCacheAccessor = SharedCacheAccessor(
        new QuotaCacheAccessor(
            SharedQuotaCacheAccessor(),
            logSegmentAccessor.pCacheAccessor,
            2));
    
    // TODO: Support an option to skip CRC's for optimized non-durable logging.
    // Also support a paranoid option for recording CRC's for long logs.
    pOutputStream = CrcSegOutputStream::newCrcSegOutputStream(
        logSegmentAccessor,onlineUuid);

    // NOTE:  We only write one page at a time to the main log, and we always
    // need to wait after each page.  So request synchronous writes.
    pOutputStream->setWriteLatency(WRITE_EAGER_SYNC);
    
    pOutputStream->getSegPos(lastCheckpointMemento.logPosition);
    lastCheckpointMemento.nUncommittedTxns = 0;
    nCommittedBeforeLastCheckpoint = 0;

    groupCommitInterval = pSegmentFactory->getConfigMap().getIntParam(
        "groupCommitInterval", 30);
}

void LogicalTxnLog::setNextTxnId(TxnId nextTxnIdInit)
{
    nextTxnId = nextTxnIdInit;
    logSegmentAccessor.pCacheAccessor->setTxnId(nextTxnId);
    nextTxnId++;
}

SharedLogicalTxnLog LogicalTxnLog::newLogicalTxnLog(
    SegmentAccessor const &logSegmentAccessor,
    PseudoUuid const &onlineUuid,
    SharedSegmentFactory pSegmentFactory)
{
    return SharedLogicalTxnLog(
        new LogicalTxnLog(logSegmentAccessor,onlineUuid,pSegmentFactory));
}

LogicalTxnLog::~LogicalTxnLog()
{
    assert(uncommittedTxns.empty());
    assert(committedLongLogSegments.empty());
}

SharedLogicalTxn LogicalTxnLog::newLogicalTxn(
    SharedCacheAccessor pCacheAccessor)
{
    StrictMutexGuard mutexGuard(mutex);
    // Set up cache accessor so that all page locks will be taken out
    // with the new TxnId.  Just for sanity-checking, set up a quota to make
    // sure logging never locks more than two pages at a time.
    pCacheAccessor = SharedCacheAccessor(
        new QuotaCacheAccessor(
            SharedQuotaCacheAccessor(),
            pCacheAccessor,
            2));
    pCacheAccessor->setTxnId(nextTxnId);
    SharedLogicalTxn pTxn(
        new LogicalTxn(nextTxnId,shared_from_this(),pCacheAccessor));
    uncommittedTxns.push_back(pTxn);
    ++nextTxnId;
    return pTxn;
}

void LogicalTxnLog::removeTxn(SharedLogicalTxn pTxn)
{
    TxnListIter pFound = std::find(
        uncommittedTxns.begin(),
        uncommittedTxns.end(),
        pTxn);
    assert(pFound != uncommittedTxns.end());
    uncommittedTxns.erase(pFound);
}

void LogicalTxnLog::commitTxn(SharedLogicalTxn pTxn)
{
    LogicalTxnEventMemento memento;
    memento.event = LogicalTxnEventMemento::EVENT_COMMIT;
    memento.txnId = pTxn->txnId;
    memento.cbActionLast = pTxn->svpt.cbActionPrev;
    memento.nParticipants = pTxn->participants.size();
    SharedSegment pSegment = pTxn->pOutputStream->getSegment();
    if (pSegment) {
        assert(pTxn->pOutputStream.unique());
        pTxn->pOutputStream->hardPageBreak();
        pTxn->pOutputStream->getSegOutputStream()->getSegPos(
            memento.logPosition);
        pTxn->pOutputStream.reset();
        pSegment->checkpoint(CHECKPOINT_FLUSH_AND_UNMAP);
        StrictMutexGuard mutexGuard(mutex);
        committedLongLogSegments.push_back(pSegment);
    } else {
        if (!pTxn->svpt.cbLogged) {
            // NOTE jvs 27-Feb-2006: "empty commit" is an important
            // optimization for queries in autocommit mode, where JDBC
            // specifies a commit whenever a cursor is closed.
            StrictMutexGuard mutexGuard(mutex);
            removeTxn(pTxn);
            return;
        }
        CompoundId::setPageId(memento.logPosition.segByteId,NULL_PAGE_ID);
        CompoundId::setByteOffset(
            memento.logPosition.segByteId,
            pTxn->svpt.cbLogged);
        memento.logPosition.cbOffset = pTxn->svpt.cbLogged;
    }
    memento.longLog = pSegment ? true : false;
    StrictMutexGuard mutexGuard(mutex);
    pOutputStream->writeValue(memento);
    if (!pSegment) {
        SharedByteInputStream pInputStream =
            pTxn->pOutputStream->getInputStream();
        uint cbActual;
        PConstBuffer pBuffer = pInputStream->getReadPointer(1,&cbActual);
        pOutputStream->writeBytes(pBuffer,cbActual);
    }

    commitTxnWithGroup(mutexGuard);
    removeTxn(pTxn);
}

void LogicalTxnLog::commitTxnWithGroup(StrictMutexGuard &mutexGuard)
{
    boost::xtime groupCommitExpiration;
    if (groupCommitInterval) {
        convertTimeout(groupCommitInterval,groupCommitExpiration);
    }
    SegStreamPosition logPos;
    pOutputStream->getSegPos(logPos);
    PageId startPageId = CompoundId::getPageId(logPos.segByteId);
    for (;;) {
        bool timeout = true;
        if (groupCommitInterval) {
            timeout = !condition.timed_wait(mutexGuard,groupCommitExpiration);

            pOutputStream->getSegPos(logPos);
            PageId lastPageId = CompoundId::getPageId(logPos.segByteId);
            if (lastPageId != startPageId) {
                // someone else has flushed for us
                break;
            }
        }

        if (timeout) {
            // timeout:  we're in charge of flushing
            
            // NOTE:  Since we're using synchronous writes, there's no need to
            // checkpoint (assuming the underlying device has been correctly
            // initialized to write through).
            pOutputStream->hardPageBreak();
            condition.notify_all();
            break;
        } else {
            // spurious wakeup:  go 'round again
        }
    }
}

void LogicalTxnLog::rollbackTxn(SharedLogicalTxn pTxn)
{
    if (!pTxn->checkpointed) {
        // we never stored a checkpoint record for this txn, so during recovery
        // it can be ignored entirely
        StrictMutexGuard mutexGuard(mutex);
        removeTxn(pTxn);
        return;
    }
    // otherwise, write an EVENT_ROLLBACK so that the txn's fate is known
    // during recovery (eliminating the need for multiple passes over the log)
    LogicalTxnEventMemento memento;
    memento.event = LogicalTxnEventMemento::EVENT_ROLLBACK;
    memento.txnId = pTxn->txnId;
    memento.cbActionLast = 0;
    memento.nParticipants = 0;
    CompoundId::setPageId(memento.logPosition.segByteId,NULL_PAGE_ID);
    CompoundId::setByteOffset(memento.logPosition.segByteId,0);
    memento.logPosition.cbOffset = 0;
    memento.longLog = true;
    StrictMutexGuard mutexGuard(mutex);
    pOutputStream->writeValue(memento);
    // no need for group commit since caller doesn't need to wait for
    // commit confirmation
    removeTxn(pTxn);
}

void LogicalTxnLog::checkpoint(
    LogicalTxnLogCheckpointMemento &memento,
    CheckpointType checkpointType)
{
    StrictMutexGuard mutexGuard(mutex);
    if (checkpointType == CHECKPOINT_DISCARD) {
        uncommittedTxns.clear();
        committedLongLogSegments.clear();
        return;
    }
    pOutputStream->getSegPos(memento.logPosition);
    memento.nUncommittedTxns = uncommittedTxns.size();
    memento.nextTxnId = nextTxnId;
    std::for_each(
        uncommittedTxns.begin(),
        uncommittedTxns.end(),
        boost::bind(&LogicalTxnLog::checkpointTxn,this,_1));
    pOutputStream->hardPageBreak();
    logSegmentAccessor.pSegment->checkpoint(checkpointType);
    if (checkpointType == CHECKPOINT_FLUSH_FUZZY) {
        // memento gets lastCheckpointMemento, and lastCheckpointMemento gets
        // new memento just created above
        std::swap(memento,lastCheckpointMemento);
    }
}

void LogicalTxnLog::deallocateCheckpointedLog(
    LogicalTxnLogCheckpointMemento const &memento,
    CheckpointType checkpointType)
{
    PageId lastObsoletePageId =
        CompoundId::getPageId(memento.logPosition.segByteId);
    if (lastObsoletePageId != FIRST_LINEAR_PAGE_ID) {
        assert(lastObsoletePageId != NULL_PAGE_ID);
        // Segment::deallocatePageRange is inclusive, so decrement to
        // exclude the checkpoint page
        CompoundId::decBlockNum(lastObsoletePageId);
        if (logSegmentAccessor.pSegment->isPageIdAllocated(
                lastObsoletePageId))
        {
            logSegmentAccessor.pSegment->deallocatePageRange(
                NULL_PAGE_ID,lastObsoletePageId);
        }
    }
    
    if (checkpointType == CHECKPOINT_FLUSH_FUZZY) {
        committedLongLogSegments.erase(
            committedLongLogSegments.begin(),
            committedLongLogSegments.begin() + nCommittedBeforeLastCheckpoint);
    } else {
        committedLongLogSegments.clear();
    }
    nCommittedBeforeLastCheckpoint = committedLongLogSegments.size();
}

void LogicalTxnLog::checkpointTxn(SharedLogicalTxn pTxn)
{
    // NOTE: hardPageBreak will automatically convert small txns into large
    // ones.  It would probably be better to record their incomplete state in
    // the main log instead.
    LogicalTxnEventMemento memento;
    pTxn->describeAllParticipants();
    pTxn->pOutputStream->hardPageBreak();
    pTxn->pOutputStream->getSegOutputStream()->getSegPos(
        memento.logPosition);
    // TODO:  see previous comment on pLogSegment->checkpoint()
    pTxn->pOutputStream->getSegment()->checkpoint();
    memento.event = LogicalTxnEventMemento::EVENT_CHECKPOINT;
    memento.txnId = pTxn->txnId;
    memento.cbActionLast = pTxn->svpt.cbActionPrev;
    memento.longLog = true;
    memento.nParticipants = pTxn->participants.size();
    pOutputStream->writeValue(memento);
    pTxn->checkpointed = true;
}

TxnId LogicalTxnLog::getOldestActiveTxnId()
{
    StrictMutexGuard mutexGuard(mutex);
    TxnId oldestTxnId = NULL_TXN_ID;
    for (TxnListIter ppTxn = uncommittedTxns.begin();
        ppTxn != uncommittedTxns.end();
        ++ppTxn)
    {
        SharedLogicalTxn pTxn = *ppTxn;
        if (oldestTxnId == NULL_TXN_ID || pTxn->getTxnId() < oldestTxnId) {
            oldestTxnId = pTxn->getTxnId();
        }
    }

    // If there are no active txns, return the txnId that will be assigned to
    // the next, new txn
    if (oldestTxnId == NULL_TXN_ID) {
        return nextTxnId;
    } else {
        return oldestTxnId;
    }
}

FENNEL_END_CPPFILE("$Id: //open/lu/dev/fennel/txn/LogicalTxnLog.cpp#11 $");

// End LogicalTxnLog.cpp

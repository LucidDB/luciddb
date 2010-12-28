/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalRecoveryLog.h"
#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/txn/LogicalTxnParticipant.h"
#include "fennel/txn/LogicalTxnParticipantFactory.h"
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/cache/Cache.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class LogicalTxnTest
    : virtual public SegStorageTestBase,
        public LogicalTxnParticipant,
        public LogicalTxnParticipantFactory
{
    static const int participantDescription;
    static const LogicalActionType ACTION_TEST;

    SharedLogicalTxnLog pTxnLog;
    LogicalTxnLogCheckpointMemento firstCheckpointMemento;
    LogicalTxnLogCheckpointMemento intermediateCheckpointMemento;
    LogicalTxnLogCheckpointMemento finalCheckpointMemento;
    SavepointId svptId;
    PseudoUuid onlineUuid;

    typedef std::pair<int, int> ExpectedRange;
    std::vector<ExpectedRange> expected;

    void rollbackFull();
    void commit();
    void checkpointTxnLog(LogicalTxnLogCheckpointMemento &);
    SharedLogicalRecoveryLog createRecoveryLog();

public:
    explicit LogicalTxnTest()
    {
        onlineUuid.generateInvalid();

        // TODO jvs 26-Oct-2007:  need multi-threading tests,
        // e.g. for FNL-68

        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testTxnIdSequence);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testRollbackEmpty);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testRollbackShort);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testRollbackLong);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testRollbackSavepointNoGap);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testRollbackSavepointGap);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCheckpointCommitSavepoint);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCommitEmpty);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCommitShort);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCommitLong);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCheckpointCommitEmpty);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCheckpointCommitShort);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCheckpointCommitLong);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCheckpointRollbackShort);
        FENNEL_UNIT_TEST_CASE(LogicalTxnTest, testCheckpointRollbackLong);
    }

    void testCaseSetUp()
    {
        expected.clear();
    }

    void testTxn(int nActions,int iCheckpoint = -1,int iSvpt = -1);
    void testActions(int nActions, int iFirst);

    void testRollback(
        int nActions,
        bool checkpoint = false);
    void testTxnIdSequence();
    void testRollbackEmpty();
    void testRollbackShort();
    void testRollbackLong();

    void testRollbackSavepointNoGap();
    void testRollbackSavepointGap();
    void testRollbackSavepoint(bool gap);
    void testCheckpointCommitSavepoint();

    void testCommit(int nActions, bool checkpoint = false);
    void testCommitEmpty();
    void testCommitShort();
    void testCommitLong();
    void testCheckpointCommitEmpty();
    void testCheckpointCommitShort();
    void testCheckpointCommitLong();
    void testCheckpointRollbackShort();
    void testCheckpointRollbackLong();

    // implement LogicalTxnParticipant
    virtual LogicalTxnClassId getParticipantClassId() const;
    virtual void describeParticipant(ByteOutputStream &logStream);
    virtual void redoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream);
    virtual void undoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream);

    // implement LogicalTxnParticipantFactory
    virtual SharedLogicalTxnParticipant loadParticipant(
        LogicalTxnClassId classId,
        ByteInputStream &logStream);
};

const int LogicalTxnTest::participantDescription = 42;
const LogicalActionType LogicalTxnTest::ACTION_TEST = 1;

LogicalTxnClassId LogicalTxnTest::getParticipantClassId() const
{
    return LogicalTxnClassId(0x83f6b9edfe168b93LL);
}

void LogicalTxnTest::describeParticipant(ByteOutputStream &logStream)
{
    int x = participantDescription;
    logStream.writeValue(x);
}

void LogicalTxnTest::checkpointTxnLog(
    LogicalTxnLogCheckpointMemento &memento)
{
    pTxnLog->checkpoint(memento);
    pTxnLog->deallocateCheckpointedLog(memento);
}

void LogicalTxnTest::undoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    // TODO:  symbolic const
    BOOST_CHECK_EQUAL(actionType, ACTION_TEST);
    int i;
    logStream.readValue(i);
    BOOST_CHECK(!expected.empty());
    ExpectedRange &range = expected.front();
    BOOST_CHECK_EQUAL(i, range.first);
    range.first--;
    if (range.first < range.second) {
        expected.erase(expected.begin());
    }
}

void LogicalTxnTest::redoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    // TODO:  symbolic const
    BOOST_CHECK_EQUAL(actionType, ACTION_TEST);
    int i;
    logStream.readValue(i);
    BOOST_CHECK(!expected.empty());
    ExpectedRange &range = expected.front();
    BOOST_CHECK_EQUAL(i, range.first);
    range.first++;
    if (range.first > range.second) {
        expected.erase(expected.begin());
    }
}

void LogicalTxnTest::testTxnIdSequence()
{
    testTxn(1);
    TxnId id1 = getLogicalTxn()->getTxnId();
    SharedLogicalTxn pTxn2 = pTxnLog->newLogicalTxn(pCache);
    TxnId id2 = pTxn2->getTxnId();
    pTxn2->commit();
    pTxn2.reset();
    commit();
    // TxnId reflects start order, not commit order
    BOOST_CHECK(id2 > id1);
}

void LogicalTxnTest::testRollbackEmpty()
{
    testRollback(0);
}

void LogicalTxnTest::testRollbackShort()
{
    testRollback(10);
}

void LogicalTxnTest::testRollbackLong()
{
    testRollback(10000);
}

void LogicalTxnTest::testRollbackSavepointNoGap()
{
    testRollbackSavepoint(false);
}

void LogicalTxnTest::testRollbackSavepointGap()
{
    testRollbackSavepoint(true);
}

void LogicalTxnTest::testRollbackSavepoint(bool gap)
{
    // log actions 0 through 99, creating a savepoint after 50
    testTxn(100,-1,50);

    // rollback 99 through 51
    expected.push_back(ExpectedRange(99, 51));
    getLogicalTxn()->rollback(&svptId);
    BOOST_CHECK(expected.empty());

    if (gap) {
        // log 40 new actions (200 through 239)
        testActions(40, 200);
        expected.push_back(ExpectedRange(239, 200));
    }

    // roll everything back
    expected.push_back(ExpectedRange(50, 0));
    rollbackFull();
}

SharedLogicalRecoveryLog LogicalTxnTest::createRecoveryLog()
{
    SegmentAccessor segmentAccessor(pLinearSegment, pCache);
    SharedLogicalRecoveryLog pRecoveryLog =
        LogicalRecoveryLog::newLogicalRecoveryLog(
            *this,
            segmentAccessor,
            onlineUuid,
            pSegmentFactory);
    return pRecoveryLog;
}

void LogicalTxnTest::testCheckpointCommitSavepoint()
{
    // log actions 0 through 99, checkpointing after 75 and creating a
    // savepoint after 50
    testTxn(100, 75, 50);

    // rollback 99 through 51
    expected.push_back(ExpectedRange(99, 51));
    getLogicalTxn()->rollback(&svptId);
    BOOST_CHECK(expected.empty());

    // log 40 new actions (200 through 239)
    testActions(40, 200);

    commit();
    SharedLogicalRecoveryLog pRecoveryLog = createRecoveryLog();

    // recovery should first see 76 through 99 (redo),
    // then 99 through 51 (undo),
    // then 200 through 239 (redo)
    expected.push_back(ExpectedRange(76, 99));
    expected.push_back(ExpectedRange(99, 51));
    expected.push_back(ExpectedRange(200, 239));
    pRecoveryLog->recover(intermediateCheckpointMemento);
    BOOST_CHECK(expected.empty());
    assert(pRecoveryLog.unique());
}

void LogicalTxnTest::testCheckpointRollbackShort()
{
    testRollback(10, true);
}

void LogicalTxnTest::testCheckpointRollbackLong()
{
    testRollback(10000, true);
}

void LogicalTxnTest::testCommitEmpty()
{
    testCommit(0);
}

void LogicalTxnTest::testCommitShort()
{
    testCommit(10);
}

void LogicalTxnTest::testCommitLong()
{
    testCommit(10000);
}

void LogicalTxnTest::testCheckpointCommitEmpty()
{
    testCommit(0, true);
}

void LogicalTxnTest::testCheckpointCommitShort()
{
    testCommit(10, true);
}

void LogicalTxnTest::testCheckpointCommitLong()
{
    testCommit(10000, true);
}

void LogicalTxnTest::testRollback(
    int nActions,
    bool checkpoint)
{
    int iCheckpoint = checkpoint ? (nActions / 2) : -1;
    testTxn(nActions, iCheckpoint);
    if (checkpoint) {
        SharedLogicalRecoveryLog pRecoveryLog = createRecoveryLog();
        expected.push_back(ExpectedRange(iCheckpoint, 0));
        pRecoveryLog->recover(intermediateCheckpointMemento);
        BOOST_CHECK(expected.empty());
        assert(pRecoveryLog.unique());
    }
    if (nActions) {
        expected.push_back(ExpectedRange(nActions - 1, 0));
    }
    rollbackFull();
}

void LogicalTxnTest::rollbackFull()
{
    getLogicalTxn()->rollback();
    BOOST_CHECK(expected.empty());
    checkpointTxnLog(finalCheckpointMemento);
    assert(pTxnLog.unique());
    pTxnLog.reset();
}

void LogicalTxnTest::commit()
{
    getLogicalTxn()->commit();
    checkpointTxnLog(finalCheckpointMemento);
    assert(pTxnLog.unique());
    pTxnLog.reset();
}

void LogicalTxnTest::testCommit(int nActions, bool checkpoint)
{
    int iCheckpoint = checkpoint ? (nActions / 2) : -1;
    testTxn(nActions, iCheckpoint);
    commit();

    SharedLogicalRecoveryLog pRecoveryLog = createRecoveryLog();
    if (checkpoint) {
        if (nActions) {
            expected.push_back(ExpectedRange(iCheckpoint + 1, nActions - 1));
        }
        pRecoveryLog->recover(intermediateCheckpointMemento);
    } else {
        if (nActions) {
            expected.push_back(ExpectedRange(0, nActions - 1));
        }
        pRecoveryLog->recover(firstCheckpointMemento);
    }
    BOOST_CHECK(expected.empty());
    assert(pRecoveryLog.unique());
}

void LogicalTxnTest::testTxn(int nActions, int iCheckpoint, int iSvpt)
{
    openStorage(DeviceMode::createNew);
    SegmentAccessor segmentAccessor(pLinearSegment, pCache);
    pTxnLog = LogicalTxnLog::newLogicalTxnLog(
        segmentAccessor, onlineUuid, pSegmentFactory);
    checkpointTxnLog(firstCheckpointMemento);
    SharedLogicalTxn pTxn = pTxnLog->newLogicalTxn(pCache);
    pTxn->addParticipant(
        boost::dynamic_pointer_cast<LogicalTxnParticipant>(
            shared_from_this()));
    for (int i = 0; i < nActions; ++i) {
        ByteOutputStream &logStream =
            getLogicalTxn()->beginLogicalAction(*this, ACTION_TEST);
        logStream.writeValue(i);
        getLogicalTxn()->endLogicalAction();
        if (i == iCheckpoint) {
            checkpointTxnLog(intermediateCheckpointMemento);
        }
        if (i == iSvpt) {
            svptId = getLogicalTxn()->createSavepoint();
        }
    }
    if (!nActions && !iCheckpoint) {
        checkpointTxnLog(intermediateCheckpointMemento);
    }
}

void LogicalTxnTest::testActions(int nActions, int iFirst)
{
    for (int i = 0; i < nActions; ++i) {
        ByteOutputStream &logStream =
            getLogicalTxn()->beginLogicalAction(*this, ACTION_TEST);
        int x = iFirst + i;
        logStream.writeValue(x);
        getLogicalTxn()->endLogicalAction();
    }
}

SharedLogicalTxnParticipant LogicalTxnTest::loadParticipant(
    LogicalTxnClassId classId,
    ByteInputStream &logStream)
{
    BOOST_CHECK_EQUAL(classId, getParticipantClassId());
    int x;
    logStream.readValue(x);
    BOOST_CHECK_EQUAL(x, participantDescription);
    return boost::dynamic_pointer_cast<LogicalTxnParticipant>(
        shared_from_this());
}

FENNEL_UNIT_TEST_SUITE(LogicalTxnTest);

// End LogicalTxnTest.cpp

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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

class TestLogicalTxn
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

    typedef std::pair<int,int> ExpectedRange;
    std::vector<ExpectedRange> expected;

    void rollbackFull();
    void commit();
    void checkpointTxnLog(LogicalTxnLogCheckpointMemento &);
    SharedLogicalRecoveryLog createRecoveryLog();

public:
    explicit TestLogicalTxn()
    {
        onlineUuid.generateInvalid();
        
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testRollbackEmpty);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testRollbackShort);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testRollbackLong);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testRollbackSavepointNoGap);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testRollbackSavepointGap);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCheckpointCommitSavepoint);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCommitEmpty);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCommitShort);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCommitLong);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCheckpointCommitEmpty);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCheckpointCommitShort);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCheckpointCommitLong);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCheckpointRollbackShort);
        FENNEL_UNIT_TEST_CASE(TestLogicalTxn,testCheckpointRollbackLong);
    }

    void testCaseSetUp()
    {
        expected.clear();
    }

    void testTxn(int nActions,int iCheckpoint = -1,int iSvpt = -1);
    void testActions(int nActions,int iFirst);
    
    void testRollback(
        int nActions,
        bool checkpoint = false);
    void testRollbackEmpty();
    void testRollbackShort();
    void testRollbackLong();
    
    void testRollbackSavepointNoGap();
    void testRollbackSavepointGap();
    void testRollbackSavepoint(bool gap);
    void testCheckpointCommitSavepoint();

    void testCommit(int nActions,bool checkpoint = false);
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

const int TestLogicalTxn::participantDescription = 42;
const LogicalActionType TestLogicalTxn::ACTION_TEST = 1;

LogicalTxnClassId TestLogicalTxn::getParticipantClassId() const
{
    return LogicalTxnClassId(0x83f6b9edfe168b93LL);
}

void TestLogicalTxn::describeParticipant(ByteOutputStream &logStream)
{
    int x = participantDescription;
    logStream.writeValue(x);
}

void TestLogicalTxn::checkpointTxnLog(
    LogicalTxnLogCheckpointMemento &memento)
{
    pTxnLog->checkpoint(memento);
    pTxnLog->deallocateCheckpointedLog(memento);
}

void TestLogicalTxn::undoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    // TODO:  symbolic const
    BOOST_CHECK_EQUAL(actionType,ACTION_TEST);
    int i;
    logStream.readValue(i);
    BOOST_CHECK(!expected.empty());
    ExpectedRange &range = expected.front();
    BOOST_CHECK_EQUAL(i,range.first);
    range.first--;
    if (range.first < range.second) {
        expected.erase(expected.begin());
    }
}

void TestLogicalTxn::redoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    // TODO:  symbolic const
    BOOST_CHECK_EQUAL(actionType,ACTION_TEST);
    int i;
    logStream.readValue(i);
    BOOST_CHECK(!expected.empty());
    ExpectedRange &range = expected.front();
    BOOST_CHECK_EQUAL(i,range.first);
    range.first++;
    if (range.first > range.second) {
        expected.erase(expected.begin());
    }
}

void TestLogicalTxn::testRollbackEmpty()
{
    testRollback(0);
}

void TestLogicalTxn::testRollbackShort()
{
    testRollback(10);
}

void TestLogicalTxn::testRollbackLong()
{
    testRollback(10000);
}

void TestLogicalTxn::testRollbackSavepointNoGap()
{
    testRollbackSavepoint(false);
}

void TestLogicalTxn::testRollbackSavepointGap()
{
    testRollbackSavepoint(true);
}

void TestLogicalTxn::testRollbackSavepoint(bool gap)
{
    // log actions 0 through 99, creating a savepoint after 50
    testTxn(100,-1,50);

    // rollback 99 through 51
    expected.push_back(ExpectedRange(99,51));
    getLogicalTxn()->rollback(&svptId);
    BOOST_CHECK(expected.empty());

    if (gap) {
        // log 40 new actions (200 through 239)
        testActions(40,200);
        expected.push_back(ExpectedRange(239,200));
    }
        
    // roll everything back
    expected.push_back(ExpectedRange(50,0));
    rollbackFull();
}

SharedLogicalRecoveryLog TestLogicalTxn::createRecoveryLog()
{
    SegmentAccessor segmentAccessor(pLinearSegment,pCache);
    SharedLogicalRecoveryLog pRecoveryLog =
        LogicalRecoveryLog::newLogicalRecoveryLog(
            *this,
            segmentAccessor,
            onlineUuid,
            pSegmentFactory);
    return pRecoveryLog;
}

void TestLogicalTxn::testCheckpointCommitSavepoint()
{
    // log actions 0 through 99, checkpointing after 75 and creating a
    // savepoint after 50
    testTxn(100,75,50);

    // rollback 99 through 51
    expected.push_back(ExpectedRange(99,51));
    getLogicalTxn()->rollback(&svptId);
    BOOST_CHECK(expected.empty());

    // log 40 new actions (200 through 239)
    testActions(40,200);
    
    commit();
    SharedLogicalRecoveryLog pRecoveryLog = createRecoveryLog();

    // recovery should first see 76 through 99 (redo),
    // then 99 through 51 (undo),
    // then 200 through 239 (redo)
    expected.push_back(ExpectedRange(76,99));
    expected.push_back(ExpectedRange(99,51));
    expected.push_back(ExpectedRange(200,239));
    pRecoveryLog->recover(intermediateCheckpointMemento);
    BOOST_CHECK(expected.empty());
    assert(pRecoveryLog.unique());
}

void TestLogicalTxn::testCheckpointRollbackShort()
{
    testRollback(10,true);
}

void TestLogicalTxn::testCheckpointRollbackLong()
{
    testRollback(10000,true);
}

void TestLogicalTxn::testCommitEmpty()
{
    testCommit(0);
}

void TestLogicalTxn::testCommitShort()
{
    testCommit(10);
}

void TestLogicalTxn::testCommitLong()
{
    testCommit(10000);
}

void TestLogicalTxn::testCheckpointCommitEmpty()
{
    testCommit(0,true);
}

void TestLogicalTxn::testCheckpointCommitShort()
{
    testCommit(10,true);
}

void TestLogicalTxn::testCheckpointCommitLong()
{
    testCommit(10000,true);
}

void TestLogicalTxn::testRollback(
    int nActions,
    bool checkpoint)
{
    int iCheckpoint = checkpoint ? nActions/2 : -1;
    testTxn(nActions,iCheckpoint);
    if (checkpoint) {
        SharedLogicalRecoveryLog pRecoveryLog = createRecoveryLog();
        expected.push_back(ExpectedRange(iCheckpoint,0));
        pRecoveryLog->recover(intermediateCheckpointMemento);
        BOOST_CHECK(expected.empty());
        assert(pRecoveryLog.unique());
    }
    if (nActions) {
        expected.push_back(ExpectedRange(nActions-1,0));
    }
    rollbackFull();
}

void TestLogicalTxn::rollbackFull()
{
    getLogicalTxn()->rollback();
    BOOST_CHECK(expected.empty());
    checkpointTxnLog(finalCheckpointMemento);
    assert(pTxnLog.unique());
    pTxnLog.reset();
}

void TestLogicalTxn::commit()
{
    getLogicalTxn()->commit();
    checkpointTxnLog(finalCheckpointMemento);
    assert(pTxnLog.unique());
    pTxnLog.reset();
}

void TestLogicalTxn::testCommit(int nActions,bool checkpoint)
{
    int iCheckpoint = checkpoint ? nActions/2 : -1;
    testTxn(nActions,iCheckpoint);
    commit();

    SharedLogicalRecoveryLog pRecoveryLog = createRecoveryLog();
    if (checkpoint) {
        if (nActions) {
            expected.push_back(ExpectedRange(iCheckpoint+1,nActions-1));
        }
        pRecoveryLog->recover(intermediateCheckpointMemento);
    } else {
        if (nActions) {
            expected.push_back(ExpectedRange(0,nActions-1));
        }
        pRecoveryLog->recover(firstCheckpointMemento);
    }
    BOOST_CHECK(expected.empty());
    assert(pRecoveryLog.unique());
}

void TestLogicalTxn::testTxn(int nActions,int iCheckpoint,int iSvpt)
{
    openStorage(DeviceMode::createNew);
    SegmentAccessor segmentAccessor(pLinearSegment,pCache);
    pTxnLog = LogicalTxnLog::newLogicalTxnLog(
        segmentAccessor,onlineUuid,pSegmentFactory);
    checkpointTxnLog(firstCheckpointMemento);
    SharedLogicalTxn pTxn = pTxnLog->newLogicalTxn(pCache);
    pTxn->addParticipant(
        boost::dynamic_pointer_cast<LogicalTxnParticipant>(
            shared_from_this()));
    for (int i = 0; i < nActions; ++i) {
        ByteOutputStream &logStream =
            getLogicalTxn()->beginLogicalAction(*this,ACTION_TEST);
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

void TestLogicalTxn::testActions(int nActions,int iFirst)
{
    for (int i = 0; i < nActions; ++i) {
        ByteOutputStream &logStream =
            getLogicalTxn()->beginLogicalAction(*this,ACTION_TEST);
        int x = iFirst + i;
        logStream.writeValue(x);
        getLogicalTxn()->endLogicalAction();
    }
}

SharedLogicalTxnParticipant TestLogicalTxn::loadParticipant(
    LogicalTxnClassId classId,
    ByteInputStream &logStream)
{
    BOOST_CHECK_EQUAL(classId,getParticipantClassId());
    int x;
    logStream.readValue(x);
    BOOST_CHECK_EQUAL(x,participantDescription);
    return boost::dynamic_pointer_cast<LogicalTxnParticipant>(
        shared_from_this());
}

FENNEL_UNIT_TEST_SUITE(TestLogicalTxn);

// End TestLogicalTxn.cpp

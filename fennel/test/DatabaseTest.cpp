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

#include "fennel/common/CommonPreamble.h"
#include "fennel/test/TestBase.h"
#include "fennel/db/Database.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/segment/Segment.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalTxnParticipant.h"
#include "fennel/txn/LogicalTxnParticipantFactory.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/common/ByteOutputStream.h"
#include "fennel/segment/SegPageLock.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

class DatabaseTest
    : virtual public TestBase,
        public LogicalTxnParticipant,
        public LogicalTxnParticipantFactory
{
    static const LogicalActionType ACTION_INCREMENT;

    static const LogicalActionType ACTION_INCREMENT_FORCE;

    struct TestNode : public StoredNode
    {
        static const MagicNumber MAGIC_NUMBER = 0xa496c71bff0d41bdLL;

        uint x;
    };

    typedef SegNodeLock<TestNode> TestPageLock;

    SharedCache pCache;
    SharedDatabase pDatabase;
    PageId persistentPageId;

    void loadDatabase();
    void executeIncrementAction(int i);
    void executeIncrementAction(int i, LogicalActionType action);
    void executeIncrementTxn(int i);
    void executeCheckpointedTxn(
        int i, int j, bool commit, CheckpointType = CHECKPOINT_FLUSH_ALL);
    void verifyData(uint x);
    PageId writeData(uint x);
    void addTxnParticipant(SharedLogicalTxn);

public:
    explicit DatabaseTest()
    {
        configMap.setStringParam(
            Database::paramDatabaseDir, ".");
        configMap.setStringParam(
            "databaseInitSize", "1000");
        configMap.setStringParam(
            "tempInitSize", "1000");
        configMap.setStringParam(
            "databaseShadowLogInitSize", "1000");
        configMap.setStringParam(
            "databaseTxnLogInitSize", "1000");

        CacheParams cacheParams;
        cacheParams.readConfig(configMap);
        pCache = Cache::newCache(cacheParams);

        // FIXME jvs 6-Mar-2006:  some of these tests depend on
        // being run in this sequence; make each test self-contained

        FENNEL_UNIT_TEST_CASE(DatabaseTest, testCreateEmpty);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testLoadEmpty);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testRecoverEmpty);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testCreateData);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testLoadData);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testRecoverDataWithFlush);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testRecoverDataWithoutFlush);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testRecoverDataFromCheckpoint);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testRecoverDataFromFuzzyCheckpoint);
        FENNEL_UNIT_TEST_CASE(DatabaseTest, testRecoverDataFromRollback);

        FENNEL_UNIT_TEST_CASE(DatabaseTest, testForceTxns);
    }

    virtual ~DatabaseTest()
    {
    }

    virtual void testCaseTearDown()
    {
        pDatabase.reset();
    }

    void testCreateEmpty();
    void testLoadEmpty();
    void testRecoverEmpty();

    void testCreateData();
    void testLoadData();
    void testRecoverData(bool);
    void testRecoverDataFromCheckpoint(CheckpointType);
    void testRecoverDataFromCheckpoint();
    void testRecoverDataFromFuzzyCheckpoint();
    void testRecoverDataFromRollback();
    void testRecoverDataWithFlush();
    void testRecoverDataWithoutFlush();
    void testForceTxns();

    void executeForceTxn();

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

const LogicalActionType DatabaseTest::ACTION_INCREMENT = 1;

const LogicalActionType DatabaseTest::ACTION_INCREMENT_FORCE = 2;

void DatabaseTest::testCreateEmpty()
{
    pDatabase = Database::newDatabase(
        pCache,
        configMap,
        DeviceMode::createNew,
        shared_from_this());
    BOOST_CHECK(!pDatabase->isRecoveryRequired());
}

void DatabaseTest::testCreateData()
{
    testCreateEmpty();
    persistentPageId = writeData(0);
    pDatabase->checkpointImpl();
    executeIncrementTxn(5);
}

void DatabaseTest::testLoadEmpty()
{
    loadDatabase();
    BOOST_CHECK(!pDatabase->isRecoveryRequired());
}

void DatabaseTest::testLoadData()
{
    testLoadEmpty();
    verifyData(5);
}

void DatabaseTest::testRecoverEmpty()
{
    testCreateEmpty();
    // Flush the pages that have been created in the empty db,
    // then discard the checkpoint to simulate a crash
    pDatabase->checkpointImpl();
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
}

void DatabaseTest::testRecoverDataWithoutFlush()
{
    testRecoverData(false);
}

void DatabaseTest::testRecoverDataWithFlush()
{
    testRecoverData(true);
}

void DatabaseTest::testRecoverData(bool flush)
{
    testCreateData();
    executeIncrementTxn(10);
    executeIncrementTxn(30);
    if (flush) {
        pDatabase->getDataSegment()->checkpoint();
    }
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
    verifyData(45);
}

void DatabaseTest::testRecoverDataFromCheckpoint(CheckpointType checkpointType)
{
    testCreateData();
    executeIncrementTxn(10);
    executeCheckpointedTxn(25, 70, true, checkpointType);
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
    verifyData(110);
}

void DatabaseTest::testRecoverDataFromCheckpoint()
{
    testRecoverDataFromCheckpoint(CHECKPOINT_FLUSH_ALL);
}

void DatabaseTest::testRecoverDataFromFuzzyCheckpoint()
{
    testRecoverDataFromCheckpoint(CHECKPOINT_FLUSH_FUZZY);
}

void DatabaseTest::testRecoverDataFromRollback()
{
    testCreateData();
    executeIncrementTxn(10);
    executeCheckpointedTxn(25, 70, false);
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
    verifyData(15);
}

void DatabaseTest::testForceTxns()
{
    configMap.setStringParam(
        "forceTxns", "true");
    configMap.setStringParam(
        "disableSnapshots", "true");
    testCreateData();
    pDatabase->checkpointImpl();
    verifyData(5);

    // Allocate an extra page for use below.
    PageId extraPageId = writeData(42);

    pDatabase.reset();
    loadDatabase();

    // Pin the extra page to make sure that doesn't cause problems
    // for rollback on unrelated data.
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(), pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockShared(extraPageId);

    executeForceTxn();
    executeForceTxn();

    pageLock.unlock();

    pDatabase.reset();
}

void DatabaseTest::executeForceTxn()
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    addTxnParticipant(pTxn);
    executeIncrementAction(10, ACTION_INCREMENT_FORCE);
    verifyData(15);
    pTxn->rollback();
    pTxn.reset();
    // Give the background flush thread time to flush the data page
    snooze(3);
    pDatabase->recoverOnline();
    verifyData(5);
}

void DatabaseTest::loadDatabase()
{
    pDatabase = Database::newDatabase(
        pCache,
        configMap,
        DeviceMode::load,
        shared_from_this());
}

LogicalTxnClassId DatabaseTest::getParticipantClassId() const
{
    return LogicalTxnClassId(0xa470573b38dcaa0aLL);
}

void DatabaseTest::describeParticipant(ByteOutputStream &)
{
}

void DatabaseTest::undoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    int i;
    logStream.readValue(i);
    if (actionType == ACTION_INCREMENT_FORCE) {
        return;
    }
    assert(actionType == ACTION_INCREMENT);
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(), pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(persistentPageId);
    pageLock.getNodeForWrite().x -= i;
}

void DatabaseTest::redoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    int i;
    logStream.readValue(i);
    if (actionType == ACTION_INCREMENT_FORCE) {
        return;
    }
    assert(actionType == ACTION_INCREMENT);
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(), pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(persistentPageId);
    pageLock.getNodeForWrite().x += i;
}

SharedLogicalTxnParticipant DatabaseTest::loadParticipant(
    LogicalTxnClassId classId,
    ByteInputStream &)
{
    assert(classId == getParticipantClassId());
    return boost::dynamic_pointer_cast<LogicalTxnParticipant>(
        shared_from_this());
}

void DatabaseTest::executeIncrementAction(int i)
{
    executeIncrementAction(i, ACTION_INCREMENT);
}

void DatabaseTest::executeIncrementAction(int i, LogicalActionType action)
{
    ByteOutputStream &logStream =
        getLogicalTxn()->beginLogicalAction(*this,action);
    logStream.writeValue(i);
    getLogicalTxn()->endLogicalAction();
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(), pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(persistentPageId);
    pageLock.getNodeForWrite().x += i;
}

void DatabaseTest::addTxnParticipant(SharedLogicalTxn pTxn)
{
    pTxn->addParticipant(
        boost::dynamic_pointer_cast<LogicalTxnParticipant>(
            shared_from_this()));
}

void DatabaseTest::executeIncrementTxn(int i)
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    addTxnParticipant(pTxn);
    executeIncrementAction(i);
    pTxn->commit();
}

void DatabaseTest::executeCheckpointedTxn(
    int i, int j, bool commit, CheckpointType checkpointType)
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    addTxnParticipant(pTxn);
    executeIncrementAction(i);
    pDatabase->checkpointImpl(checkpointType);
    executeIncrementAction(j);
    if (commit) {
        pTxn->commit();
    } else {
        pTxn->rollback();
    }
}

void DatabaseTest::verifyData(uint x)
{
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(), pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockShared(persistentPageId);
    BOOST_CHECK_EQUAL(pageLock.getNodeForRead().x, x);
}

PageId DatabaseTest::writeData(uint x)
{
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(), pCache);
    TestPageLock pageLock(segmentAccessor);
    PageId pageId = pageLock.allocatePage();
    pageLock.getNodeForWrite().x = x;
    pageLock.unlock();
    return pageId;
}

FENNEL_UNIT_TEST_SUITE(DatabaseTest);

// End DatabaseTest.cpp

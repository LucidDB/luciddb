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

class TestDatabase
    : virtual public TestBase,
        public LogicalTxnParticipant,
        public LogicalTxnParticipantFactory
{
    static const LogicalActionType ACTION_INCREMENT;
    
    struct TestNode : public StoredNode
    {
        static const MagicNumber MAGIC_NUMBER = 0xa496c71bff0d41bdLL;

        uint x;
    };

    typedef SegNodeLock<TestNode> TestPageLock;
    
    SharedCache pCache;
    SharedDatabase pDatabase;
    PageId pageId;

    void loadDatabase();
    void executeIncrementAction(int i);
    void executeIncrementTxn(int i);
    void executeCheckpointedTxn(
        int i,int j,bool commit,CheckpointType = CHECKPOINT_FLUSH_ALL);
    void verifyData(uint x);
    void addTxnParticipant(SharedLogicalTxn);
    
public:
    explicit TestDatabase()
    {
        configMap.setStringParam(
            Database::paramDatabaseDir,".");
        configMap.setStringParam(
            "databaseInitSize","1000");
        configMap.setStringParam(
            "tempInitSize","1000");
        configMap.setStringParam(
            "databaseShadowLogInitSize","1000");
        configMap.setStringParam(
            "databaseTxnLogInitSize","1000");

        CacheParams cacheParams;
        cacheParams.readConfig(configMap);
        pCache = Cache::newCache(cacheParams);

        FENNEL_UNIT_TEST_CASE(TestDatabase,testCreateEmpty);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testLoadEmpty);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testRecoverEmpty);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testCreateData);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testLoadData);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testRecoverDataWithFlush);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testRecoverDataWithoutFlush);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testRecoverDataFromCheckpoint);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testRecoverDataFromFuzzyCheckpoint);
        FENNEL_UNIT_TEST_CASE(TestDatabase,testRecoverDataFromRollback);
    }
    
    virtual ~TestDatabase()
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

const LogicalActionType TestDatabase::ACTION_INCREMENT = 1;

void TestDatabase::testCreateEmpty()
{
    pDatabase.reset(
        new Database(
            pCache,
            configMap,
            DeviceMode::createNew,
            this));
    BOOST_CHECK(!pDatabase->isRecoveryRequired());
}

void TestDatabase::testCreateData()
{
    testCreateEmpty();
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageId = pageLock.allocatePage();
    pageLock.getNodeForWrite().x = 0;
    pageLock.unlock();
    pDatabase->checkpointImpl();
    executeIncrementTxn(5);
}

void TestDatabase::testLoadEmpty()
{
    loadDatabase();
    BOOST_CHECK(!pDatabase->isRecoveryRequired());
}

void TestDatabase::testLoadData()
{
    testLoadEmpty();
    verifyData(5);
}

void TestDatabase::testRecoverEmpty()
{
    testCreateEmpty();
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
}

void TestDatabase::testRecoverDataWithoutFlush()
{
    testRecoverData(false);
}

void TestDatabase::testRecoverDataWithFlush()
{
    testRecoverData(true);
}

void TestDatabase::testRecoverData(bool flush)
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

void TestDatabase::testRecoverDataFromCheckpoint(CheckpointType checkpointType)
{
    testCreateData();
    executeIncrementTxn(10);
    executeCheckpointedTxn(25,70,true,checkpointType);
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
    verifyData(110);
}

void TestDatabase::testRecoverDataFromCheckpoint()
{
    testRecoverDataFromCheckpoint(CHECKPOINT_FLUSH_ALL);
}

void TestDatabase::testRecoverDataFromFuzzyCheckpoint()
{
    testRecoverDataFromCheckpoint(CHECKPOINT_FLUSH_FUZZY);
}

void TestDatabase::testRecoverDataFromRollback()
{
    testCreateData();
    executeIncrementTxn(10);
    executeCheckpointedTxn(25,70,false);
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
    verifyData(15);
}

void TestDatabase::loadDatabase()
{
    pDatabase.reset(
        new Database(
            pCache,
            configMap,
            DeviceMode::load,
            this));
}

LogicalTxnClassId TestDatabase::getParticipantClassId() const
{
    return LogicalTxnClassId(0xa470573b38dcaa0aLL);
}

void TestDatabase::describeParticipant(ByteOutputStream &)
{
}

void TestDatabase::undoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    assert(actionType == ACTION_INCREMENT);
    int i;
    logStream.readValue(i);
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(pageId);
    pageLock.getNodeForWrite().x -= i;
}

void TestDatabase::redoLogicalAction(
    LogicalActionType actionType,
    ByteInputStream &logStream)
{
    assert(actionType == ACTION_INCREMENT);
    int i;
    logStream.readValue(i);
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(pageId);
    pageLock.getNodeForWrite().x += i;
}

SharedLogicalTxnParticipant TestDatabase::loadParticipant(
    LogicalTxnClassId classId,
    ByteInputStream &)
{
    assert(classId == getParticipantClassId());
    return boost::dynamic_pointer_cast<LogicalTxnParticipant>(
        shared_from_this());
}

void TestDatabase::executeIncrementAction(int i)
{
    ByteOutputStream &logStream =
        getLogicalTxn()->beginLogicalAction(*this,ACTION_INCREMENT);
    logStream.writeValue(i);
    getLogicalTxn()->endLogicalAction();
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(pageId);
    pageLock.getNodeForWrite().x += i;
}

void TestDatabase::addTxnParticipant(SharedLogicalTxn pTxn)
{
    pTxn->addParticipant(
        boost::dynamic_pointer_cast<LogicalTxnParticipant>(
            shared_from_this()));
}

void TestDatabase::executeIncrementTxn(int i)
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    addTxnParticipant(pTxn);
    executeIncrementAction(i);
    pTxn->commit();
}

void TestDatabase::executeCheckpointedTxn(
    int i,int j,bool commit,CheckpointType checkpointType)
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

void TestDatabase::verifyData(uint x)
{
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockShared(pageId);
    BOOST_CHECK_EQUAL(pageLock.getNodeForRead().x,x);
}

FENNEL_UNIT_TEST_SUITE(TestDatabase);

// End TestDatabase.cpp

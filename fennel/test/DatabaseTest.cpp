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
    explicit DatabaseTest()
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

        FENNEL_UNIT_TEST_CASE(DatabaseTest,testCreateEmpty);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testLoadEmpty);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testRecoverEmpty);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testCreateData);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testLoadData);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testRecoverDataWithFlush);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testRecoverDataWithoutFlush);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testRecoverDataFromCheckpoint);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testRecoverDataFromFuzzyCheckpoint);
        FENNEL_UNIT_TEST_CASE(DatabaseTest,testRecoverDataFromRollback);
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

void DatabaseTest::testCreateEmpty()
{
    pDatabase.reset(
        new Database(
            pCache,
            configMap,
            DeviceMode::createNew,
            this));
    BOOST_CHECK(!pDatabase->isRecoveryRequired());
}

void DatabaseTest::testCreateData()
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
    executeCheckpointedTxn(25,70,true,checkpointType);
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
    executeCheckpointedTxn(25,70,false);
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase.reset();
    loadDatabase();
    BOOST_CHECK(pDatabase->isRecoveryRequired());
    pDatabase->recover(*this);
    verifyData(15);
}

void DatabaseTest::loadDatabase()
{
    pDatabase.reset(
        new Database(
            pCache,
            configMap,
            DeviceMode::load,
            this));
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
    assert(actionType == ACTION_INCREMENT);
    int i;
    logStream.readValue(i);
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(pageId);
    pageLock.getNodeForWrite().x -= i;
}

void DatabaseTest::redoLogicalAction(
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
    ByteOutputStream &logStream =
        getLogicalTxn()->beginLogicalAction(*this,ACTION_INCREMENT);
    logStream.writeValue(i);
    getLogicalTxn()->endLogicalAction();
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(pageId);
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

void DatabaseTest::verifyData(uint x)
{
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockShared(pageId);
    BOOST_CHECK_EQUAL(pageLock.getNodeForRead().x,x);
}

FENNEL_UNIT_TEST_SUITE(DatabaseTest);

// End DatabaseTest.cpp

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
#include "fennel/test/SegStorageTestBase.h"
#include "fennel/test/ThreadedTestBase.h"
#include "fennel/btree/BTreeWriter.h"
#include "fennel/btree/BTreeBuilder.h"
#include "fennel/btree/BTreeVerifier.h"
#include "fennel/btree/BTreeRecoveryFactory.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/CacheParams.h"
#include "fennel/synch/SXMutex.h"

#include <boost/test/test_tools.hpp>
#include <boost/scoped_ptr.hpp>

#include <functional>

using namespace fennel;

// TODO:  factor out BTreeTestBase

class BTreeTxnTest
    : virtual public ThreadedTestBase
{
    struct TestThreadData 
    {
        std::subtractive_rng randomNumberGenerator;
        SharedBTreeReader pReader;
        SharedBTreeWriter pWriter;
        TupleData keyData;
    };

    /**
     * The various operations that can be run in the multi-threaded test.
     */
    enum OpType {
        OP_INSERT,
        OP_DELETE,
        OP_SCAN,
        OP_CHECKPOINT,
        OP_MAX
    };
    
    // NOTE:  this matches the Fennel Tuple format, so it can be used
    // directly for input into BTreeWriter
    struct Record 
    {
        int32_t key;
        int32_t value;
    };
    SharedCache pCache;
    SharedDatabase pDatabase;

    BTreeDescriptor treeDescriptor;
    
    boost::thread_specific_ptr<TestThreadData> pTestThreadData;
    PageId rootPageId;

    bool testRollback;
    uint iKeyMax;
    uint nInsertsPerTxn;
    uint nDeletesPerTxn;
    uint nKeysPerScan;

    uint nSecondsBetweenCheckpoints;
    
    void testTxns();

    void insertTxn();
    void deleteTxn();
    void scanTxn();
    void testCheckpoint();

    void endTxn(SharedLogicalTxn pTxn);
    
    uint generateRandomNumber(uint iMax);

    void createTree();
    RecordNum verifyTree();
    BTreeReader &getReader();
    BTreeWriter &getWriter();
    TupleData &getKeyData();
    void bindKey(int32_t &key);
    
public:
    explicit BTreeTxnTest();

    virtual void testCaseSetUp();
    virtual void testCaseTearDown();
    
    virtual void threadInit();
    virtual void threadTerminate();
    virtual bool testThreadedOp(int iOp);
};

BTreeTxnTest::BTreeTxnTest()
{
    nInsertsPerTxn = configMap.getIntParam("insertsPerTxn",5);
    nDeletesPerTxn = configMap.getIntParam("deletesPerTxn",5);
    nKeysPerScan = configMap.getIntParam("keysPerScan",5);
    iKeyMax = configMap.getIntParam("maxKey",1000000);
    nSecondsBetweenCheckpoints = configMap.getIntParam("checkpointInterval",20);
    testRollback = configMap.getIntParam(
        "testRollback",1);

    threadCounts.resize(OP_MAX,-1);

    threadCounts[OP_INSERT] = configMap.getIntParam(
        "insertThreads",-1);
    threadCounts[OP_DELETE] = configMap.getIntParam(
        "deleteThreads",-1);
    threadCounts[OP_SCAN] = configMap.getIntParam(
        "scanThreads",-1);
        
    if (nSecondsBetweenCheckpoints < nSeconds) {
        threadCounts[OP_CHECKPOINT] = 1;
    } else {
        threadCounts[OP_CHECKPOINT] = 0;
    }

    FENNEL_UNIT_TEST_CASE(BTreeTxnTest,testTxns);
}

void BTreeTxnTest::testCaseSetUp()
{
    // TODO:  cleanup
    
    configMap.setStringParam(
        Database::paramDatabaseDir,".");
    configMap.setStringParam(
        "databaseInitSize","1000");
    configMap.setStringParam(
        "tempInitSize","1000");
    configMap.setStringParam(
        "databaseShadowLogInitSize","2000");
    if (!configMap.isParamSet("databaseTxnLogInitSize")) {
        configMap.setStringParam(
            "databaseTxnLogInitSize","2000");
    }

    CacheParams cacheParams;
    cacheParams.readConfig(configMap);
    pCache = Cache::newCache(cacheParams);
    pDatabase.reset(
        new Database(
            pCache,
            configMap,
            DeviceMode::createNew,
            this));
    
    statsTimer.addSource(pDatabase);
    statsTimer.start();
    
    rootPageId = NULL_PAGE_ID;
    createTree();
    pDatabase->checkpointImpl();
}

void BTreeTxnTest::testCaseTearDown()
{
    statsTimer.stop();
    
    pDatabase.reset();
    pCache.reset();
}

void BTreeTxnTest::createTree()
{
    TupleDescriptor &tupleDesc = treeDescriptor.tupleDescriptor;
    StandardTypeDescriptorFactory stdTypeFactory;
    TupleAttributeDescriptor attrDesc(
        stdTypeFactory.newDataType(STANDARD_TYPE_INT_32));
    tupleDesc.push_back(attrDesc);
    tupleDesc.push_back(attrDesc);
    TupleProjection &keyProj = treeDescriptor.keyProjection;
    keyProj.push_back(0);

    treeDescriptor.segmentAccessor.pSegment = pDatabase->getDataSegment();
    treeDescriptor.segmentAccessor.pCacheAccessor = pCache;
    treeDescriptor.rootPageId = rootPageId;

    BTreeBuilder builder(treeDescriptor,pDatabase->getDataSegment());
    builder.createEmptyRoot();
    treeDescriptor.rootPageId = builder.getRootPageId();
}

void BTreeTxnTest::threadInit()
{
    ThreadedTestBase::threadInit();
    pTestThreadData.reset(new TestThreadData());
    pTestThreadData->pReader.reset(new BTreeReader(treeDescriptor));
    pTestThreadData->keyData.compute(getReader().getKeyDescriptor());

    SegmentAccessor scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pCache,
            1);
    
    pTestThreadData->pWriter.reset(
        new BTreeWriter(treeDescriptor,scratchAccessor));
}

void BTreeTxnTest::threadTerminate()
{
    // NOTE:  see corresponding code in PagingTestBase for why this is
    // commented out.
    
    // pTestThreadData.reset();
    ThreadedTestBase::threadTerminate();
}
    
uint BTreeTxnTest::generateRandomNumber(uint iMax)
{
    return pTestThreadData->randomNumberGenerator(iMax);
}

BTreeReader &BTreeTxnTest::getReader()
{
    return *(pTestThreadData->pReader);
}

TupleData &BTreeTxnTest::getKeyData()
{
    return pTestThreadData->keyData;
}

BTreeWriter &BTreeTxnTest::getWriter()
{
    return *(pTestThreadData->pWriter);
}

// NOTE: eventually this will fail due to non-serializability (if an
// insert/delete sequence affecting the same key is reordered during recovery).
// Need to add locking support.

void BTreeTxnTest::testTxns()
{
    runThreadedTestCase();
    RecordNum nEntries = verifyTree();
    pDatabase->checkpointImpl(CHECKPOINT_DISCARD);

    statsTimer.stop();
    pDatabase.reset();
    pDatabase.reset(
        new Database(
            pCache,
            configMap,
            DeviceMode::load,
            this));
    BOOST_CHECK(pDatabase->isRecoveryRequired());

    statsTimer.addSource(pDatabase);
    statsTimer.start();
    
    StandardTypeDescriptorFactory typeFactory;
    SegmentAccessor scratchAccessor =
        pDatabase->getSegmentFactory()->newScratchSegment(
            pCache,
            1);
    SegmentAccessor segmentAccessor(pDatabase->getDataSegment(),pCache);
    BTreeRecoveryFactory btreeRecoveryFactory(
        segmentAccessor,
        scratchAccessor,
        typeFactory);
    segmentAccessor.reset();
    scratchAccessor.reset();
    pDatabase->recover(btreeRecoveryFactory);
    treeDescriptor.segmentAccessor.pSegment = pDatabase->getDataSegment();
    RecordNum nEntriesRecovered = verifyTree();

    // FIXME jvs 8-Mar-2004:  Turn this back on once NOTE above is taken
    // care of.  Tautological checks are just to shut warnings up.
    
    // BOOST_CHECK_EQUAL(nEntries,nEntriesRecovered);
    BOOST_CHECK_EQUAL(nEntries,nEntries);
    BOOST_CHECK_EQUAL(nEntriesRecovered,nEntriesRecovered);
}

void BTreeTxnTest::insertTxn()
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    pTxn->addParticipant(pTestThreadData->pWriter);
    BTreeWriter &writer = getWriter();
    for (uint i = 0; i < nInsertsPerTxn; ++i) {
        Record record;
        record.key = generateRandomNumber(iKeyMax);
        record.value = generateRandomNumber(iKeyMax);
        // TODO:  test with and without duplicates
        writer.insertTupleFromBuffer(
            reinterpret_cast<PConstBuffer>(&record),DUP_DISCARD);
    }
    endTxn(pTxn);
}

void BTreeTxnTest::deleteTxn()
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    pTxn->addParticipant(pTestThreadData->pWriter);
    BTreeWriter &writer = getWriter();
    for (uint i = 0; i < nDeletesPerTxn; ++i) {
        int32_t key = generateRandomNumber(iKeyMax);
        bindKey(key);
        if (writer.searchForKey(getKeyData(),DUP_SEEK_ANY)) {
            writer.deleteCurrent();
        }
        writer.endSearch();
    }
    endTxn(pTxn);
}

void BTreeTxnTest::scanTxn()
{
    BTreeReader &reader = getReader();
    int32_t key = generateRandomNumber(iKeyMax);
    bindKey(key);
    if (reader.searchForKey(getKeyData(),DUP_SEEK_ANY)) {
        for (uint i = 0; i < nKeysPerScan; ++i) {
            if (!reader.searchNext()) {
                break;
            }
        }
    }
    reader.endSearch();
}
    
void BTreeTxnTest::endTxn(SharedLogicalTxn pTxn)
{
    if (testRollback) {
        if (generateRandomNumber(2)) {
            pTxn->commit();
        } else {
            pTxn->rollback();
        }
    } else {
        pTxn->commit();
    }
}

bool BTreeTxnTest::testThreadedOp(int iOp)
{
    SXMutexSharedGuard checkpointSharedGuard(
        pDatabase->getCheckpointThread()->getActionMutex(),false);
    assert(iOp < OP_MAX);
    OpType op = static_cast<OpType>(iOp);
    switch(op) {
    case OP_INSERT:
        checkpointSharedGuard.lock();
        insertTxn();
        break;
    case OP_DELETE:
        checkpointSharedGuard.lock();
        deleteTxn();
        break;
    case OP_SCAN:
        scanTxn();
        break;
    case OP_CHECKPOINT:
        testCheckpoint();
        break;
    default:
        assert(false);
        return false;
    }
    return true;
}

void BTreeTxnTest::bindKey(int32_t &key)
{
    getKeyData()[0].pData = reinterpret_cast<PConstBuffer>(&key);
}

void BTreeTxnTest::testCheckpoint()
{
    snooze(nSecondsBetweenCheckpoints);
    CheckpointType checkpointType;
    if (configMap.getIntParam("fuzzyCheckpoint",1)) {
        checkpointType = CHECKPOINT_FLUSH_FUZZY;
    } else {
        checkpointType = CHECKPOINT_FLUSH_ALL;
    }
    pDatabase->requestCheckpoint(checkpointType,false);
}

RecordNum BTreeTxnTest::verifyTree()
{
    BTreeVerifier verifier(treeDescriptor);
    verifier.verify(false);

    BTreeStatistics const &stats = verifier.getStatistics();
    BOOST_MESSAGE("height = " << stats.nLevels);
    BOOST_MESSAGE("record count = " << stats.nTuples);
    BOOST_MESSAGE("leaf nodes = " << stats.nLeafNodes);
    BOOST_MESSAGE("nonleaf nodes = " << stats.nNonLeafNodes);
    return stats.nTuples;
}

FENNEL_UNIT_TEST_SUITE(BTreeTxnTest);

// End BTreeTxnTest.cpp

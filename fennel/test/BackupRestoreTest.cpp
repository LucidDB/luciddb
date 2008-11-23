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
#include "fennel/common/FileSystem.h"
#include "fennel/test/SnapshotSegmentTestBase.h"
#include "fennel/cache/PagePredicate.h"
#include "fennel/txn/LogicalTxn.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/segment/SnapshotRandomAllocationSegment.h"
#include "fennel/segment/SegPageBackupRestoreDevice.h"
#include "fennel/db/Database.h"

#include <boost/test/test_tools.hpp>

using namespace fennel;

/**
 * Unit test for backup and restore of database header pages and a 
 * VersionedRandomAllocationSegment.
 */
class BackupRestoreTest : virtual public SnapshotSegmentTestBase
{
    struct TestNode : public StoredNode
    {
        static const MagicNumber MAGIC_NUMBER = 0xa496c71bff0d41bdLL;

        uint x;
    };

    typedef SegNodeLock<TestNode> TestPageLock;
    
    SharedDatabase pDatabase;
    PageId persistentPageId;

    void createSnapshotData();
    void executeSnapshotTxn(int i);
    void verifySnapshotData(uint x);

    void testBackupRestore(bool isCompressed);
    void backup(
        std::string backupFileName,
        TxnId lowerBoundCsn,
        TxnId upperBoundCsn,
        bool isCompressed);
    void restore(
        std::string backupFileName,
        TxnId lowerBoundCsn,
        TxnId upperBoundCsn,
        bool isCompressed);
    std::string getCompressionProgram(bool isCompressed);
    void verifyData();

public:
    explicit BackupRestoreTest()
    {
        FENNEL_UNIT_TEST_CASE(BackupRestoreTest, testHeaderBackupRestore);
        FENNEL_UNIT_TEST_CASE(BackupRestoreTest, testBackupCleanup);
        FENNEL_UNIT_TEST_CASE(BackupRestoreTest, testBackupRestoreUncompressed);
        FENNEL_UNIT_TEST_CASE(BackupRestoreTest, testBackupRestoreCompressed);
    }

    /**
     * Tests backup and restore of the database header pages.
     */
    void testHeaderBackupRestore();

    /**
     * Tests that backup properly cleans up after either an error or an abort.
     */
    void testBackupCleanup();

    /**
     * Tests backup and restore of data pages, without compression.
     */
    void testBackupRestoreUncompressed();

    /**
     * Tests backup and restore of data pages, with compression.
     */
    void testBackupRestoreCompressed();
};
  
void BackupRestoreTest::testBackupRestoreUncompressed()
{
    testBackupRestore(false);
}

void BackupRestoreTest::testBackupRestoreCompressed()
{
    testBackupRestore(true);
}

void BackupRestoreTest::createSnapshotData()
{
    // Create a database with a single data page with an initial value of 0.
    pDatabase = Database::newDatabase(
        pCache,
        configMap,
        DeviceMode::createNew,
        shared_from_this());

    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    SharedSegment pSegment = 
        pDatabase->getSegmentFactory()->newSnapshotRandomAllocationSegment(
            pDatabase->getDataSegment(),
            pDatabase->getDataSegment(),
            pTxn->getTxnId());
    SnapshotRandomAllocationSegment *pSnapshotSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pSegment);
    SegmentAccessor segmentAccessor(pSegment,pCache);

    TestPageLock pageLock(segmentAccessor);
    persistentPageId = pageLock.allocatePage();
    pageLock.getNodeForWrite().x = 0;
    pageLock.unlock();
    pTxn->commit();
    pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    pSnapshotSegment->commitChanges(pTxn->getTxnId());
    pSnapshotSegment->checkpoint(CHECKPOINT_FLUSH_ALL);
    pTxn->commit();
    pDatabase->checkpointImpl();

    // Update the value to 5.
    executeSnapshotTxn(5);
}

void BackupRestoreTest::testHeaderBackupRestore()
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
    configMap.setStringParam(
        "forceTxns","true");
    configMap.setStringParam(
        "disableSnapshots","false");

    CacheParams cacheParams;
    cacheParams.readConfig(configMap);
    pCache = Cache::newCache(cacheParams);

    // Create a single data page in the database with an initial value,
    // and then update the value to 5.  Doing the update will version
    // the data page.
    createSnapshotData();
    pDatabase->checkpointImpl();
    verifySnapshotData(5);

    // Backup the data with value 5.
    std::string fullBackup = "fullBackup.dat";
    FileSize dataDeviceSize;
    bool aborted = false;
    TxnId fullTxnId =
        pDatabase->initiateBackup(
            fullBackup,
            false, 
            0, 
            NULL_TXN_ID, 
            "",
            dataDeviceSize,
            aborted);
    pDatabase->completeBackup(NULL_TXN_ID, fullTxnId, aborted);

    // Update the value to 15.
    executeSnapshotTxn(10);
    verifySnapshotData(15);

    // Do an incremental backup of value 15.
    std::string incrBackup1 = "incrBackup1.dat";
    TxnId incrTxnId1 =
        pDatabase->initiateBackup(
            incrBackup1, 
            true, 
            0, 
            fullTxnId,
            "",
            dataDeviceSize,
            aborted);

    // Make sure ALTER SYSTEM DEALLOCATE OLD is disabled
    uint nPagesBefore =
        pDatabase->getDataSegment()->getAllocatedSizeInPages();
    pDatabase->deallocateOldPages(incrTxnId1);
    uint nPagesAfter = 
        pDatabase->getDataSegment()->getAllocatedSizeInPages();
    BOOST_REQUIRE(nPagesBefore == nPagesAfter);

    pDatabase->completeBackup(fullTxnId, incrTxnId1, aborted);

    // Make sure ALTER SYSTEM DEALLOCATE OLD is reenabled
    pDatabase->deallocateOldPages(incrTxnId1);
    nPagesAfter = pDatabase->getDataSegment()->getAllocatedSizeInPages();
    BOOST_REQUIRE(nPagesBefore > nPagesAfter);

    // Update the value to 35.
    executeSnapshotTxn(20);
    verifySnapshotData(35);

    // Do a second incremental backup with value 35.
    std::string incrBackup2 = "incrBackup2.dat";
    TxnId incrTxnId2 =
        pDatabase->initiateBackup(
            incrBackup2, 
            true, 
            4096, 
            incrTxnId1,
            "",
            dataDeviceSize,
            aborted);
    pDatabase->completeBackup(incrTxnId1, incrTxnId2, aborted);

    // Restore the full backup.  The value should be 5.
    pDatabase->restoreFromBackup(
        fullBackup,
        1002 * pCache->getPageSize(),
        "",
        NULL_TXN_ID,
        fullTxnId,
        aborted);
    verifySnapshotData(5);

    // Restore the first incremental backup.  The value should be 15.
    pDatabase->restoreFromBackup(
        incrBackup1,
        1002 * pCache->getPageSize(),
        "",
        fullTxnId,
        incrTxnId1,
        aborted);
    verifySnapshotData(15);

    // Restore the second incremental backup.  The value should be 35.
    pDatabase->restoreFromBackup(
        incrBackup2,
        1002 * pCache->getPageSize(),
        "",
        incrTxnId1,
        incrTxnId2,
        aborted);
    verifySnapshotData(35);

    pDatabase.reset();
}

void BackupRestoreTest::testBackupCleanup()
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
    configMap.setStringParam(
        "forceTxns","true");
    configMap.setStringParam(
        "disableSnapshots","false");

    CacheParams cacheParams;
    cacheParams.readConfig(configMap);
    pCache = Cache::newCache(cacheParams);

    // Create a single data page in the database with an initial value,
    // and then update the value to 5.  Doing the update will version
    // the data page.
    createSnapshotData();
    pDatabase->checkpointImpl();
    verifySnapshotData(5);

    // Update the value to 15.
    executeSnapshotTxn(10);
    verifySnapshotData(15);

    std::string fullBackup = "fullBackup.dat";

    // Set the space padding to the amount of space currently available 
    // multipled by 1000.  This should result in a failure unless some other
    // user is using this filesystem and either:
    // A) that user is freeing up a large amount of space, or 
    // B) that user is freeing up space and the filesystem is short on space,
    //    in which case, multiplying by 1000 doesn't yield a large enough value
    //    to offset the amount that the user has freed
    FileSystem::remove(fullBackup.c_str());
    FileSize spaceAvailable;
    FileSystem::getDiskFreeSpace(".", spaceAvailable);
    FileSize dataDeviceSize;
    bool aborted = false;
    try {
        pDatabase->initiateBackup(
            fullBackup,
            true,
            spaceAvailable * 1000, 
            NULL_TXN_ID, 
            "",
            dataDeviceSize,
            aborted);
        BOOST_FAIL("Out of space exception not returned");
    } catch (FennelExcn &ex) {
        std::string errMsg = ex.getMessage();
        if (errMsg.find("Insufficient space") != 0) {
            BOOST_FAIL("Wrong exception returned");
        }
    } 

    // Make sure ALTER SYSTEM DEALLOCATE OLD is enabled, even after the
    // exception
    uint nPagesBefore =
        pDatabase->getDataSegment()->getAllocatedSizeInPages();
    pDatabase->deallocateOldPages(pDatabase->getLastCommittedTxnId());
    uint nPagesAfter = 
        pDatabase->getDataSegment()->getAllocatedSizeInPages();
    BOOST_REQUIRE(nPagesBefore > nPagesAfter);

    // Do another update so a new page is allocated
    executeSnapshotTxn(20);
    verifySnapshotData(35);

    // Initiate a new backup and then abort it.
    pDatabase->initiateBackup(
        fullBackup,
        false, 
        0,
        NULL_TXN_ID, 
        getCompressionProgram(true),
        dataDeviceSize,
        aborted);
    pDatabase->abortBackup();

    // Make sure ALTER SYSTEM DEALLOCATE OLD is enabled, even after the
    // backup was aborted
    nPagesBefore =
        pDatabase->getDataSegment()->getAllocatedSizeInPages();
    pDatabase->deallocateOldPages(pDatabase->getLastCommittedTxnId());
    nPagesAfter = 
        pDatabase->getDataSegment()->getAllocatedSizeInPages();
    BOOST_REQUIRE(nPagesBefore > nPagesAfter);

    // Abort a backup that was never initiated; should be a no-op
    pDatabase->abortBackup();

    pDatabase.reset();
}

void BackupRestoreTest::executeSnapshotTxn(int i)
{
    SharedLogicalTxn pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    SharedSegment pSegment = 
        pDatabase->getSegmentFactory()->newSnapshotRandomAllocationSegment(
            pDatabase->getDataSegment(),
            pDatabase->getDataSegment(),
            pTxn->getTxnId());
    SnapshotRandomAllocationSegment *pSnapshotSegment =
        SegmentFactory::dynamicCast<SnapshotRandomAllocationSegment *>(
            pSegment);

    // Update the value, which will version the page.
    SegmentAccessor segmentAccessor(pSegment, pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockExclusive(persistentPageId);
    pageLock.getNodeForWrite().x += i;
    pageLock.unlock();
    pTxn->commit();

    // Commit the changes through the snapshot segment.
    pTxn = pDatabase->getTxnLog()->newLogicalTxn(pCache);
    pSnapshotSegment->commitChanges(pTxn->getTxnId());
    pSnapshotSegment->checkpoint(CHECKPOINT_FLUSH_ALL);
    pTxn->commit();
    pDatabase->checkpointImpl();
}

void BackupRestoreTest::verifySnapshotData(uint x)
{
    // Lock the original page, but because we're accessing the segment
    // through a snapshot segment with the csn set to that of the last
    // committed transaction, we'll pick up the latest version of the page.
    SharedSegment pSegment = 
        pDatabase->getSegmentFactory()->newSnapshotRandomAllocationSegment(
            pDatabase->getDataSegment(),
            pDatabase->getDataSegment(),
            pDatabase->getLastCommittedTxnId());
    SegmentAccessor segmentAccessor(pSegment,pCache);
    TestPageLock pageLock(segmentAccessor);
    pageLock.lockShared(persistentPageId);
    BOOST_CHECK_EQUAL(pageLock.getNodeForRead().x,x);
}

void BackupRestoreTest::testBackupRestore(bool isCompressed)
{
    // Create and initialize a VersionedRandomAllocationSegment using
    // a snapshot with TxnId(0)
    currCsn = TxnId(0);
    openStorage(DeviceMode::createNew);
    testAllocateAll();
    closeStorage();

    // Update every 5 pages
    currCsn = TxnId(5);
    updatedCsns.push_back(currCsn);
    openStorage(DeviceMode::load);
    testSkipWrite(5);
    closeStorage();

    // Do a backup of all pages as of TxnId 5
    std::string backup5 = "backupTxnId5.dat";
    backup(backup5, NULL_TXN_ID, TxnId(5), isCompressed);

    // Update every 7 pages
    currCsn = TxnId(7);
    updatedCsns.push_back(currCsn);
    openStorage(DeviceMode::load);
    testSkipWrite(7);
    closeStorage();

    // Do another backup of all pages between TxnId's 5 and 7
    std::string backup5to7 = "backupTxnId5to7.dat";
    backup(backup5to7, TxnId(5), TxnId(7), isCompressed);

    // Update every 11 pages
    currCsn = TxnId(11);
    updatedCsns.push_back(currCsn);
    openStorage(DeviceMode::load);
    testSkipWrite(11);
    closeStorage();

    // Do a backup of all pages between TxnId's 7 and 11
    std::string backup7to11 = "backupTxnId7to11.dat";
    backup(backup7to11, TxnId(7), TxnId(11), isCompressed);

    // Do a backup of all pages between TxnId's 5 and 11
    std::string backup5to11 = "backupTxnId5to11.dat";
    backup(backup5to11, TxnId(5), TxnId(11), isCompressed);

    // Restore the backup as of TxnId 5
    restore(backup5, NULL_TXN_ID, TxnId(5), isCompressed);

    // Make sure the data only reflects up to TxnId 5.  Leave the currCsn
    // set to the larger value, but have updatedCsn's reflect only the
    // updates up to TxnId 5.
    updatedCsns.clear();
    updatedCsns.push_back(TxnId(5));
    verifyData();

    // Restore the backup as of TxnId 7 and verify that it only includes
    // updates up to TxnId 7
    restore(backup5to7, TxnId(5), TxnId(7), isCompressed);
    updatedCsns.push_back(TxnId(7));
    verifyData();

    // Restore the backup as of TxnId 11 and verify that it includes all
    // updates
    restore(backup7to11, TxnId(7), TxnId(11), isCompressed);
    updatedCsns.push_back(TxnId(11));
    verifyData();

    // Go back and do restores of the backup at TxnId5 followed by the
    // backup of TxnId's 5 through 11.
    restore(backup5, NULL_TXN_ID, TxnId(5), isCompressed);
    updatedCsns.clear();
    updatedCsns.push_back(TxnId(5));
    verifyData();
    restore(backup5to11, TxnId(5), TxnId(11), isCompressed);
    updatedCsns.push_back(TxnId(7));
    updatedCsns.push_back(TxnId(11));
    verifyData();
}

void BackupRestoreTest::backup(
    std::string backupFileName,
    TxnId lowerBoundCsn,
    TxnId upperBoundCsn,
    bool isCompressed)
{
    openStorage(DeviceMode::load);
    VersionedRandomAllocationSegment *pVRSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            pVersionedRandomSegment);
    SegmentAccessor scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 12);

    if (isCompressed) {
        backupFileName.append(".gz");
    }
    SharedSegPageBackupRestoreDevice pBackupDevice =
        SegPageBackupRestoreDevice::newSegPageBackupRestoreDevice(
            backupFileName,
            "w",
            getCompressionProgram(isCompressed),
            10,
            2,
            scratchAccessor,
            pCache->getDeviceAccessScheduler(*pRandomAccessDevice),
            pRandomAccessDevice);
    bool abortFlag = false;
    pVRSegment->backupAllocationNodes(
        pBackupDevice,
        false,
        lowerBoundCsn,
        upperBoundCsn,
        abortFlag);
    pVRSegment->backupDataPages(
        pBackupDevice,
        lowerBoundCsn,
        upperBoundCsn,
        abortFlag);
    pBackupDevice.reset();

    scratchAccessor.reset();
    closeStorage();
}

void BackupRestoreTest::restore(
    std::string backupFileName,
    TxnId lowerBoundCsn,
    TxnId upperBoundCsn,
    bool isCompressed)
{
    openStorage(DeviceMode::load);
    VersionedRandomAllocationSegment *pVRSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            pVersionedRandomSegment);
    SegmentAccessor scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache, 10);

    // Flush and unmap any pages currently in the cache so the restore
    // avoids reading out-of-date pages
    MappedPageListenerPredicate pagePredicate(*pVRSegment);
    pCache->checkpointPages(pagePredicate, CHECKPOINT_FLUSH_AND_UNMAP);

    if (isCompressed) {
        backupFileName.append(".gz");
    }
    SharedSegPageBackupRestoreDevice pBackupDevice =
        SegPageBackupRestoreDevice::newSegPageBackupRestoreDevice(
            backupFileName,
            "r",
            getCompressionProgram(isCompressed),
            10,
            0,
            scratchAccessor,
            pCache->getDeviceAccessScheduler(*pRandomAccessDevice),
            pRandomAccessDevice);
    bool abortFlag = false;
    pVRSegment->restoreFromBackup(
        pBackupDevice, 
        lowerBoundCsn, 
        upperBoundCsn,
        abortFlag);
    pBackupDevice.reset();

    scratchAccessor.reset();
    closeStorage();
}

std::string BackupRestoreTest::getCompressionProgram(bool isCompressed)
{
    if (!isCompressed) {
        return "";
    } else {
        return "gzip";
    }
}

void BackupRestoreTest::verifyData()
{
    openStorage(DeviceMode::load);
    testSequentialRead();
    closeStorage();
}

FENNEL_UNIT_TEST_SUITE(BackupRestoreTest);

// End BackupRestoreTest.cpp

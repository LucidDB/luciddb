/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2003 SQLstream, Inc.
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
#include "fennel/common/AbortExcn.h"
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/db/DataFormatExcn.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FileSystem.h"
#include "fennel/common/FennelResource.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/cache/Cache.h"
#include "fennel/cache/PagePredicate.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/segment/LinearDeviceSegment.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/VersionedSegment.h"
#include "fennel/segment/VersionedRandomAllocationSegment.h"
#include "fennel/common/CompoundId.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalRecoveryLog.h"
#include "fennel/common/StatsTarget.h"
#include "fennel/common/FennelResource.h"

#include <boost/filesystem/operations.hpp>

#ifdef __MSVC__
#include <process.h>
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace boost::filesystem;

ParamName Database::paramDatabaseDir = "databaseDir";
ParamName Database::paramResourceDir = "resourceDir";
ParamName Database::paramForceTxns = "forceTxns";
ParamName Database::paramDisableSnapshots = "disableSnapshots";
ParamName Database::paramDatabasePrefix = "database";
ParamName Database::paramTempPrefix = "temp";
ParamName Database::paramShadowLogPrefix = "databaseShadowLog";
ParamName Database::paramTxnLogPrefix = "databaseTxnLog";
ParamName Database::paramInitSizeSuffix = "InitSize";
ParamName Database::paramMaxSizeSuffix = "MaxSize";
ParamName Database::paramIncSizeSuffix = "IncrementSize";

ParamVal Database::valLogAllocLinear = "linear";
ParamVal Database::valLogAllocCircular = "circular";

const SegmentId Database::DEFAULT_DATA_SEGMENT_ID = SegmentId(1);
const SegmentId Database::TEMP_SEGMENT_ID = SegmentId(2);

// NOTE:  correct sequence is critical in most Database operations

SharedDatabase Database::newDatabase(
    SharedCache pCacheInit,
    ConfigMap const &configMapInit,
    DeviceMode openModeInit,
    SharedTraceTarget pTraceTarget,
    SharedPseudoUuidGenerator pUuidGenerator)
{
    if (!pUuidGenerator) {
        pUuidGenerator.reset(new PseudoUuidGenerator());
    }
    SharedDatabase pDb =
        SharedDatabase(
            new Database(
                pCacheInit, configMapInit, openModeInit, pTraceTarget,
                pUuidGenerator),
            ClosableObjectDestructor());
    pDb->init();
    return pDb;
}

Database::Database(
    SharedCache pCacheInit,
    ConfigMap const &configMapInit,
    DeviceMode openModeInit,
    SharedTraceTarget pTraceTarget,
    SharedPseudoUuidGenerator pUuidGeneratorInit)
    : TraceSource(pTraceTarget, "database"),
      pCache(pCacheInit),
      configMap(configMapInit),
      pUuidGenerator(pUuidGeneratorInit)
{
    openMode = openModeInit;
    disableDeallocateOld = false;
}

void Database::init()
{
    forceTxns = configMap.getBoolParam(paramForceTxns);
    disableSnapshots = configMap.getBoolParam(paramDisableSnapshots);

    // NOTE:  do this early in case other initialization throws exceptions
    // (and to prevent thread-safety issues later on)
    std::string resourceDir = configMap.getStringParam(paramResourceDir);
    if (resourceDir == "") {
        // If we weren't told explicitly where to find resources, make sure
        // we can rely on an environment variable setting.  TODO:
        // instead of pre-checking this, get information back from
        // FennelResource to tell us whether it was successfully initialized.
        assert(getenv("FENNEL_HOME"));
    } else {
        FennelResource::setResourceFileLocation(resourceDir);
    }
    FennelResource::instance();

    dataDeviceId = DeviceId(1);
    shadowDeviceId = DeviceId(2);
    txnLogDeviceId = DeviceId(3);
    tempDeviceId = DeviceId(4);

    headerPageId1 = Segment::getLinearPageId(0);
    headerPageId2 = Segment::getLinearPageId(1);

    header.versionNumber = SegVersionNum(0);
    recoveryRequired = false;

    // REVIEW:  Have to do this so that later assignments from header to stored
    // data work correctly.  But it breaks encapsulation.  Find a better way.
    header.magicNumber = DatabaseHeader::MAGIC_NUMBER;

    // TODO:  use boost filesystem library for platform-independent path
    // manipulation
    std::string databaseDir = configMap.getStringParam(paramDatabaseDir);
    // TODO:  real excn
    assert(databaseDir != "");

    dataDeviceName = databaseDir + "/db.dat";
    shadowDeviceName = databaseDir + "/shadowlog.dat";
    txnLogDeviceName = databaseDir + "/txnlog.dat";
    tempDeviceName = databaseDir + "/temp.dat";

    nCheckpoints = nCheckpointsStat = 0;

    pSegmentFactory = SegmentFactory::newSegmentFactory(
        configMap, getSharedTraceTarget());

    if (!openMode.create) {
        // TODO:  real excn
        assert(FileSystem::doesFileExist(dataDeviceName.c_str()));
        if (FileSystem::doesFileExist(shadowDeviceName.c_str())) {
            prepareForRecovery();
            return;
        }
    }

    openSegments();
}

void Database::prepareForRecovery()
{
    FENNEL_TRACE(TRACE_WARNING, "recovery required");
    recoveryRequired = true;
    createTempSegment();
    LinearDeviceSegmentParams dataDeviceParams;
    createDataDevice(dataDeviceParams);
    loadHeader(true);
    writeHeader();
    SharedSegment pShadowLogSegment = createShadowLog(openMode);
    createDataSegment(pShadowLogSegment, dataDeviceParams);
}

void Database::openSegments()
{
#ifdef NDEBUG
    FENNEL_TRACE(TRACE_INFO, "Fennel build:  --with-optimization");
#else
    FENNEL_TRACE(TRACE_INFO, "Fennel build:  --without-optimization");
#endif
    FENNEL_TRACE(TRACE_INFO, "opening database; process ID = " << getpid());

    pCheckpointThread = SharedCheckpointThread(
        new CheckpointThread(*this),
        ClosableObjectDestructor());

    createTempSegment();

    pUuidGenerator->generateUuid(header.onlineUuid);
    FENNEL_TRACE(TRACE_INFO, "online UUID = " << header.onlineUuid);

    DeviceMode txnLogMode = openMode;
    txnLogMode.create = true;
    txnLogMode.direct = true;
    createTxnLog(txnLogMode);

    LinearDeviceSegmentParams dataDeviceParams;
    createDataDevice(dataDeviceParams);

    if (openMode.create) {
        // online UUID will be written out by allocateHeader
        allocateHeader();
    } else {
        PseudoUuid newOnlineUuid = header.onlineUuid;
        loadHeader(false);
        // overwrite old online UUID; new one will be written out
        // by checkpoint below
        header.onlineUuid = newOnlineUuid;
    }

    DeviceMode shadowMode = openMode;
    shadowMode.create = true;
    shadowMode.direct = true;

    SharedSegment pShadowLogSegment = createShadowLog(shadowMode);
    createDataSegment(pShadowLogSegment, dataDeviceParams);

    FENNEL_TRACE(
        TRACE_INFO,
        "database opened; page version = "
        << header.versionNumber);

    if (!openMode.create) {
        checkpointImpl();
    }

    pCheckpointThread->start();
}

Database::~Database()
{
}

void Database::closeImpl()
{
    FENNEL_TRACE(
        TRACE_INFO,
        "closing database");
    if (pCheckpointThread) {
        pCheckpointThread->close();
    }

    // Free any leftover temp pages used for page versioning
    if (pDataSegment && areSnapshotsEnabled()) {
        VersionedRandomAllocationSegment *pVersionedRandomSegment =
            SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
                pDataSegment);
        pVersionedRandomSegment->freeTempPages();
    }

    // Verify that no garbage temp pages remain allocated.
    if (pTempSegment) {
        assert(pTempSegment->getAllocatedSizeInPages() == 0);
    }

    if (isRecoveryRequired()) {
        closeDevices();
    } else {
        // NOTE jvs 14-Nov-2006:  In case we're auto-closing after
        // a failed startup, skip checkpoint if we don't have
        // everything we need for it.
        if (pTxnLog && pDataSegment) {
            checkpointImpl();
        }
        closeDevices();
        deleteLogs();
    }
    FENNEL_TRACE(
        TRACE_INFO,
        "database closed; page version = "
        << header.versionNumber);
}

void Database::closeDevices()
{
    pVersionedSegment = NULL;
    pTxnLog.reset();
    // REVIEW: have to explicitly close in case someone (like a recovery
    // factory) else still has a segment reference; should probably find a
    // better way to deal with this
    if (pDataSegment) {
        pDataSegment->close();
        pDataSegment.reset();
    }
    pHeaderSegment.reset();
    if (pTempSegment) {
        pTempSegment->close();
        pTempSegment.reset();
    }

    // for incomplete recovery or startup, these devices may not have been
    // opened yet
    if (pCache) {
        if (pCache->getDevice(txnLogDeviceId)) {
            pCache->unregisterDevice(txnLogDeviceId);
        }
        if (pCache->getDevice(shadowDeviceId)) {
            pCache->unregisterDevice(shadowDeviceId);
        }
        if (pCache->getDevice(dataDeviceId)) {
            pCache->unregisterDevice(dataDeviceId);
        }
        if (pCache->getDevice(tempDeviceId)) {
            pCache->unregisterDevice(tempDeviceId);
        }
    }
}

void Database::deleteLogs()
{
    FileSystem::remove(shadowDeviceName.c_str());
    FileSystem::remove(txnLogDeviceName.c_str());

    // TODO jvs 25-June-2005:  here and in LogicalRecoveryLog, we should
    // be using ConfigMap to determine where to store txn logs
    // instead of current_path()
    directory_iterator end_itr;
    for (directory_iterator itr(current_path()); itr != end_itr; ++itr) {
        std::string filename = itr->path().filename();
        // TODO jvs 25-June-2005:  encapsulate filename parsing in
        // LogicalRecoveryLog
        if (filename.length() < 4) {
            continue;
        }
        if (filename.substr(0, 3) != "txn") {
            continue;
        }
        if (filename.substr(filename.length() - 4, 4) != ".dat") {
            continue;
        }
        FileSystem::remove(filename.c_str());
    }
}

SharedSegment Database::createTxnLogSegment(
    DeviceMode txnLogMode, PageId oldestPageId)
{
    SharedRandomAccessDevice pTxnLogDevice(
        new RandomAccessFileDevice(txnLogDeviceName, txnLogMode));
    pCache->registerDevice(txnLogDeviceId, pTxnLogDevice);

    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramTxnLogPrefix, txnLogMode, deviceParams);
    CompoundId::setDeviceId(deviceParams.firstBlockId, txnLogDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 0);
    if (forceTxns) {
        deviceParams.nPagesAllocated = 0;
    } else {
        deviceParams.nPagesAllocated = MAXU;
        deviceParams.nPagesIncrement = 0;
        deviceParams.nPagesMax = deviceParams.nPagesMin;
    }

    SharedSegment pLinearSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    SharedSegment pTxnLogSegment;
    if (forceTxns) {
        pTxnLogSegment = pLinearSegment;
    } else {
        pTxnLogSegment = pSegmentFactory->newCircularSegment(
            pLinearSegment, pCheckpointThread, oldestPageId);
    }

    return pTxnLogSegment;
}

void Database::createTxnLog(DeviceMode txnLogMode)
{
    SharedSegment pTxnLogSegment = createTxnLogSegment(
        txnLogMode, NULL_PAGE_ID);
    SegmentAccessor segmentAccessor(pTxnLogSegment, pCache);
    pTxnLog = LogicalTxnLog::newLogicalTxnLog(
        segmentAccessor,
        header.onlineUuid,
        pSegmentFactory);
    pTxnLog->checkpoint(header.txnLogCheckpointMemento);
}

SharedSegment Database::createShadowLog(DeviceMode shadowLogMode)
{
    SharedRandomAccessDevice pShadowDevice(
        new RandomAccessFileDevice(shadowDeviceName, shadowLogMode));
    pCache->registerDevice(shadowDeviceId, pShadowDevice);

    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramShadowLogPrefix, shadowLogMode, deviceParams);
    CompoundId::setDeviceId(deviceParams.firstBlockId, shadowDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 0);

    if (forceTxns) {
        if (shadowLogMode.create) {
            // start allocating from beginning of device
            deviceParams.nPagesAllocated = 0;
        } else {
            // treat entire device as pre-allocated because we're
            // going to scan it during recovery
            deviceParams.nPagesAllocated = MAXU;
        }
    } else {
        deviceParams.nPagesAllocated = MAXU;
        deviceParams.nPagesIncrement = 0;
        deviceParams.nPagesMax = deviceParams.nPagesMin;
    }

    SharedSegment pShadowSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    PageId oldestPageId = NULL_PAGE_ID;
    if (!shadowLogMode.create) {
        oldestPageId = header.shadowRecoveryPageId;
    }

    if (!forceTxns) {
        pShadowSegment = pSegmentFactory->newCircularSegment(
            pShadowSegment,
            pCheckpointThread,
            oldestPageId);
    }

    return pSegmentFactory->newWALSegment(pShadowSegment);
}

void Database::createDataDevice(LinearDeviceSegmentParams &deviceParams)
{
    readDeviceParams(paramDatabasePrefix, openMode, deviceParams);

    FileSize initialSize = FileSize(0);
    if (shouldForceTxns()) {
        // include +2 for the database header pages
        initialSize = (deviceParams.nPagesMin + 2) * pCache->getPageSize();
    }

    pDataDevice =
        SharedRandomAccessDevice(
            new RandomAccessFileDevice(
                dataDeviceName,
                openMode,
                initialSize));
    pCache->registerDevice(dataDeviceId, pDataDevice);
}

void Database::createDataSegment(
    SharedSegment pShadowLogSegment,
    LinearDeviceSegmentParams &deviceParams)
{
    // first data BlockId is located after the two database header pages
    CompoundId::setDeviceId(deviceParams.firstBlockId, dataDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 2);

    deviceParams.nPagesAllocated = MAXU;

    SharedSegment pDataDeviceSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    SharedSegment pVersionedDataSegment =
        pSegmentFactory->newVersionedSegment(
            pDataDeviceSegment,
            pShadowLogSegment,
            header.onlineUuid,
            header.versionNumber + (recoveryRequired ? 0 : 1));

    pVersionedSegment = SegmentFactory::dynamicCast<VersionedSegment *>(
        pVersionedDataSegment);

    // If recovery is required, defer initializing the data segment for use
    // until after recovery has completed.
    if (areSnapshotsEnabled()) {
        pDataSegment =
            pSegmentFactory->newVersionedRandomAllocationSegment(
                pVersionedDataSegment,
                pTempSegment,
                openMode.create,
                recoveryRequired);
    } else {
        pDataSegment =
            pSegmentFactory->newRandomAllocationSegment(
                pVersionedDataSegment,
                openMode.create,
                recoveryRequired);
    }
}

void Database::createTempSegment()
{
    // If the temp device file already exists, use it; otherwise, create it.
    // There's no point in recreating it on every startup.  But REVIEW:  sizing
    // issues.
    DeviceMode tempMode = openMode;
    tempMode.create = !FileSystem::doesFileExist(tempDeviceName.c_str());

    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramTempPrefix, tempMode, deviceParams);
    FileSize initialSize = FileSize(0);
    if (shouldForceTxns()) {
        initialSize = deviceParams.nPagesMin * pCache->getPageSize();
    }

    SharedRandomAccessDevice pTempDevice(
        new RandomAccessFileDevice(tempDeviceName, tempMode, initialSize));
    pCache->registerDevice(tempDeviceId, pTempDevice);

    // This forces the full device size to be used.
    tempMode.create = false;

    // no header for temp device
    CompoundId::setDeviceId(deviceParams.firstBlockId, tempDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 0);

    SharedSegment pTempDeviceSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    // Reformat any existing temp data.
    pTempSegment =
        pSegmentFactory->newRandomAllocationSegment(
            pTempDeviceSegment,
            true);
}

const ConfigMap& Database::getConfigMap() const
{
    return configMap;
}

SharedCache Database::getCache() const
{
    return pCache;
}

SharedSegment Database::getDataSegment() const
{
    return pDataSegment;
}

SharedSegment Database::getTempSegment() const
{
    return pTempSegment;
}

SharedCheckpointThread Database::getCheckpointThread() const
{
    return pCheckpointThread;
}

SharedSegment Database::getSegmentById(
    SegmentId segmentId,
    SharedSegment pDataSegment)
{
    if (segmentId == TEMP_SEGMENT_ID) {
        return getTempSegment();
    } else {
        assert(segmentId == DEFAULT_DATA_SEGMENT_ID);
        if (pDataSegment) {
            return pDataSegment;
        } else {
            return getDataSegment();
        }
    }
}

SharedSegmentFactory Database::getSegmentFactory() const
{
    return pSegmentFactory;
}

SharedLogicalTxnLog Database::getTxnLog() const
{
    return pTxnLog;
}

void Database::allocateHeader()
{
    LinearDeviceSegmentParams deviceParams;
    CompoundId::setDeviceId(deviceParams.firstBlockId, dataDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 0);
    pHeaderSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    SegmentAccessor segmentAccessor(pHeaderSegment, pCache);
    DatabaseHeaderPageLock headerPageLock(segmentAccessor);

    PageId pageId;
    pTxnLog->setNextTxnId(FIRST_TXN_ID);
    pageId = headerPageLock.allocatePage();
    assert(pageId == headerPageId1);
    headerPageLock.getNodeForWrite() = header;
    pageId = headerPageLock.allocatePage();
    assert(pageId == headerPageId2);
    headerPageLock.getNodeForWrite() = header;
    headerPageLock.unlock();
    pHeaderSegment->checkpoint();
}

void Database::loadHeader(bool recovery)
{
    LinearDeviceSegmentParams deviceParams;
    CompoundId::setDeviceId(deviceParams.firstBlockId, dataDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId, 0);
    deviceParams.nPagesAllocated = 2;
    deviceParams.nPagesMax = 2;
    pHeaderSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    SegmentAccessor segmentAccessor(pHeaderSegment, pCache);
    DatabaseHeaderPageLock headerPageLock1(segmentAccessor);
    headerPageLock1.lockShared(headerPageId1);
    if (!headerPageLock1.checkMagicNumber()) {
        throw DataFormatExcn();
    }

    DatabaseHeaderPageLock headerPageLock2(segmentAccessor);
    headerPageLock2.lockShared(headerPageId2);
    if (!headerPageLock2.checkMagicNumber()) {
        throw DataFormatExcn();
    }

    DatabaseHeader const &header1 = headerPageLock1.getNodeForRead();
    DatabaseHeader const &header2 = headerPageLock2.getNodeForRead();
    if (recovery) {
        // TODO:  crc
        if (header2.versionNumber < header1.versionNumber) {
            header = header2;
        } else {
            header = header1;
        }
    } else {
        assert(header1.versionNumber == header2.versionNumber);
        // REVIEW:  should assert other fields equal as well?
        header = header1;
    }
    if (pTxnLog) {
        TxnId nextTxnId = header.txnLogCheckpointMemento.nextTxnId;
        pTxnLog->setNextTxnId(nextTxnId);
    }
}

TxnId Database::getLastCommittedTxnId()
{
    return header.txnLogCheckpointMemento.nextTxnId - 1;
}

void Database::checkpointImpl(CheckpointType checkpointType)
{
    assert(!isRecoveryRequired());

    if (checkpointType == CHECKPOINT_DISCARD) {
        recoveryRequired = true;
        // NOTE jvs 6-Mar-2006:  record this BEFORE anything else,
        // since pDataSegment->checkpoint(CHECKPOINT_DISCARD) will
        // destroy it
        header.shadowRecoveryPageId =
            pVersionedSegment->getOnlineRecoveryPageId();
        pDataSegment->checkpoint(checkpointType);
        if (!forceTxns) {
            // REVIEW jvs 8-Mar-2006:  I put in the forceTxns test
            // because when forceTxn is true, we actually use
            // CHECKPOINT_DISCARD for rollback, and there we DON'T
            // want to remove the other uncommitted transactions,
            // which is a side-effect of LogicalTxnLog::checkpoint(DISCARD).
            // Really, for forceTxns, we shouldn't be using
            // LogicalTxnLog at all.  And we should discriminate
            // between CHECKPOINT_DISCARD used to simulate a crash in
            // tests and CHECKPOINT_DISCARD used to implement rollback
            // as part of forceTxns.
            LogicalTxnLogCheckpointMemento crashMemento;
            pTxnLog->checkpoint(crashMemento, checkpointType);
        }
        return;
    }

    header.versionNumber = pVersionedSegment->getVersionNumber();
    if (checkpointType == CHECKPOINT_FLUSH_FUZZY) {
        header.versionNumber--;
    }
    pDataSegment->checkpoint(checkpointType);
    header.shadowRecoveryPageId = pVersionedSegment->getRecoveryPageId();
    pTxnLog->checkpoint(
        header.txnLogCheckpointMemento,
        checkpointType);
    writeHeader();
    pVersionedSegment->deallocateCheckpointedLog(checkpointType);
    pTxnLog->deallocateCheckpointedLog(
        header.txnLogCheckpointMemento, checkpointType);

    StrictMutexGuard mutexGuard(mutex);
    // TODO:  provide a counter which records the amount of data flushed by the
    // checkpoint
    ++nCheckpointsStat;
    ++nCheckpoints;
    condition.notify_all();
}

void Database::requestCheckpoint(CheckpointType checkpointType, bool async)
{
    StrictMutexGuard mutexGuard(mutex);
    uint nCheckpointsBefore = nCheckpoints;
    mutexGuard.unlock();

    if (forceTxns && (checkpointType == CHECKPOINT_FLUSH_FUZZY)) {
        // fuzzy checkpoints aren't meaningful in forceTxns mode,
        // so treat them as sharp
        checkpointType = CHECKPOINT_FLUSH_ALL;
    }

    pCheckpointThread->requestCheckpoint(checkpointType);

    if (async) {
        return;
    }

    mutexGuard.lock();
    while (nCheckpoints == nCheckpointsBefore) {
        condition.wait(mutexGuard);
    }
}

void Database::writeHeader()
{
    // TODO:  crc

    // NOTE:  use synchronous writes to guarantee that first write completes
    // before second one starts (otherwise a crash could leave both copies
    // corrupted)

    SegmentAccessor segmentAccessor(pHeaderSegment, pCache);
    DatabaseHeaderPageLock headerPageLock(segmentAccessor);

    headerPageLock.lockExclusive(headerPageId1);
    headerPageLock.getNodeForWrite() = header;
    pCache->flushPage(headerPageLock.getPage(), false);

    headerPageLock.lockExclusive(headerPageId2);
    headerPageLock.getNodeForWrite() = header;
    pCache->flushPage(headerPageLock.getPage(), false);
}

void Database::recoverOnline()
{
    // REVIEW jvs 8-Aug-2006:  This procedure has one questionable aspect,
    // which is that it leaves images of newly allocated pages in cache.
    // Updated pages are handled by recovery from the log, but newly allocated
    // pages are not.  They will be clean, so they shouldn't really cause any
    // trouble, but their presence could be, at a minimum, confusing.

    assert(forceTxns);
    header.shadowRecoveryPageId =
        pVersionedSegment->getOnlineRecoveryPageId();
    pVersionedSegment->prepareOnlineRecovery();
    recoveryRequired = true;

    // after recovery, flush recovered data pages; no need to discard them
    recoverPhysical(CHECKPOINT_FLUSH_ALL);

    // this will bump up version number to be used by further page writes
    checkpointImpl(CHECKPOINT_FLUSH_ALL);
}

void Database::recover(
    LogicalTxnParticipantFactory &txnParticipantFactory)
{
    recoverPhysical(CHECKPOINT_FLUSH_AND_UNMAP);

    // REVIEW:  are shadows being correctly logged during recovery?  They have
    // to be, otherwise we can't re-recover after a failed recovery.

    // TODO:  encapsulate memento->PageId translation in txn somewhere
    SharedSegment pTxnLogSegment = createTxnLogSegment(
        openMode,
        CompoundId::getPageId(
            header.txnLogCheckpointMemento.logPosition.segByteId));

    SegmentAccessor logSegmentAccessor(pTxnLogSegment, pCache);

    SharedLogicalRecoveryLog pRecoveryLog =
        LogicalRecoveryLog::newLogicalRecoveryLog(
            txnParticipantFactory,
            logSegmentAccessor,
            header.onlineUuid,
            pSegmentFactory);
    logSegmentAccessor.reset();

    pRecoveryLog->recover(header.txnLogCheckpointMemento);
    assert(pRecoveryLog.unique());
    pRecoveryLog.reset();
    assert(pTxnLogSegment.unique());
    pTxnLogSegment.reset();

    closeDevices();
    deleteLogs();
    FENNEL_TRACE(TRACE_INFO, "recovery completed");

    openSegments();
}

void Database::recoverPhysical(CheckpointType checkpointType)
{
    assert(!openMode.create);
    assert(isRecoveryRequired());
    recoveryRequired = false;

    FENNEL_TRACE(
        TRACE_INFO,
        "recovery beginning; page version = "
        << header.versionNumber);

    if (header.shadowRecoveryPageId != NULL_PAGE_ID) {
        pVersionedSegment->recover(
            pDataSegment,
            header.shadowRecoveryPageId,
            header.versionNumber);
        pDataSegment->checkpoint(checkpointType);
        header.versionNumber = pVersionedSegment->getVersionNumber();
        header.shadowRecoveryPageId = NULL_PAGE_ID;
        writeHeader();
        pVersionedSegment->deallocateCheckpointedLog(
            CHECKPOINT_FLUSH_AND_UNMAP);
    }
}

bool Database::isRecoveryRequired() const
{
    return recoveryRequired;
}

void Database::readDeviceParams(
    std::string paramNamePrefix,
    DeviceMode deviceMode,
    LinearDeviceSegmentParams &deviceParams)
{
    deviceParams.nPagesMin = configMap.getIntParam(
        paramNamePrefix + paramInitSizeSuffix);
    if (configMap.isParamSet(paramNamePrefix + paramIncSizeSuffix)) {
        deviceParams.nPagesIncrement = configMap.getIntParam(
            paramNamePrefix + paramIncSizeSuffix);
    }
    deviceParams.nPagesMax = configMap.getIntParam(
        paramNamePrefix + paramMaxSizeSuffix);
    if (!deviceParams.nPagesMax) {
        deviceParams.nPagesMax = MAXU;
    }
    if (deviceMode.create) {
        deviceParams.nPagesAllocated = 0;
    } else {
        deviceParams.nPagesAllocated = MAXU;
    }
}

StoredTypeDescriptorFactory const &Database::getTypeFactory() const
{
    return typeFactory;
}

void Database::writeStats(StatsTarget &target)
{
    pCache->writeStats(target);

    StrictMutexGuard mutexGuard(mutex);
    target.writeCounter(
        "DatabaseCheckpoints", nCheckpointsStat);
    target.writeCounter(
        "DatabaseCheckpointsSinceInit", nCheckpoints);
    if (pDataSegment) {
        target.writeCounter(
            "DatabasePagesAllocated", pDataSegment->getAllocatedSizeInPages());
        // +2 for the database header pages
        target.writeCounter(
            "DatabasePagesOccupiedHighWaterSinceInit",
            pDataSegment->getNumPagesOccupiedHighWater() + 2);
        target.writeCounter(
            "DatabasePagesExtendedSinceInit",
            pDataSegment->getNumPagesExtended());
    }
    if (pTempSegment) {
        target.writeCounter(
            "TempPagesAllocated", pTempSegment->getAllocatedSizeInPages());
        target.writeCounter(
            "TempPagesOccupiedHighWaterSinceInit",
            pTempSegment->getNumPagesOccupiedHighWater());
        target.writeCounter(
            "TempPagesExtendedSinceInit",
            pTempSegment->getNumPagesExtended());
    }
    nCheckpointsStat = 0;
}

bool Database::shouldForceTxns() const
{
    return forceTxns;
}

bool Database::areSnapshotsEnabled() const
{
    return (forceTxns && !disableSnapshots);
}

void Database::deallocateOldPages(TxnId oldestLabelCsn)
{
    uint iSegAlloc = 0;
    ExtentNum extentNum = 0;
    // REVIEW zfong 3/12/07 - Determine a good value for numPages.  This
    // corresponds to the number of pages we will deallocate during a single
    // iteration.  We will be holding the checkpoint mutex for the duration
    // of an iteration so we don't want to make the value too big.  But at
    // the same time, we don't want to make it too small either, because it
    // would then require a large number of iterations to clean out all old
    // pages.
    uint numPages = 100;

    // Determine the oldest active txnId.
    TxnId oldestActiveTxnId = pTxnLog->getOldestActiveTxnId();

    // Take the minimum of the oldest active txnId and the
    // oldest active label + 1, if there are any active labels.
    // +1 because txns using that label will have ids bigger than that
    // label's csn.
    TxnId oldestTxnId;
    if (oldestLabelCsn == NULL_TXN_ID) {
        oldestTxnId = oldestActiveTxnId;
    } else {
        oldestTxnId = std::min(oldestActiveTxnId, oldestLabelCsn + 1);
    }

    // Gather a batch of old pageIds and then deallocate them.  After each
    // deallocation, issue a checkpoint to flush the modified allocation
    // node pages.  Continue this in a loop until we've read through all
    // allocation node pages.

    PageSet oldPageSet;
    VersionedRandomAllocationSegment *pVersionedRandomSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            pDataSegment);
    bool morePages = true;
    do {
        morePages =
            pVersionedRandomSegment->getOldPageIds(
                iSegAlloc,
                extentNum,
                oldestTxnId,
                numPages,
                oldPageSet);

        // Hold the checkpoint mutex while deallocating old pages, if there
        // are pages to deallocate.
        if (!oldPageSet.empty()) {
            SXMutexSharedGuard actionMutexGuard(
                pCheckpointThread->getActionMutex());
            if (disableDeallocateOld) {
                return;
            }
            pVersionedRandomSegment->deallocateOldPages(
                oldPageSet,
                oldestTxnId);

            actionMutexGuard.unlock();
            requestCheckpoint(CHECKPOINT_FLUSH_ALL, false);
            oldPageSet.clear();
        }
    } while (morePages);
}

TxnId Database::initiateBackup(
    const std::string &backupFilePathname,
    bool checkSpaceRequirements,
    FileSize spacePadding,
    TxnId lowerBoundCsn,
    const std::string &compressionProgram,
    FileSize &dataDeviceSize,
    const volatile bool &aborted)
{
    FENNEL_TRACE(TRACE_FINE, "Started Fennel metadata backup");

    // Snapshots must be enabled
    if (!areSnapshotsEnabled()) {
        throw FennelExcn(
            FennelResource::instance().unsupportedOperation("System backup"));
    }

    // Hold the checkpoint mutex while backing up the header and allocation
    // node pages
    SXMutexSharedGuard actionMutexGuard(pCheckpointThread->getActionMutex());

    // Another backup should not have already been initiated
    assert(!disableDeallocateOld);
    assert(pBackupRestoreDevice == NULL);

    // The upper bound csn for this backup is the txnId of the last committed,
    // write txn.  Note that the next txnId to be assigned may be a larger
    // value because of read-only txns.  But we don't care about read-only
    // txns.  We just want the txnId that's in sync with what's reflected in
    // the header.
    TxnId upperBoundCsn = getLastCommittedTxnId();

    disableDeallocateOld = true;
    dataDeviceSize = pDataDevice->getSizeInBytes();

    // Use the prefetch setting to determine how many scratch pages to
    // allocate.  Note that these scratch pages are not being accounted
    // for in the resource governor and come from the reserve pool that
    // the resource governor currently sets aside.
    uint nScratchPages, rate;
    pCache->getPrefetchParams(nScratchPages, rate);

    scratchAccessor = pSegmentFactory->newScratchSegment(pCache);
    pBackupRestoreDevice =
        SegPageBackupRestoreDevice::newSegPageBackupRestoreDevice(
             backupFilePathname,
#ifdef __MSVC__
             "wb",
#else
             "w",
#endif
             compressionProgram,
             nScratchPages,
             2,
             scratchAccessor,
             pCache->getDeviceAccessScheduler(*pDataDevice),
             pDataDevice);
    VersionedRandomAllocationSegment *pVRSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            pDataSegment);

    try {
        pBackupRestoreDevice->backupPage(
            pHeaderSegment->translatePageId(headerPageId1));
        pBackupRestoreDevice->backupPage(
            pHeaderSegment->translatePageId(headerPageId2));
        // First wait for writes of the header pages to complete before backing
        // up the allocation node pages.
        pBackupRestoreDevice->waitForPendingWrites();
        BlockNum nDataPages =
            pVRSegment->backupAllocationNodes(
                pBackupRestoreDevice,
                checkSpaceRequirements,
                lowerBoundCsn,
                upperBoundCsn,
                aborted);

        // Verify space if specified, now that we know how many data pages
        // will be backed up
        if (checkSpaceRequirements) {
            FileSize spaceAvailable;
            FileSystem::getDiskFreeSpace(
                backupFilePathname.c_str(),
                spaceAvailable);
            FileSize spaceRequired =
                nDataPages * pDataSegment->getFullPageSize();
            // TODO zfong 9/16/08 - Revisit the compression factor after more
            // testing.  Set conservatively to 5, for now.
            if (compressionProgram.length() != 0) {
                spaceRequired /= 5;
            }
            spaceRequired += spacePadding;
            if (spaceAvailable < spaceRequired) {
                throw FennelExcn(FennelResource::instance().outOfBackupSpace());
            }
        }
    } catch (...) {
        cleanupBackupRestore(true);
        // abort exception takes precedence
        if (aborted) {
            FENNEL_TRACE(TRACE_FINE, "abort detected");
            throw AbortExcn();
        } else {
            throw;
        }
    }

    FENNEL_TRACE(TRACE_FINE, "Finished Fennel metadata backup");
    return upperBoundCsn;
}

void Database::completeBackup(
    TxnId lowerBoundCsn,
    TxnId upperBoundCsn,
    const volatile bool &aborted)
{
    FENNEL_TRACE(TRACE_FINE, "Started Fennel data page backup");
    assert(disableDeallocateOld);
    assert(pBackupRestoreDevice != NULL);

    VersionedRandomAllocationSegment *pVRSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            pDataSegment);
    try {
        pVRSegment->backupDataPages(
            pBackupRestoreDevice,
            lowerBoundCsn,
            upperBoundCsn,
            aborted);
        cleanupBackupRestore(true);
    } catch (...) {
        cleanupBackupRestore(true);
        // abort exception takes precedence
        if (aborted) {
            FENNEL_TRACE(TRACE_FINE, "abort detected");
            throw AbortExcn();
        } else {
            throw;
        }
    }

    FENNEL_TRACE(TRACE_FINE, "Finished Fennel data page backup");
}

void Database::abortBackup()
{
    FENNEL_TRACE(TRACE_FINE, "Aborting Fennel backup");
    cleanupBackupRestore(true);
}

void Database::restoreFromBackup(
    const std::string &backupFilePathname,
    FileSize newSize,
    const std::string &compressionProgram,
    TxnId lowerBoundCsn,
    TxnId upperBoundCsn,
    const volatile bool &aborted)
{
    FENNEL_TRACE(TRACE_FINE, "Started Fennel restore");

    // Snapshots must be enabled
    if (!areSnapshotsEnabled()) {
        throw FennelExcn(
            FennelResource::instance().unsupportedOperation("System restore"));
    }

    // Verify that the last committed csn in the database header matches the
    // lower bound csn.
    if (lowerBoundCsn != NULL_TXN_ID) {
        TxnId headerTxnId = getLastCommittedTxnId();
        if (headerTxnId != lowerBoundCsn) {
            throw FennelExcn(
                FennelResource::instance().mismatchedRestore());
        }
    }

    pDataDevice->setSizeInBytes(newSize);

    VersionedRandomAllocationSegment *pVRSegment =
        SegmentFactory::dynamicCast<VersionedRandomAllocationSegment *>(
            pDataSegment);

    uint nScratchPages, rate;
    pCache->getPrefetchParams(nScratchPages, rate);

    scratchAccessor =
        pSegmentFactory->newScratchSegment(pCache);
    pBackupRestoreDevice =
        SegPageBackupRestoreDevice::newSegPageBackupRestoreDevice(
             backupFilePathname,
#ifdef __MSVC__
             "rb",
#else
             "r",
#endif
             compressionProgram,
             nScratchPages,
             0,
             scratchAccessor,
             pCache->getDeviceAccessScheduler(*pDataDevice),
             pDataDevice);

    // Flush and unmap pages from the cache that will be restored, i.e., any
    // VersionedRandomAllocationSegment or database header pages, including
    // the header page just read above.  We need to unmap these pages to
    // ensure that the restore doesn't read old pages.
    MappedPageListenerPredicate dataPredicate(*pVRSegment);
    pCache->checkpointPages(dataPredicate, CHECKPOINT_FLUSH_AND_UNMAP);
    MappedPageListenerPredicate headerPredicate(*pHeaderSegment);
    pCache->checkpointPages(headerPredicate, CHECKPOINT_FLUSH_AND_UNMAP);

    try {
        // Restore the rest of the pages, including the database header pages
        pBackupRestoreDevice->restorePage(
            pHeaderSegment->translatePageId(headerPageId1));
        pBackupRestoreDevice->restorePage(
            pHeaderSegment->translatePageId(headerPageId2));
        pVRSegment->restoreFromBackup(
            pBackupRestoreDevice,
            lowerBoundCsn,
            upperBoundCsn,
            aborted);
        cleanupBackupRestore(false);
    } catch (...) {
        cleanupBackupRestore(false);
        // abort exception takes precedence
        if (aborted) {
            FENNEL_TRACE(TRACE_FINE, "abort detected");
            throw AbortExcn();
        } else {
            throw;
        }
    }

    // Reload the header pages so future checkpoints will flush the
    // restored data.  Issue a recover call on the versioned segment
    // to reset the version number and online uuid to the values that are
    // now in the header.
    loadHeader(false);
    pVersionedSegment->recover(
        pDataSegment,
        NULL_PAGE_ID,
        header.versionNumber,
        header.onlineUuid);

    FENNEL_TRACE(TRACE_FINE, "Finished Fennel restore");
}

void Database::cleanupBackupRestore(bool isBackup)
{
    if (pBackupRestoreDevice) {
        pBackupRestoreDevice.reset();
    }
    if (scratchAccessor.pSegment) {
        scratchAccessor.reset();
    }
    if (isBackup) {
        SXMutexSharedGuard actionMutexGuard(
            pCheckpointThread->getActionMutex());
        disableDeallocateOld = false;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End Database.cpp

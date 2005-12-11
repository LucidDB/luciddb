/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2003-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
#include "fennel/db/Database.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FileSystem.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/cache/Cache.h"
#include "fennel/segment/SegmentFactory.h"
#include "fennel/segment/LinearDeviceSegment.h"
#include "fennel/segment/Segment.h"
#include "fennel/segment/VersionedSegment.h"
#include "fennel/common/CompoundId.h"
#include "fennel/txn/LogicalTxnLog.h"
#include "fennel/txn/LogicalRecoveryLog.h"
#include "fennel/common/StatsTarget.h"
#include "fennel/common/FennelResource.h"

#include <boost/filesystem/operations.hpp>

FENNEL_BEGIN_CPPFILE("$Id$");

using namespace boost::filesystem;

ParamName Database::paramDatabaseDir = "databaseDir";
ParamName Database::paramResourceDir = "resourceDir";
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
    SharedTraceTarget pTraceTarget)
{
    return SharedDatabase(
        new Database(pCacheInit, configMapInit, openModeInit, pTraceTarget),
        ClosableObjectDestructor());
}

Database::Database(
    SharedCache pCacheInit,
    ConfigMap const &configMapInit,
    DeviceMode openModeInit,
    SharedTraceTarget pTraceTarget)
    : TraceSource(pTraceTarget,"database"),
      pCache(pCacheInit),
      configMap(configMapInit)
{
    openMode = openModeInit;

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
        configMap,pTraceTarget);

    if (FileSystem::doesFileExist(tempDeviceName.c_str())) {
        FileSystem::remove(tempDeviceName.c_str());
    }
    
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
    createDataDevice();
    loadHeader(true);
    writeHeader();
    SharedSegment pShadowLogSegment = createShadowLog(openMode);
    createDataSegment(pShadowLogSegment);
}

void Database::openSegments()
{
    FENNEL_TRACE(TRACE_INFO, "opening database");
    
    pCheckpointThread = SharedCheckpointThread(
        new CheckpointThread(*this),
        ClosableObjectDestructor());
    
    createTempSegment();

    header.onlineUuid.generate();
    
    DeviceMode txnLogMode = openMode;
    txnLogMode.create = true;
    txnLogMode.direct = true;
    createTxnLog(txnLogMode);

    createDataDevice();
    
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
    createDataSegment(pShadowLogSegment);
    
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
    
    if (isRecoveryRequired()) {
        closeDevices();
    } else {
        checkpointImpl();
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
    pDataSegment->close();
    pDataSegment.reset();
    pHeaderSegment.reset();
    pTempSegment->close();
    pTempSegment.reset();

    // for incomplete recovery, this device may not have been opened yet
    if (pCache->getDevice(txnLogDeviceId)) {
        pCache->unregisterDevice(txnLogDeviceId);
    }
    pCache->unregisterDevice(shadowDeviceId);
    pCache->unregisterDevice(dataDeviceId);
    pCache->unregisterDevice(tempDeviceId);
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
        std::string filename = itr->leaf();
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
    DeviceMode txnLogMode,PageId oldestPageId)
{
    SharedRandomAccessDevice pTxnLogDevice(
        new RandomAccessFileDevice(txnLogDeviceName,txnLogMode));
    pCache->registerDevice(txnLogDeviceId,pTxnLogDevice);

    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramTxnLogPrefix,txnLogMode,deviceParams);
    CompoundId::setDeviceId(deviceParams.firstBlockId,txnLogDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId,0);
    deviceParams.nPagesAllocated = MAXU;
    deviceParams.nPagesIncrement = 0;
    deviceParams.nPagesMax = deviceParams.nPagesMin;
    
    SharedSegment pLinearSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);
    
    SharedSegment pTxnLogSegment = pSegmentFactory->newCircularSegment(
        pLinearSegment,pCheckpointThread,oldestPageId);
        
    return pTxnLogSegment;
}

void Database::createTxnLog(DeviceMode txnLogMode)
{
    SharedSegment pTxnLogSegment = createTxnLogSegment(
        txnLogMode,NULL_PAGE_ID);
    SegmentAccessor segmentAccessor(pTxnLogSegment,pCache);
    pTxnLog = LogicalTxnLog::newLogicalTxnLog(
        segmentAccessor,
        header.onlineUuid,
        pSegmentFactory);
    pTxnLog->checkpoint(header.txnLogCheckpointMemento);
}

SharedSegment Database::createShadowLog(DeviceMode shadowLogMode)
{
    SharedRandomAccessDevice pShadowDevice(
        new RandomAccessFileDevice(shadowDeviceName,shadowLogMode));
    pCache->registerDevice(shadowDeviceId,pShadowDevice);

    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramShadowLogPrefix,shadowLogMode,deviceParams);
    CompoundId::setDeviceId(deviceParams.firstBlockId,shadowDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId,0);
    deviceParams.nPagesAllocated = MAXU;
    deviceParams.nPagesIncrement = 0;
    deviceParams.nPagesMax = deviceParams.nPagesMin;
    
    SharedSegment pShadowSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    PageId oldestPageId = NULL_PAGE_ID;
    if (!shadowLogMode.create) {
        oldestPageId = header.shadowRecoveryPageId;
    }
    pShadowSegment = pSegmentFactory->newCircularSegment(
        pShadowSegment,
        pCheckpointThread,
        oldestPageId);
    
    return pSegmentFactory->newWALSegment(pShadowSegment);
}

void Database::createDataDevice()
{
    SharedRandomAccessDevice pDataDevice(
        new RandomAccessFileDevice(dataDeviceName,openMode));
    pCache->registerDevice(dataDeviceId,pDataDevice);
}

void Database::createDataSegment(
    SharedSegment pShadowLogSegment)
{
    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramDatabasePrefix,openMode,deviceParams);
    
    // first data BlockId is located after the two database header pages
    CompoundId::setDeviceId(deviceParams.firstBlockId,dataDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId,2);

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

    pDataSegment =
        pSegmentFactory->newRandomAllocationSegment(
            pVersionedDataSegment,
            openMode.create);
}

void Database::createTempSegment()
{
    // If the temp device file already exists, use it; otherwise, create it.
    // There's no point in recreating it on every startup.  But REVIEW:  sizing
    // issues.
    DeviceMode tempMode = openMode;
    tempMode.create = !FileSystem::doesFileExist(tempDeviceName.c_str());
    
    SharedRandomAccessDevice pTempDevice(
        new RandomAccessFileDevice(tempDeviceName,tempMode));
    pCache->registerDevice(tempDeviceId,pTempDevice);

    // This forces the full device size to be used.
    tempMode.create = false;
    
    LinearDeviceSegmentParams deviceParams;
    readDeviceParams(paramTempPrefix,tempMode,deviceParams);
    
    // no header for temp device
    CompoundId::setDeviceId(deviceParams.firstBlockId,tempDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId,0);

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

SharedSegment Database::getSegmentById(SegmentId segmentId)
{
    if (segmentId == TEMP_SEGMENT_ID) {
        return getTempSegment();
    } else {
        assert(segmentId == DEFAULT_DATA_SEGMENT_ID);
        return getDataSegment();
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
    CompoundId::setDeviceId(deviceParams.firstBlockId,dataDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId,0);
    pHeaderSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);

    SegmentAccessor segmentAccessor(pHeaderSegment,pCache);
    DatabaseHeaderPageLock headerPageLock(segmentAccessor);
        
    PageId pageId;
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
    CompoundId::setDeviceId(deviceParams.firstBlockId,dataDeviceId);
    CompoundId::setBlockNum(deviceParams.firstBlockId,0);
    deviceParams.nPagesAllocated = 2;
    deviceParams.nPagesMax = 2;
    pHeaderSegment =
        pSegmentFactory->newLinearDeviceSegment(
            pCache,
            deviceParams);
    
    SegmentAccessor segmentAccessor(pHeaderSegment,pCache);
    DatabaseHeaderPageLock headerPageLock1(segmentAccessor);
    headerPageLock1.lockShared(headerPageId1);

    DatabaseHeaderPageLock headerPageLock2(segmentAccessor);
    headerPageLock2.lockShared(headerPageId2);

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
}

void Database::checkpointImpl(CheckpointType checkpointType)
{
    assert(!isRecoveryRequired());

    if (checkpointType == CHECKPOINT_DISCARD) {
        recoveryRequired = true;
        pDataSegment->checkpoint(checkpointType);
        LogicalTxnLogCheckpointMemento crashMemento;
        pTxnLog->checkpoint(crashMemento,checkpointType);
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
        header.txnLogCheckpointMemento,checkpointType);

    StrictMutexGuard mutexGuard(mutex);
    // TODO:  make this proportional to the amount of data flushed by the
    // checkpoint?
    nCheckpointsStat += 10;
    ++nCheckpoints;
    condition.notify_all();
}

void Database::requestCheckpoint(CheckpointType checkpointType,bool async)
{
    StrictMutexGuard mutexGuard(mutex);
    uint nCheckpointsBefore = nCheckpoints;
    mutexGuard.unlock();

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
    
    SegmentAccessor segmentAccessor(pHeaderSegment,pCache);
    DatabaseHeaderPageLock headerPageLock(segmentAccessor);
    
    headerPageLock.lockExclusive(headerPageId1);
    headerPageLock.getNodeForWrite() = header;
    pCache->flushPage(headerPageLock.getPage(),false);
    
    headerPageLock.lockExclusive(headerPageId2);
    headerPageLock.getNodeForWrite() = header;
    pCache->flushPage(headerPageLock.getPage(),false);
}

void Database::recover(
    LogicalTxnParticipantFactory &txnParticipantFactory)
{
    assert(!openMode.create);
    assert(isRecoveryRequired());

    FENNEL_TRACE(
        TRACE_INFO,
        "recovery beginning; page version = "
        << header.versionNumber);

    if (header.shadowRecoveryPageId != NULL_PAGE_ID) {
        pVersionedSegment->recover(header.shadowRecoveryPageId);
        pVersionedSegment->checkpoint(CHECKPOINT_FLUSH_AND_UNMAP);
        header.versionNumber = pVersionedSegment->getVersionNumber();
        header.shadowRecoveryPageId = NULL_PAGE_ID;
        writeHeader();
        pVersionedSegment->deallocateCheckpointedLog(
            CHECKPOINT_FLUSH_AND_UNMAP);
    }

    // REVIEW:  are shadows being correctly logged during recovery?  They have
    // to be, otherwise we can't re-recover after a failed recovery.
    
    // TODO:  encapsulate memento->PageId translation in txn somewhere
    SharedSegment pTxnLogSegment = createTxnLogSegment(
        openMode,
        CompoundId::getPageId(
            header.txnLogCheckpointMemento.logPosition.segByteId));
    
    SegmentAccessor logSegmentAccessor(pTxnLogSegment,pCache);

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
    recoveryRequired = false;
    FENNEL_TRACE(TRACE_INFO, "recovery completed");

    openSegments();
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
    target.writeCounter("DatabaseCheckpoints",nCheckpointsStat);
    nCheckpointsStat = 0;
}

FENNEL_END_CPPFILE("$Id$");

// End Database.cpp

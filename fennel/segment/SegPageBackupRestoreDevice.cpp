/*
// Licensed to DynamoBI Corporation (DynamoBI) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  DynamoBI licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at

//   http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/common/FennelResource.h"
#include "fennel/common/FileSystem.h"
#include "fennel/segment/SegPageBackupRestoreDevice.h"
#include "fennel/segment/SegmentFactory.h"

FENNEL_BEGIN_CPPFILE("$Id$");

void BackupRestorePage::setParent(WeakSegPageBackupRestoreDevice pParentInit)
{
    pParent = pParentInit;
}

PBuffer BackupRestorePage::getBuffer() const
{
    return pBuffer;
}

void BackupRestorePage::setBufferSize(uint bufferSizeInit)
{
    bufferSize = bufferSizeInit;
}

uint BackupRestorePage::getBufferSize() const
{
    return bufferSize;
}

void BackupRestorePage::setBuffer(PBuffer pBufferInit)
{
    pBuffer = pBufferInit;
}

BlockNum BackupRestorePage::getPageCounter()
{
    return pageCounter;
}

void BackupRestorePage::setPageCounter(BlockNum counter)
{
    pageCounter = counter;
}

void BackupRestorePage::setReadRequest(bool isReadInit)
{
    isRead = isReadInit;
}

void BackupRestorePage::notifyTransferCompletion(bool bSuccess)
{
    SharedSegPageBackupRestoreDevice sharedPtr = pParent.lock();
    StrictMutexGuard mutexGuard(sharedPtr->getMutex());

    if (isRead) {
        sharedPtr->notifyReadTransferCompletion(*this, bSuccess);
    } else {
        sharedPtr->notifyWriteTransferCompletion(*this, bSuccess);
    }

    // release the reference to the shared pointer while we're still holding
    // the parent mutex
    sharedPtr.reset();
}

SharedSegPageBackupRestoreDevice
SegPageBackupRestoreDevice::newSegPageBackupRestoreDevice(
    const std::string &backupFilePathname,
    const char *mode,
    const std::string &compressionProgram,
    uint nScratchPages,
    uint nReservedPages,
    SegmentAccessor &scratchAccessor,
    DeviceAccessScheduler &scheduler,
    SharedRandomAccessDevice pDataDevice)
{
    SharedSegPageBackupRestoreDevice pDevice =
        SharedSegPageBackupRestoreDevice(
            new SegPageBackupRestoreDevice(
                backupFilePathname,
                mode,
                compressionProgram,
                nScratchPages,
                nReservedPages,
                scratchAccessor,
                scheduler,
                pDataDevice),
            ClosableObjectDestructor());
    pDevice->init();
    return pDevice;
}

SegPageBackupRestoreDevice::SegPageBackupRestoreDevice(
    const std::string &backupFilePathnameInit,
    const char *modeInit,
    const std::string &compressionProgramInit,
    uint nScratchPagesInit,
    uint nReservedPagesInit,
    SegmentAccessor &scratchAccessorInit,
    DeviceAccessScheduler &schedulerInit,
    SharedRandomAccessDevice pDataDeviceInit)
    : backupFilePathname(backupFilePathnameInit),
      nReservedPages(nReservedPagesInit),
      scratchAccessor(scratchAccessorInit),
      scheduler(schedulerInit),
      pDataDevice(pDataDeviceInit)
{
    mode = (char *) modeInit;
    setCompressionProgramPathname(compressionProgramInit);
    nScratchPages = nScratchPagesInit;
    currPageReadCount = 0;
    currPageWriteCount = 0;
    backupFile = NULL;
}

void SegPageBackupRestoreDevice::init()
{
    if (compressionProgram.length() == 0) {
        backupFile = fopen(backupFilePathname.c_str(), mode);
        isCompressed = false;
    } else {
#ifdef __MSVC__
        throw FennelExcn(
            FennelResource::instance().unsupportedOperation("popen"));
#else
        std::ostringstream cmd;
        if (mode[0] == 'r') {
            cmd << compressionProgram.c_str() << " -dc "
                << backupFilePathname.c_str();
        } else {
            cmd << compressionProgram.c_str() << " > "
                << backupFilePathname.c_str();
        }
        backupFile = popen(cmd.str().c_str(), mode);
        isCompressed = true;
#endif
    }

    if (backupFile == NULL) {
        throw SysCallExcn(
            FennelResource::instance().openBackupFileFailed(
                backupFilePathname));
    }

    scratchLock.accessSegment(scratchAccessor);
    pageSize = scratchAccessor.pSegment->getUsablePageSize();
    initScratchPages(scratchLock, nScratchPages, nReservedPages, pageSize);
}

void SegPageBackupRestoreDevice::setCompressionProgramPathname(
    const std::string &programName)
{
    if (programName.length() == 0) {
        compressionProgram = "";
    } else {
        std::string path = "/bin/";
        compressionProgram = path + programName;
        if (!FileSystem::doesFileExist(compressionProgram.c_str())) {
            path = "/usr/bin/";
            compressionProgram = path + programName;
            if (!FileSystem::doesFileExist(compressionProgram.c_str())) {
                compressionProgram = programName;
            }
        }
    }
}

void SegPageBackupRestoreDevice::initScratchPages(
    SegPageLock &scratchLock,
    uint nScratchPages,
    uint nReservedPages,
    uint bufferSize)
{
    StrictMutexGuard mutexGuard(mutex);

    // Allocate an array of requests and associate a scratch page with each
    // one.  Initialize the free scratch page queue with these requests.
    backupRestorePages.reset(new BackupRestorePage[nScratchPages]);
    for (uint i = 0; i < nScratchPages; i++) {
        scratchLock.allocatePage();
        backupRestorePages[i].setParent(
            WeakSegPageBackupRestoreDevice(shared_from_this()));
        backupRestorePages[i].setBufferSize(pageSize);
        backupRestorePages[i].setBuffer(
            scratchLock.getPage().getWritableData());
        freeScratchPageQueue.push_back(&backupRestorePages[i]);
    }

    reservedPages.reset(new PBuffer[nReservedPages]);
    for (uint i = 0; i < nReservedPages; i++) {
        scratchLock.allocatePage();
        reservedPages[i] = scratchLock.getPage().getWritableData();
    }
}

PBuffer SegPageBackupRestoreDevice::getReservedBufferPage()
{
    if (nReservedPages == 0) {
        assert(false);
    }
    return reservedPages[--nReservedPages];
}

void SegPageBackupRestoreDevice::writeBackupPage(PConstBuffer pageBuffer)
{
    writeBackupPage(pageBuffer, false);
}

void SegPageBackupRestoreDevice::writeBackupPage(
    PConstBuffer pageBuffer,
    bool scheduledWrite)
{
    size_t pagesWritten = fwrite(pageBuffer, pageSize, 1, backupFile);
    if (pagesWritten < 1) {
        if (!scheduledWrite) {
            throw SysCallExcn(
                FennelResource::instance().writeBackupFileFailed(
                    backupFilePathname));
        } else if (!pPendingExcn) {
            // For scheduled writes, indicate that an exception has occurred
            // so the calling thread can return the exception, unless there
            // already is a pending exception.
            pPendingExcn.reset(
                new SysCallExcn(
                    FennelResource::instance().writeBackupFileFailed(
                        backupFilePathname)));
        }
    }
}

void SegPageBackupRestoreDevice::backupPage(BlockId blockId)
{
    // This only schedules the read request.  Later when the read request
    // has been met, the write will be done in notifyReadTransferCompletion.

    // First, find a free scratch page
    BackupRestorePage *pScratchPage = getFreeScratchPage();

    pScratchPage->setPageCounter(currPageReadCount++);
    RandomAccessRequest request;
    request.pDevice = pDataDevice.get();
    request.cbOffset = (FileSize) pageSize * CompoundId::getBlockNum(blockId);
    request.cbTransfer = pageSize;
    request.type = RandomAccessRequest::READ;
    pScratchPage->setReadRequest(true);
    request.bindingList.push_back(*pScratchPage);
    scheduler.schedule(request);
}

BackupRestorePage *SegPageBackupRestoreDevice::getFreeScratchPage()
{
    while (true) {
        StrictMutexGuard mutexGuard(mutex);
        if (!freeScratchPageQueue.empty()) {
            BackupRestorePage *freePage = freeScratchPageQueue.back();
            freeScratchPageQueue.pop_back();
            return freePage;
        }

        // If no page is available, wait for one to become available (with
        // timeout just in case)
        boost::xtime atv;
        convertTimeout(100, atv);
        freeScratchPageCondition.timed_wait(mutexGuard, atv);
        checkPendingException();
    }
}

void SegPageBackupRestoreDevice::notifyReadTransferCompletion(
    BackupRestorePage &scratchPage,
    bool bSuccess)
{
    if (!bSuccess) {
        if (!pPendingExcn) {
            pPendingExcn.reset(
                new SysCallExcn(
                    FennelResource::instance().readDataPageFailed()));
        }
        return;
    }


    // If the page read is the next one to be written, write it out to the
    // backup file.  Otherwise, add it to the map of pending pages to
    // be written out.
    if (scratchPage.getPageCounter() == currPageWriteCount) {
        writeBackupPage(scratchPage.getBuffer(), true);
        freeScratchPage(scratchPage);
    } else {
        pendingWriteMap[scratchPage.getPageCounter()] = &scratchPage;
    }

    // Go back and see if the next set of pages that need to be written are
    // already in the pending write queue.  If they are, write them out.
    while (true) {
        PendingWriteMapIter iter = pendingWriteMap.find(currPageWriteCount);
        if (iter == pendingWriteMap.end()) {
            break;
        }
        BackupRestorePage *nextPage = iter->second;
        pendingWriteMap.erase(currPageWriteCount);
        writeBackupPage(nextPage->getBuffer(), true);
        freeScratchPage(*nextPage);
        // freeScratchPage increments currPageWriteCount, so loop to see
        // if that next page is available to be written
    }
}

void SegPageBackupRestoreDevice::freeScratchPage(BackupRestorePage &scratchPage)
{
    ++currPageWriteCount;
    freeScratchPageQueue.push_back(&scratchPage);
    freeScratchPageCondition.notify_all();
}

void SegPageBackupRestoreDevice::waitForPendingWrites()
{
    StrictMutexGuard mutexGuard(mutex);
    while (currPageWriteCount < currPageReadCount) {
        freeScratchPageCondition.wait(mutexGuard);
        checkPendingException();
    }

    checkPendingException();
}

void SegPageBackupRestoreDevice::restorePage(BlockId blockId)
{
    // Get a free scratch buffer to read the page from the backup file
    BackupRestorePage *pScratchPage = getFreeScratchPage();

    size_t pagesRead =
        fread(pScratchPage->getBuffer(), pageSize, 1, backupFile);
    if (pagesRead < 1) {
        throw SysCallExcn(
            FennelResource::instance().readBackupFileFailed(
                backupFilePathname));
    }

    // Schedule a request to write it
    pScratchPage->setPageCounter(currPageReadCount++);
    RandomAccessRequest request;
    request.pDevice = pDataDevice.get();
    request.cbOffset = (FileSize) pageSize * CompoundId::getBlockNum(blockId);
    request.cbTransfer = pageSize;
    request.type = RandomAccessRequest::WRITE;
    pScratchPage->setReadRequest(false);
    request.bindingList.push_back(*pScratchPage);
    scheduler.schedule(request);
}

void SegPageBackupRestoreDevice::notifyWriteTransferCompletion(
    BackupRestorePage &scratchPage,
    bool bSuccess)
{
    if (!bSuccess) {
        if (!pPendingExcn) {
            pPendingExcn.reset(
                new SysCallExcn(
                    FennelResource::instance().writeDataPageFailed()));
        }
        return;
    }

    freeScratchPage(scratchPage);
}

void SegPageBackupRestoreDevice::checkPendingException()
{
    if (pPendingExcn) {
        pPendingExcn->throwSelf();
    }
}

void SegPageBackupRestoreDevice::closeImpl()
{
    // Wait for any pending writes to complete before freeing up structures
    // because those writes were issued as async requests and their
    // notifications will need to come back to this object.
    waitForPendingWrites();

    StrictMutexGuard mutexGuard(mutex);
    if (scratchAccessor.pSegment) {
        scratchAccessor.pSegment->deallocatePageRange(
            NULL_PAGE_ID, NULL_PAGE_ID);
    }
    backupRestorePages.reset();
    reservedPages.reset();
    freeScratchPageQueue.clear();
    if (backupFile != NULL) {
        if (isCompressed) {
#ifdef __MSVC__
        throw FennelExcn(
            FennelResource::instance().unsupportedOperation("pclose"));
#else
            pclose(backupFile);
#endif
        } else {
            fclose(backupFile);
        }
        backupFile = NULL;
    }
}

StrictMutex &SegPageBackupRestoreDevice::getMutex()
{
    return mutex;
}

FENNEL_END_CPPFILE("$Id$");

// End SegPageBackupRestoreDevice.cpp

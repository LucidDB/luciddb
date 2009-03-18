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

#ifndef Fennel_SegPageBackupRestoreDevice_Included
#define Fennel_SegPageBackupRestoreDevice_Included

#include "fennel/common/ClosableObject.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/synch/SynchObj.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/segment/SegPageLock.h"

#include <vector>
#include <boost/enable_shared_from_this.hpp>
#include <boost/scoped_array.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * Random access request binding for I/O requests issued during backup/restore
 */
class BackupRestorePage : public RandomAccessRequestBinding
{
    /**
     * Pointer to the scratch buffer associated with this request
     */
    PBuffer pBuffer;

    /**
     * The counter that determines the order in which pages are written to
     * the backup file
     */
    BlockNum pageCounter;

    /**
     * Size of the scratch buffer
     */
    uint bufferSize;

    /**
     * True if this is a read request
     */
    bool isRead;

    /**
     * A pointer to the parent object that initiated this I/O request.  Uses
     * a weak pointer because of the circularity in the reference.
     */
    WeakSegPageBackupRestoreDevice pParent;

public:
    // implement RandomAccessRequestBinding
    virtual PBuffer getBuffer() const;
    virtual uint getBufferSize() const;
    virtual void notifyTransferCompletion(bool bSuccess);

    /**
     * Sets the initiating backup/restore object.
     *
     * @param pParentInit a pointer to the initiating backup/restore object
     */
    void setParent(WeakSegPageBackupRestoreDevice pParentInit);

    /**
     * Sets the buffer associated with this page.
     *
     * @param pBuffer the buffer
     */
    void setBuffer(PBuffer pBuffer);

    /**
     * Sets the size of the buffer.
     *
     * @param bufferSize the buffer size
     */
    void setBufferSize(uint bufferSize);

    /**
     * @return the counter that determines the order in which pages are
     * written to the backup file
     */
    BlockNum getPageCounter();

    /**
     * Sets the counter that determines the order in which pages are written
     * to the backup file.
     *
     * @param counter the counter
     */
    void setPageCounter(BlockNum counter);

    /**
     * Indicates whether the request is a read request.
     *
     * @param isRead true if this is a read request
     */
    void setReadRequest(bool isRead);
};

/**
 * Device used to backup and restore pages from a data segment.
 */
class SegPageBackupRestoreDevice
    : public boost::noncopyable,
        public ClosableObject,
        public boost::enable_shared_from_this<SegPageBackupRestoreDevice>
{
    /**
     * Full pathname of the backup file
     */
    std::string backupFilePathname;

    /**
     * Number of reserved scratch pages available
     */
    uint nReservedPages;

    /**
     * Scratch accessor used for temporary buffers
     */
    SegmentAccessor scratchAccessor;

    /**
     * Scheduler for I/O requests
     */
    DeviceAccessScheduler &scheduler;

    /**
     * Device corresponding to the segment pages that are backed up or restored
     */
    SharedRandomAccessDevice pDataDevice;

    /**
     * String value indicating how the device should be opened
     */
    char *mode;

    /*
     * If non-empty string, the full path of the program to use to compress or
     * decompress the backup file
     */
    std::string compressionProgram;

    /*
     * The number of scratch pages to allocate for backing up and restoring
     * data pages
     */
    uint nScratchPages;

    /**
     * True if the backup file is compressed
     */
    bool isCompressed;

    /**
     * Scratch lock
     */
    SegPageLock scratchLock;

    /**
     * File stream for the backup file
     */
    FILE *backupFile;

    /**
     * Size of each page backed up or restored
     */
    uint pageSize;

    /**
     * Array of reserved scratch pages
     */
    boost::scoped_array<PBuffer> reservedPages;

    /**
     * Queue of free scratch pages that can be used to read/write pages during
     * backup and restore
     */
    std::vector<BackupRestorePage *> freeScratchPageQueue;

    /**
     * Condition variable used for notification of free scratch page
     * availability
     */
    LocalCondition freeScratchPageCondition;

    /**
     * Mutex to ensure that only one thread is modifying shared data
     */
    StrictMutex mutex;

    typedef std::hash_map<BlockNum, BackupRestorePage *> PendingWriteMap;

    typedef PendingWriteMap::iterator PendingWriteMapIter;

    /**
     * A map containing pages waiting to be written to the backup file.  Each
     * page is keyed by the page's destination count within the backup file.
     */
    PendingWriteMap pendingWriteMap;

    /**
     * In the case of a backup, the counter corresponding to the page that
     * needs to be read, for reads that are scheduled.  In the case of a
     * restore, the counter corresponding to the page currently read from
     * the backup file.
     */
    BlockNum currPageReadCount;

    /**
     * In case of a backup, the counter corresponding to the page that needs to
     * be written to the backup file, for pages for which the reads were
     * scheduled.  In the case of a restore, the counter corresponding to
     * pages that have been written to the data device.
     */
    BlockNum currPageWriteCount;

    /**
     * Array of requests, representing pages being backed up or restored
     */
    boost::scoped_array<BackupRestorePage> backupRestorePages;

    /**
     * Pending exception, if there is one
     */
    boost::scoped_ptr<FennelExcn> pPendingExcn;

    /**
     * Initializes the backup/restore device.
     */
    void init();

    /**
     * Determines the full pathname for a compression program by locating
     * the program either in /bin or /usr/bin.  If the program doesn't exist
     * in either, then the user must specify the location of the program via
     * their path.
     *
     * @param programName name of the compression program, or an empty string
     * if compression isn't being used
     */
    void setCompressionProgramPathname(const std::string &programName);

    /**
     * Allocates scratch pages used by backup and restore.
     *
     * @param scratchLock scratch lock used to allocate pages
     * @param nScratchPages the number of scratch pages to allocate
     * @param nReservedPages the number of reserved scratch pages to allocate
     * @param bufferSize size of scratch page buffers
     */
    void initScratchPages(
        SegPageLock &scratchLock,
        uint nScratchPages,
        uint nReservedPages,
        uint bufferSize);

    /**
     * Retrieves an available scratch page.  Waits if necessary until a
     * free scratch page is available.
     *
     * @return a pointer to a scratch page
     */
    BackupRestorePage *getFreeScratchPage();

    /**
     * Puts the scratch page that is no longer being used back into the
     * free scratch page queue, increments counters tracking which page needs
     * to be written next to the backup file, and notifies any threads
     * waiting for a free scratch page.
     *
     * @param scratchPage scratch page that's now free
     */
    void freeScratchPage(BackupRestorePage &scratchPage);

    /**
     * Writes a single page of data to the backup file.
     *
     * @param pageBuffer buffer containing the page data
     * @param scheduledWrite true if the write was scheduled by the
     * device scheduler
     */
    void writeBackupPage(PConstBuffer pageBuffer, bool scheduledWrite);

    /**
     * Determines if there is a pending I/O exception, and if so, throws
     * the exception.
     */
    void checkPendingException();

    // Implement ClosableObject
    virtual void closeImpl();

public:
    /**
     * Opens a device that is used to backup and restore pages from a
     * segment.
     *
     * @param backupFilePathname pathname of the backup file
     * @param mode string indicating how the device should be opened
     * @param compressionProgram if non-empty string, the name of the program
     * to use to compress or decompress the backup file
     * @param nScratchPages number of scratch pages to allocate for backing
     * up and restoring data pages
     * @param nReservedPages additional scratch pages to allocate for
     * special reserved buffers
     * @param scratchAccessor accessor used to allocate temporary scratch
     * buffers
     * @param scheduler scheduler used for scheduling I/O requests
     * @param pDataDevice the device containing the data segment pages that
     * will be backed up or restored
     */
    SegPageBackupRestoreDevice(
        const std::string &backupFilePathname,
        const char *mode,
        const std::string &compressionProgram,
        uint nScratchPages,
        uint nReservedPages,
        SegmentAccessor &scratchAccessor,
        DeviceAccessScheduler &scheduler,
        SharedRandomAccessDevice pDataDevice);

    /**
     * Creates a new SegPageBackupRestoreDevice, returning a shared pointer
     * to the object.
     *
     * @param backupFilePathname pathname of the backup file
     * @param mode string indicating how the device should be opened
     * @param compressionProgram if non-empty string, the name of the program
     * to use to compress or decompress the backup file
     * @param nScratchPages number of scratch pages to allocate for backing
     * up and restoring data pages
     * @param nReservedPages additional scratch pages to allocate for
     * special reserved buffers
     * @param scratchAccessor accessor used to allocate temporary scratch
     * buffers
     * @param scheduler scheduler used for scheduling I/O requests
     * @param pDataDevice the device containing the data segment pages that
     * will be backed up or restored
     *
     * @return shared pointer to the new object
     */
    static SharedSegPageBackupRestoreDevice newSegPageBackupRestoreDevice(
        const std::string &backupFilePathname,
        const char *mode,
        const std::string &compressionProgram,
        uint nScratchPages,
        uint nReservedPages,
        SegmentAccessor &scratchAccessor,
        DeviceAccessScheduler &scheduler,
        SharedRandomAccessDevice pDataDevice);

    /**
     * Returns a pointer to one of the reserved buffer pages.  The number of
     * calls to this method cannot exceed the number of reserved buffers
     * available.
     *
     * @return pointer to an available reserved buffer; NULL if no more buffers
     * are available
     */
    PBuffer getReservedBufferPage();

    /**
     * Writes a single page of data to the backup file.
     *
     * @param pageBuffer the buffer containing the data
     */
    void writeBackupPage(PConstBuffer pageBuffer);

    /**
     * Backs up a data page by scheduling a request to read it, then writing
     * it to the backup file once the read request has been met.
     *
     * @param blockId block id of the page that needs to be backed up
     */
    void backupPage(BlockId blockId);

    /**
     * Receives notification that a read request has completed and writes
     * the page to the backup file.
     *
     * @param scratchPage the scratch page containing the data read
     * @param bSuccess whether the request was successful
     */
    void notifyReadTransferCompletion(
        BackupRestorePage &scratchPage,
        bool bSuccess);

    /**
     * Restores a page by reading it from the backup file and then
     * scheduling a request to write it to the data device.
     *
     * @param blockId block id of the page that needs to be restored
     */
    void restorePage(BlockId blockId);

    /**
     * Receives notification that a write request has completed.  Frees the
     * scratch buffer associated with the request.
     *
     * @param scratchPage the scratch page containing the data read
     * @param bSuccess whether the request was successful
     */
    void notifyWriteTransferCompletion(
        BackupRestorePage &scratchPage,
        bool bSuccess);

    /**
     * Waits for any pending writes to complete.
     */
    void waitForPendingWrites();

    /**
     * @return the mutex that ensures that only one thread is modifying this
     * object
     */
    StrictMutex &getMutex();
};

FENNEL_END_NAMESPACE

#endif

// End SegPageBackupRestoreDevice.h

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

#ifndef Fennel_Database_Included
#define Fennel_Database_Included

#include "fennel/device/DeviceMode.h"
#include "fennel/common/TraceSource.h"
#include "fennel/common/StatsSource.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/db/DatabaseHeader.h"
#include "fennel/tuple/StandardTypeDescriptor.h"
#include "fennel/segment/SegmentMap.h"
#include "fennel/synch/SynchMonitoredObject.h"

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipantFactory;
class VersionedSegment;
class LinearDeviceSegmentParams;

/**
 * Database defines the top-level database object, which is the unit of
 * self-contained storage and recovery.
 */
class Database
    : public boost::noncopyable,
        public ClosableObject,
        public TraceSource,
        public SegmentMap,
        public StatsSource,
        public SynchMonitoredObject
{
    DeviceId dataDeviceId;

    SharedRandomAccessDevice pDataDevice;

    DeviceId tempDeviceId;

    DeviceId shadowDeviceId;

    DeviceId txnLogDeviceId;

    std::string dataDeviceName;

    std::string tempDeviceName;

    std::string shadowDeviceName;

    std::string txnLogDeviceName;

    SharedCache pCache;

    SharedSegmentFactory pSegmentFactory;

    SharedSegment pHeaderSegment;

    SharedSegment pDataSegment;

    SharedSegment pTempSegment;

    VersionedSegment *pVersionedSegment;

    SharedLogicalTxnLog pTxnLog;

    PageId headerPageId1;

    PageId headerPageId2;

    DatabaseHeader header;

    LogicalTxnParticipantFactory *pTxnParticipantFactory;

    bool forceTxns;

    bool disableSnapshots;

    bool recoveryRequired;

    DeviceMode openMode;

    ConfigMap configMap;

    StandardTypeDescriptorFactory typeFactory;

    SharedCheckpointThread pCheckpointThread;

    SharedPseudoUuidGenerator pUuidGenerator;

    /**
     * Cumulative counter cleared whenever writeStats is called.
     */
    uint nCheckpointsStat;

    /**
     * Cumulative counter.
     */
    uint nCheckpoints;

    /**
     * If true, ALTER SYSTEM DEALLOCATE OLD is a no-op
     */
    bool disableDeallocateOld;

    /**
     * Device used to read and write the backup file during backup and restore
     */
    SharedSegPageBackupRestoreDevice pBackupRestoreDevice;

    /**
     * Scratch accessor used during backup and restore
     */
    SegmentAccessor scratchAccessor;

    explicit Database(
        SharedCache pCache,
        ConfigMap const &configMap,
        DeviceMode openMode,
        SharedTraceTarget pTraceTarget,
        SharedPseudoUuidGenerator pUuidGenerator);

    // implement ClosableObject
    virtual void closeImpl();

// ----------------------------------------------------------------------
// internal helper methods
// ----------------------------------------------------------------------

    void init();

    void createTxnLog(DeviceMode);

    SharedSegment createTxnLogSegment(DeviceMode,PageId);

    SharedSegment createShadowLog(DeviceMode);

    void createDataDevice(LinearDeviceSegmentParams &);

    void createDataSegment(SharedSegment, LinearDeviceSegmentParams &);

    void createTempSegment();

    void allocateHeader();

    void writeHeader();

    void loadHeader(bool);

    void closeDevices();

    void deleteLogs();

    void openSegments();

    void prepareForRecovery();

    void recoverPhysical(CheckpointType);

    void readDeviceParams(
        std::string paramNamePrefix,
        DeviceMode deviceMode,
        LinearDeviceSegmentParams &deviceParams);

    void cleanupBackupRestore(bool isBackup);

public:
    static ParamName paramDatabaseDir;
    static ParamName paramResourceDir;
    static ParamName paramForceTxns;
    static ParamName paramDisableSnapshots;
    static ParamName paramDatabasePrefix;
    static ParamName paramTempPrefix;
    static ParamName paramShadowLogPrefix;
    static ParamName paramTxnLogPrefix;
    static ParamName paramInitSizeSuffix;
    static ParamName paramMaxSizeSuffix;
    static ParamName paramIncSizeSuffix;

    static ParamVal valLogAllocLinear;
    static ParamVal valLogAllocCircular;

    static const SegmentId DEFAULT_DATA_SEGMENT_ID;
    static const SegmentId TEMP_SEGMENT_ID;

    static SharedDatabase newDatabase(
        SharedCache pCache,
        ConfigMap const &configMap,
        DeviceMode openMode,
        SharedTraceTarget pTraceTarget,
        SharedPseudoUuidGenerator pUuidGenerator = SharedPseudoUuidGenerator());

    virtual ~Database();

    const ConfigMap& getConfigMap() const;

    SharedCache getCache() const;

    SharedSegmentFactory getSegmentFactory() const;

    SharedSegment getDataSegment() const;

    SharedSegment getTempSegment() const;

    SharedCheckpointThread getCheckpointThread() const;

    // implement SegmentMap
    virtual SharedSegment getSegmentById(
        SegmentId segmentId,
        SharedSegment pDataSegment);

    // implement StatsSource
    virtual void writeStats(StatsTarget &target);

    SharedLogicalTxnLog getTxnLog() const;

    StoredTypeDescriptorFactory const &getTypeFactory() const;

    bool isRecoveryRequired() const;

    bool shouldForceTxns() const;

    bool areSnapshotsEnabled() const;

    void recoverOnline();

    void recover(
        LogicalTxnParticipantFactory &txnParticipantFactory);

    void checkpointImpl(CheckpointType = CHECKPOINT_FLUSH_ALL);

    /**
     * Receives request for a checkpoint.
     *
     * @param checkpointType must be CHECKPOINT_FLUSH_FUZZY or
     * CHECKPOINT_FLUSH_ALL
     *
     * @param async if true, just schedule checkpoint and return; if false,
     * wait for completion
     */
    void requestCheckpoint(
        CheckpointType checkpointType,
        bool async);

    /**
     * Deallocates old snapshot pages that are no longer referenced by
     * any active transactions, as well as active labels marking snapshots
     * in time.
     *
     * @param oldestLabelCsn the csn of the oldest active label; set to
     * NULL_TXN_ID if there are no active labels
     */
    void deallocateOldPages(TxnId oldestLabelCsn);

    /**
     * Saves the id of the last committed transaction.
     *
     * @param txnId id of the last committed transaction
     */
    void setLastCommittedTxnId(TxnId txnId);

    /**
     * @return the id of the last committed, write transaction
     */
    TxnId getLastCommittedTxnId();

    /**
     * Initiates a backup of the data segment.  Backs up the header pages
     * and the allocation node pages.
     *
     * @param backupFilePathname pathname of the backup file
     * @param checkSpaceRequirements if true, make sure file system space
     * is available to perform the backup of data pages
     * @param spacePadding if non-zero, the amount of padding to add to
     * the space requirements for the backup to proceed
     * @param lowerBoundCsn the lower bound allocation csn that determines
     * which pages need to be backed up; if the lower bound is set to
     * NULL_TXN_ID, then that indicates that there is no lower bound;
     * otherwise, the lower bound is exclusive
     * @param compressionProgram if non-empty string, the name of the program
     * used to compress the backup file
     * @param dataDeviceSize returns the size of the data device in bytes
     * @param aborted reference to a flag indicating whether the backup
     * should be aborted
     *
     * @return the txnId of the last committed write txn at the start of the
     * backup; i.e., the upper bound allocationCsn for the backup
     */
    TxnId initiateBackup(
        const std::string &backupFilePathname,
        bool checkSpaceRequirements,
        FileSize spacePadding,
        TxnId lowerBoundCsn,
        const std::string &compressionProgram,
        FileSize &dataDeviceSize,
        const volatile bool &aborted);

    /**
     * Completes the remainder of a backup by backing up the data pages that
     * have allocationCsn's in between two specified bounds.  If the
     * lower bound is set to NULL_TXN_ID, then that indicates that there is
     * no lower bound; otherwise, the lower bound is exclusive.  There must
     * always be an upper bound and it is inclusive.
     *
     * @param lowerBoundCsn the lower bound allocation csn
     * @param upperBoundCsn the upper bound allocation csn
     * @param aborted reference to a flag indicating whether the backup
     * should be aborted
     */
    void completeBackup(
        TxnId lowerBoundCsn,
        TxnId upperBoundCsn,
        const volatile bool &aborted);

    /**
     * Aborts a backup that may or may not have been previously initiated.
     * This is necessary to perform cleanup on the backup in case an error
     * occurred after the backup was initiated, but before there was a chance
     * to complete it.
     */
    void abortBackup();

    /**
     * Restores the data segment from a backup file.  The data pages in the
     * backup file correspond to pages with allocationCsn's in between a
     * lower and upper bound.  If the lower bound is set to NULL_TXN_ID,
     * then that indicates that there is no lower bound; otherwise, the lower
     * bound is exclusive.  There must always be an upper bound and it is
     * inclusive.
     *
     * @param backupFilePathname pathname of the backup file
     * @param newSize the size the data segment will be extended to by the
     * restore
     * @param compressionProgram if non-empty string, the name of the program
     * used to compress the backup file
     * @param lowerBoundCsn the lower bound allocation csn
     * @param upperBoundCsn the upper bound allocation csn
     * @param aborted reference to a flag indicating whether the restore
     * should be aborted
     */
    void restoreFromBackup(
        const std::string &backupFilePathname,
        FileSize newSize,
        const std::string &compressionProgram,
        TxnId lowerBoundCsn,
        TxnId upperBoundCsn,
        const volatile bool &aborted);
};

FENNEL_END_NAMESPACE

#endif

// End Database.h

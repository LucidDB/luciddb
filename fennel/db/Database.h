/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
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

    bool recoveryRequired;

    DeviceMode openMode;

    ConfigMap configMap;

    StandardTypeDescriptorFactory typeFactory;

    SharedCheckpointThread pCheckpointThread;

    /**
     * Cumulative counter cleared whenever writeStats is called.
     */
    uint nCheckpointsStat;

    /**
     * Cumulative counter.
     */
    uint nCheckpoints;

    explicit Database(
        SharedCache pCache,
        ConfigMap const &configMap,
        DeviceMode openMode,
        TraceTarget *pTraceTarget);
    
    // implement ClosableObject
    virtual void closeImpl();
    
// ----------------------------------------------------------------------
// internal helper methods
// ----------------------------------------------------------------------

    void createTxnLog(DeviceMode);
    
    SharedSegment createTxnLogSegment(DeviceMode,PageId);
    
    SharedSegment createShadowLog(DeviceMode);

    void createDataDevice();
    
    void createDataSegment(SharedSegment);

    void createTempSegment();
    
    void allocateHeader();

    void writeHeader();

    void loadHeader(bool);

    void closeDevices();

    void deleteLogs();

    void openSegments();
    
    void prepareForRecovery();

    void readDeviceParams(
        std::string paramNamePrefix,
        DeviceMode deviceMode,
        LinearDeviceSegmentParams &deviceParams);

public:
    static ParamName paramDatabaseDir;
    static ParamName paramResourceDir;
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
        TraceTarget *pTraceTarget = NULL);
    
    virtual ~Database();

    const ConfigMap& getConfigMap() const;

    SharedCache getCache() const;

    SharedSegmentFactory getSegmentFactory() const;
    
    SharedSegment getDataSegment() const;

    SharedSegment getTempSegment() const;

    SharedCheckpointThread getCheckpointThread() const;

    // implement SegmentMap
    virtual SharedSegment getSegmentById(SegmentId segmentId);

    // implement StatsSource
    virtual void writeStats(StatsTarget &target);
    
    SharedLogicalTxnLog getTxnLog() const;

    StoredTypeDescriptorFactory const &getTypeFactory() const;

    bool isRecoveryRequired() const;

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
};

FENNEL_END_NAMESPACE

#endif

// End Database.h

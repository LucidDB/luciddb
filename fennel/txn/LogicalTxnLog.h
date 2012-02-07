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

#ifndef Fennel_LogicalTxnLog_Included
#define Fennel_LogicalTxnLog_Included

#include <vector>
#include <boost/utility.hpp>
#include <boost/enable_shared_from_this.hpp>
#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/common/PseudoUuid.h"

FENNEL_BEGIN_NAMESPACE

/**
 * LogicalTxnLog defines the log structure used by LogicalTxn to record
 * transaction data.
 */
class FENNEL_TXN_EXPORT LogicalTxnLog
    : public boost::noncopyable,
        public boost::enable_shared_from_this<LogicalTxnLog>,
        public SynchMonitoredObject
{
    friend class LogicalTxn;

    typedef std::vector<SharedLogicalTxn> TxnList;
    typedef std::vector<SharedLogicalTxn>::iterator TxnListIter;

    typedef std::vector<SharedSegment> SegList;
    typedef std::vector<SharedSegment>::iterator SegListIter;

    /**
     * SegmentFactory to use for creating long log segments.
     */
    SharedSegmentFactory pSegmentFactory;

    /**
     * TxnId generator.
     */
    TxnId nextTxnId;

    /**
     * Collection of active txns which have not yet committed.
     */
    TxnList uncommittedTxns;

    /**
     * Log segments for long log transactions which committed after the last
     * checkpoint.
     */
    SegList committedLongLogSegments;

    /**
     * Accessor for segment for the main transaction log.
     */
    SegmentAccessor logSegmentAccessor;

    /**
     * Output stream for writing to the main transaction log.
     */
    SharedSegOutputStream pOutputStream;

    /**
     * Memento recorded at last checkpoint.
     */
    LogicalTxnLogCheckpointMemento lastCheckpointMemento;

    /**
     * Size of committedLongLogSegments at time lastCheckpointMemento was
     * recorded.
     */
    uint nCommittedBeforeLastCheckpoint;

    /**
     * Group commit interval in milliseconds, or 0 for immediate commit.
     */
    uint groupCommitInterval;

    explicit LogicalTxnLog(
        SegmentAccessor const &logSegmentAccessor,
        PseudoUuid const &onlineUuid,
        SharedSegmentFactory pSegmentFactory);

    void removeTxn(SharedLogicalTxn pTxn);

    void commitTxn(SharedLogicalTxn pTxn);

    void rollbackTxn(SharedLogicalTxn pTxn);

    void checkpointTxn(SharedLogicalTxn pTxn);

    void commitTxnWithGroup(StrictMutexGuard &mutexGuard);

public:

    /**
     * Creates a new LogicalTxnLog in the given segment.
     *
     * @param logSegmentAccessor accessor for segment to contain log data
     *
     * @param onlineUuid UUID associated with log instance
     *
     * @param pSegmentFactory SegmentFactory to use for creating long log
     * segments
     *
     * @return shared_ptr to new LogicalTxnLog
     */
    static SharedLogicalTxnLog newLogicalTxnLog(
        SegmentAccessor const &logSegmentAccessor,
        PseudoUuid const &onlineUuid,
        SharedSegmentFactory pSegmentFactory);

    virtual ~LogicalTxnLog();

    /**
     * Sets the nextTxnId.
     *
     * @param nextTxnIdInit nextTxnId to be set
     */
    void setNextTxnId(TxnId nextTxnIdInit);

    /**
     * Starts a new LogicalTxn.
     *
     * @param pCacheAccessor CacheAccessor to use for cache access to log data
     *
     * @return shared_ptr to new LogicalTxn
     */
    SharedLogicalTxn newLogicalTxn(
        SharedCacheAccessor pCacheAccessor);

    /**
     * Checkpoints all active transactions. Assumes that all
     * transactions have already been quiesced for the duration of the
     * checkpoint.  In most cases, deallocateCheckpointedLog must also
     * be called after the checkpoint.
     *
     * @param memento receives the checkpoint record
     *
     * @param checkpointType type of checkpoint to execute
     */
    void checkpoint(
        LogicalTxnLogCheckpointMemento &memento,
        CheckpointType checkpointType = CHECKPOINT_FLUSH_ALL);

    /**
     * Releases log space used by transactions which committed before current
     * checkpoint.  Divorced from the checkpoint operation itself to
     * make possible atomicity as part of compound checkpoint sequences.
     *
     * @param memento memento returned by last checkpoint() call
     *
     * @param checkpointType type of checkpoint passed to the last
     * checkpoint() call
     */
    void deallocateCheckpointedLog(
        LogicalTxnLogCheckpointMemento const &memento,
        CheckpointType checkpointType = CHECKPOINT_FLUSH_ALL);

    /**
     * Returns the transaction id of the oldest, active transaction
     *
     * @return txnId of the oldest, active txn; if there are no active
     * transactions, returns the current transaction id
     */
    TxnId getOldestActiveTxnId();
};

FENNEL_END_NAMESPACE

#endif

// End LogicalTxnLog.h

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
class LogicalTxnLog
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

    explicit LogicalTxnLog(
        SegmentAccessor const &logSegmentAccessor,
        PseudoUuid const &onlineUuid,
        SharedSegmentFactory pSegmentFactory);

    void removeTxn(SharedLogicalTxn pTxn);
    
    void commitTxn(SharedLogicalTxn pTxn);

    void rollbackTxn(SharedLogicalTxn pTxn);

    void checkpointTxn(SharedLogicalTxn pTxn);

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
};

FENNEL_END_NAMESPACE

#endif

// End LogicalTxnLog.h

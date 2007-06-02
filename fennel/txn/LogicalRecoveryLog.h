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

#ifndef Fennel_LogicalRecoveryLog_Included
#define Fennel_LogicalRecoveryLog_Included

#include "fennel/txn/LogicalTxnStoredStructs.h"
#include "fennel/common/PseudoUuid.h"

#include <boost/utility.hpp>
#include <hash_map>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;
class LogicalTxnParticipantFactory;

/**
 * LogicalRecoveryLog is the recovery-time counterpart to the online
 * LogicalTxnLog.
 */
class LogicalRecoveryLog
    : public boost::noncopyable
{
    typedef std::hash_map<TxnId,LogicalTxnEventMemento> TxnMap;
    typedef TxnMap::iterator TxnMapIter;

    TxnMap checkpointTxnMap;

    LogicalTxnParticipantFactory &participantFactory;
    
    SharedSegmentFactory pSegmentFactory;

    SegmentAccessor logSegmentAccessor;

    SharedSegInputStream pInputStream;

    explicit LogicalRecoveryLog(
        LogicalTxnParticipantFactory &participantFactory,
        SegmentAccessor const &logSegmentAccessor,
        PseudoUuid const &onlineUuid,
        SharedSegmentFactory pSegmentFactory);

    void redoTxn(
        LogicalTxnEventMemento const &commitMemento,
        LogicalTxnEventMemento const *pCheckpointMemento,
        SharedSegInputStream pTxnInputStream);
    
    void undoTxn(
        LogicalTxnEventMemento const &checkpointMemento,
        SharedSegInputStream pTxnInputStream);

    SharedSegInputStream openLongLogStream(TxnId txnId);

public:
    /**
     * Opens a LogicalRecoveryLog stored in the given
     * segment.
     *
     * @param participantFactory factory for reloading instances of
     * LogicalTxnParticipant
     *
     * @param logSegmentAccessor accessor for segment containing log data
     *
     * @param onlineUuid UUID associated with log instance while it was online
     *
     * @param pSegmentFactory SegmentFactory to use for accessing long log
     * segments
     *
     * @return shared_ptr to new LogicalRecoveryLog
     */
    static SharedLogicalRecoveryLog newLogicalRecoveryLog(
        LogicalTxnParticipantFactory &participantFactory,
        SegmentAccessor const &logSegmentAccessor,
        PseudoUuid const &onlineUuid,
        SharedSegmentFactory pSegmentFactory);
        
    virtual ~LogicalRecoveryLog();
    void recover(LogicalTxnLogCheckpointMemento const &memento);
    
    /**
     * Gets the relative filename for a long transaction log.  This is
     * deterministic based on the TxnId.
     *
     * @param txnId the TxnId of the transaction
     */
    static std::string getLongLogFileName(TxnId txnId);
};

FENNEL_END_NAMESPACE

#endif

// End LogicalRecoveryLog.h

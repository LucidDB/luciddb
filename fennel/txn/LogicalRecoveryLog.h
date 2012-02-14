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
class FENNEL_TXN_EXPORT LogicalRecoveryLog
    : public boost::noncopyable
{
    typedef std::hash_map<TxnId, LogicalTxnEventMemento> TxnMap;
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

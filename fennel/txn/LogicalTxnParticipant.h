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

#ifndef Fennel_LogicalTxnParticipant_Included
#define Fennel_LogicalTxnParticipant_Included

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * LogicalTxnParticipant defines an interface which must be implemented by any
 * object which is to participate in a LogicalTxn.
 */
class FENNEL_TXN_EXPORT LogicalTxnParticipant
{
    friend class LogicalTxn;

    LogicalTxn *pTxn;

    bool loggingEnabled;

    void clearLogicalTxn();

    void enableLogging(bool);

protected:
    explicit LogicalTxnParticipant();

    /**
     * @return LogicalTxn being participated in, or null if no txn in progress;
     * null is also returned during recovery
     */
    LogicalTxn *getLogicalTxn();

    /**
     * @return true if actions should be logged; this is false during recovery
     */
    bool isLoggingEnabled() const;

public:
    virtual ~LogicalTxnParticipant();

    /**
     * @return the LogicalTxnClassId for this participant; this will
     * be used during recovery to find the correct LogicalTxnParticipantFactory
     */
    virtual LogicalTxnClassId getParticipantClassId() const = 0;

    /**
     * Called by LogicalTxn the first time an action is logged for this
     * participant.  The participant must implement this by writing a
     * description of itself to the given output stream.  This information must
     * be sufficient for reconstructing the participant during recovery.
     *
     * @param logStream stream to which the participant description should be
     * written
     */
    virtual void describeParticipant(
        ByteOutputStream &logStream) = 0;

    /**
     * Performs undo for one logical action during rollback or recovery.  The
     * implementation must consume ALL log data for this action, even if some
     * of it turns out to be unneeded.
     *
     * @param actionType the type of action to undo; the rest of the action
     * parameters should be read from the LogicalTxn's input stream
     *
     * @param logStream stream from which to read action data
     */
    virtual void undoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream) = 0;

    /**
     * Performs redo for one logical action during recovery.  The
     * implementation must consume ALL log data for this action, even if some
     * of it turns out to be unneeded.
     *
     * @param actionType the type of action to redo; the rest of the action
     * parameters should be read from the LogicalTxn's input stream
     *
     * @param logStream stream from which to read action data
     */
    virtual void redoLogicalAction(
        LogicalActionType actionType,
        ByteInputStream &logStream) = 0;
};

inline LogicalTxn *LogicalTxnParticipant::getLogicalTxn()
{
    return pTxn;
}

inline bool LogicalTxnParticipant::isLoggingEnabled() const
{
    return loggingEnabled;
}

FENNEL_END_NAMESPACE

#endif

// End LogicalTxnParticipant.h

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
*/

#ifndef Fennel_LogicalTxnParticipant_Included
#define Fennel_LogicalTxnParticipant_Included

#include <boost/utility.hpp>

FENNEL_BEGIN_NAMESPACE

/**
 * LogicalTxnParticipant defines an interface which must be implemented by any
 * object which is to participate in a LogicalTxn.
 */
class LogicalTxnParticipant
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

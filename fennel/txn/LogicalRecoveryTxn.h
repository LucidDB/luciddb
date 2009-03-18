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

#ifndef Fennel_LogicalRecoveryTxn_Included
#define Fennel_LogicalRecoveryTxn_Included

#include "fennel/common/VoidPtrHash.h"

#include <boost/utility.hpp>

#include <hash_map>

FENNEL_BEGIN_NAMESPACE

class LogicalTxnParticipant;
class LogicalTxnParticipantFactory;
struct LogicalTxnSavepoint;

/**
 * LogicalRecoveryTxn implements recovery for transactions previously logged
 * via LogicalTxn.
 */
class LogicalRecoveryTxn
    : public boost::noncopyable
{
    typedef std::hash_map<
        LogicalTxnParticipant *,
        SharedLogicalTxnParticipant,
        VoidPtrHash> ParticipantMap;
    typedef ParticipantMap::iterator ParticipantMapIter;

    /**
     * Stream from which to read log data.
     */
    SharedByteInputStream pTxnInputStream;

    /**
     * Factory for recovering txn participants, or NULL if online recovery
     * being performed.
     */
    LogicalTxnParticipantFactory *pParticipantFactory;

    /**
     * Swizzling map from logged participant to recovered participant.
     */
    ParticipantMap participantMap;

    bool isOnline() const
    {
        return pParticipantFactory ? false : true;
    }

    void recoverParticipant(
        LogicalTxnParticipant *pLoggedParticipant);

    LogicalTxnParticipant *swizzleParticipant(
        LogicalTxnParticipant *pParticipant);

public:
    /**
     * Constructor.
     *
     * @param pTxnInputStream stream for reading transaction log
     *
     * @param pParticipantFactory if NULL, online recovery is being performed,
     * and logged participants can be accessed directly; if non-NULL, logged
     * participants must be recovered via this factory
     */
    explicit LogicalRecoveryTxn(
        SharedByteInputStream pTxnInputStream,
        LogicalTxnParticipantFactory *pParticipantFactory);

    virtual ~LogicalRecoveryTxn();

    /**
     * Performs redo for actions from the log in their original logged order.
     *
     * @param cbRedo number of log bytes to read; redo stops after this
     */
    void redoActions(uint cbRedo);

    /**
     * Performs undo for actions from the log in reverse order.  The undo may
     * be limited by nActionsMax or minActionOffset, but not both.
     *
     * @param svptEnd savepoint for log position just after first action
     * to be undone
     *
     * @param nActionsMax limit on number of actions to undo, or MAXU for no
     * limit
     *
     * @param minActionOffset log position at which to stop rolling back, or 0
     * for the entire log
     */
    void undoActions(
        LogicalTxnSavepoint const &svptEnd,
        uint nActionsMax = MAXU,
        FileSize minActionOffset = 0);
};

FENNEL_END_NAMESPACE

#endif

// End LogicalRecoveryTxn.h

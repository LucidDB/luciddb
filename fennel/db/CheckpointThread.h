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

#ifndef Fennel_CheckpointThread_Included
#define Fennel_CheckpointThread_Included

#include "fennel/synch/Thread.h"
#include "fennel/synch/SXMutex.h"
#include "fennel/common/ClosableObject.h"
#include "fennel/segment/CheckpointProvider.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CheckpointThread is dedicated to listening for checkpoint requests and
 * carrying them out.
 */
class CheckpointThread :
    public Thread, public SynchMonitoredObject, public ClosableObject,
    public CheckpointProvider
{
    Database &database;
    SXMutex actionMutex;
    CheckpointType checkpointType;
    bool quit;

    /**
     * Implements ClosableObject by requesting that the checkpoint thread shut
     * itself down.
     */
    void closeImpl();
    
    // implement Thread
    virtual void run();
    
public:
    /**
     * Creates a checkpoint thread for the given database (no more than
     * one is ever needed).  This constructor does not start the thread;
     * that must be done explicitly.
     *
     * @param database the Database to checkpoint when requested
     */
    explicit CheckpointThread(Database &database);

    /**
     * Gets the action mutex.  The checkpoint thread takes an exclusive lock on
     * this mutex for the duration of each checkpoint, so any thread which
     * needs to carry out an action which must not overlap a checkpoint
     * should take a shared lock on this for the duration of the action.
     *
     * @return the mutex
     */
    SXMutex &getActionMutex();

    /**
     * Implements CheckpointProvider by signalling the checkpoint thread, which
     * in response will quiesce the system and carry out a checkpoint.
     *
     * @param checkpointType type of checkpoint to request
     */
    virtual void requestCheckpoint(CheckpointType checkpointType);
};

FENNEL_END_NAMESPACE

#endif

// End CheckpointThread.h

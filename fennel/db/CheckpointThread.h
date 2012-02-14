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
class FENNEL_DB_EXPORT CheckpointThread
    : public Thread, public SynchMonitoredObject, public ClosableObject,
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

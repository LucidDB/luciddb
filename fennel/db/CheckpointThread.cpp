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

#include "fennel/common/CommonPreamble.h"
#include "fennel/db/CheckpointThread.h"
#include "fennel/db/Database.h"

FENNEL_BEGIN_CPPFILE("$Id$");

CheckpointThread::CheckpointThread(Database &databaseInit)
    : database(databaseInit)
{
    actionMutex.setSchedulingPolicy(SXMutex::SCHEDULE_FAVOR_EXCLUSIVE);
    checkpointType = CHECKPOINT_DISCARD;
    quit = false;
}

void CheckpointThread::run()
{
    for (;;) {
        StrictMutexGuard mutexGuard(mutex);
        while ((checkpointType == CHECKPOINT_DISCARD) && !quit) {
            condition.wait(mutexGuard);
        }
        if (quit) {
            return;
        }

        // NOTE jvs 28-Feb-2006:  reset checkpointType here; we used
        // to do it after checkpoint completion, but that led to a
        // race condition whereby we might miss a new checkpoint request
        // by overwriting it.
        CheckpointType currentType = checkpointType;
        checkpointType = CHECKPOINT_DISCARD;
        mutexGuard.unlock();

        SXMutexExclusiveGuard actionMutexGuard(actionMutex);
        database.checkpointImpl(currentType);
        actionMutexGuard.unlock();
    }
}

SXMutex &CheckpointThread::getActionMutex()
{
    return actionMutex;
}

void CheckpointThread::closeImpl()
{
    StrictMutexGuard mutexGuard(mutex);

    if (!isStarted()) {
        return;
    }

    quit = true;
    condition.notify_all();
    mutexGuard.unlock();

    join();
}

void CheckpointThread::requestCheckpoint(CheckpointType request)
{
    StrictMutexGuard mutexGuard(mutex);
    switch (request) {
    case CHECKPOINT_FLUSH_ALL:
        checkpointType = request;
        break;
    case CHECKPOINT_FLUSH_FUZZY:
        if (checkpointType == CHECKPOINT_DISCARD) {
            checkpointType = request;
        }
        break;
    default:
        permAssert(false);
    }
    condition.notify_all();
}

FENNEL_END_CPPFILE("$Id$");

// End CheckpointThread.cpp

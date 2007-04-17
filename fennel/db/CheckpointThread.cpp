/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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
    switch(request) {
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

/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
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
#include "fennel/synch/ThreadPool.h"
#include "fennel/synch/Thread.h"

FENNEL_BEGIN_CPPFILE("$Id$");

/**
 * PooledThread is a Thread working for a ThreadPool.
 */
class PooledThread : public Thread
{
    ThreadPoolBase &pool;
    
public:
    explicit PooledThread(ThreadPoolBase &poolInit)
        : pool(poolInit)
    {
    }

    virtual void run()
    {
        pool.runPooledThread();
    }
};

ThreadPoolBase::ThreadPoolBase()
{
    state = STATE_STOPPED;
}

ThreadPoolBase::~ThreadPoolBase()
{
    assert(state == STATE_STOPPED);
}

void ThreadPoolBase::start(uint nThreads)
{
    StrictMutexGuard guard(mutex);
    assert(state == STATE_STOPPED);
    assert(nThreads > 0);
    state = STATE_STARTED;
    for (uint i = 0; i < nThreads; ++i) {
        PooledThread *pThread = new PooledThread(*this);
        pThread->start();
        threads.push_back(pThread);
    }
}

void ThreadPoolBase::stop()
{
    StrictMutexGuard guard(mutex);
    assert(state != STATE_STOPPING);
    if (state == STATE_STOPPED) {
        return;
    }
    state = STATE_STOPPING;

    while (!isQueueEmpty()) {
        stoppingCondition.wait(guard);
    }

    state = STATE_STOPPED;
    condition.notify_all();
    guard.unlock();
    
    for (uint i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }

    guard.lock();
    for (uint i = 0; i < threads.size(); ++i) {
        deleteAndNullify(threads[i]);
    }
    threads.clear();
}

void ThreadPoolBase::runPooledThread()
{
    StrictMutexGuard guard(mutex);
    for (;;) {
        while ((state != STATE_STOPPED) && isQueueEmpty()) {
            condition.wait(guard);
        }
        if (state == STATE_STOPPED) {
            return;
        }
        runOneTask(guard);
        stoppingCondition.notify_one();
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ThreadPool.cpp

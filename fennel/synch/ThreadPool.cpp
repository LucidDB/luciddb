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
#include "fennel/synch/ThreadPool.h"
#include "fennel/synch/Thread.h"
#include "fennel/synch/ThreadTracker.h"

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
    // TODO jvs 28-Jul-2008:  resource acquisition as initialization
    if (pThreadTracker) {
        pThreadTracker->onThreadStart();
    }
    try {
        StrictMutexGuard guard(mutex);
        for (;;) {
            while ((state != STATE_STOPPED) && isQueueEmpty()) {
                condition.wait(guard);
            }
            if (state == STATE_STOPPED) {
                break;
            }
            runOneTask(guard);
            stoppingCondition.notify_one();
        }
    } catch (...) {
        if (pThreadTracker) {
            pThreadTracker->onThreadEnd();
        }
        throw;
    }
    if (pThreadTracker) {
        pThreadTracker->onThreadEnd();
    }
}

void ThreadPoolBase::setThreadTracker(ThreadTracker &threadTracker)
{
    pThreadTracker = &threadTracker;
}

FENNEL_END_CPPFILE("$Id$");

// End ThreadPool.cpp

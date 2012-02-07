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

#ifndef Fennel_ThreadPool_Included
#define Fennel_ThreadPool_Included

#include <deque>
#include <vector>
#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/synch/Thread.h"

FENNEL_BEGIN_NAMESPACE

class PooledThread;
class ThreadTracker;

/**
 * ThreadPoolBase defines the non-templated portion of ThreadPool.
 */
class FENNEL_SYNCH_EXPORT ThreadPoolBase : protected SynchMonitoredObject
{
    friend class PooledThread;
    void runPooledThread();

protected:
    enum State {
        STATE_STARTED,
        STATE_STOPPING,
        STATE_STOPPED
    };

    std::vector<PooledThread *> threads;
    State state;
    LocalCondition stoppingCondition;
    ThreadTracker *pThreadTracker;

    explicit ThreadPoolBase();
    virtual ~ThreadPoolBase();
    virtual bool isQueueEmpty() = 0;
    virtual void runOneTask(StrictMutexGuard &) = 0;

public:
    /**
     * Starts the given number of threads in the pool.
     *
     * @param nThreads number of threads to start
     */
    void start(uint nThreads);

    /**
     * Shuts down the pool, waiting for any pending tasks to complete.
     * The start/stop calls should never be invoked from more than one thread
     * simultaneously.
     */
    void stop();

    /**
     * Sets a tracker to use for created threads.
     *
     * @param threadTracker tracker to use
     */
    void setThreadTracker(ThreadTracker &threadTracker);
};

/**
 * ThreadPool is a very simple thread-pooling implementation.  It's a template
 * to avoid requiring task queue entries to be dynamically allocated.
 *
 *<p>
 *
 * The Task template parameter must behave as a concrete data type, and
 * must have a method execute().
 */
template <class Task>
class ThreadPool : public ThreadPoolBase
{
    std::deque<Task> queue;

    virtual bool isQueueEmpty()
    {
        return queue.empty();
    }

    virtual void runOneTask(StrictMutexGuard &guard)
    {
        Task task = queue.front();
        queue.pop_front();
        guard.unlock();
        task.execute();
        guard.lock();
    }

public:
    /**
     * Constructor.
     */
    explicit ThreadPool()
    {
        pThreadTracker = NULL;
    }

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~ThreadPool()
    {
    }

    /**
     * Submits a task to the pool.  It will be executed as soon as a thread is
     * available.
     *
     * @param task the task to execute, expressed as a function object
     */
    void submitTask(Task &task)
    {
        StrictMutexGuard guard(mutex);
        assert(state == STATE_STARTED);
        queue.push_back(task);
        condition.notify_one();
    }
};

FENNEL_END_NAMESPACE

#endif

// End ThreadPool.h

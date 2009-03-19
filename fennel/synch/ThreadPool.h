/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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
class ThreadPoolBase : protected SynchMonitoredObject
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

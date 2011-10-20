/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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
#include <algorithm>
#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/synch/Thread.h"

FENNEL_BEGIN_NAMESPACE

class PooledThread;
class ThreadTracker;

/**
 * ThreadPoolBase defines the non-templated part of AbstractThreadPool.
 */
class FENNEL_SYNCH_EXPORT ThreadPoolBase : protected SynchMonitoredObject
{
    friend class PooledThread;
    void runPooledThread();

protected:
    enum State {
        STATE_INITIAL,
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
    Thread* getPooledThread(uint index);
    virtual bool isQueueEmpty() const = 0;
    virtual void clearQueue() = 0;
    virtual void runOneTask(StrictMutexGuard &) = 0;

    /**
     * wait while queue is empty (or not empty)
     * @param val true means wait while empty, false means wait until empty
     */
    virtual void waitWhileQueueEmpty(bool val);

public:
    /**
     * Starts the given number of threads in the pool.
     *
     * @param nThreads number of threads to start
     */
    virtual void start(uint nThreads);

    /**
     * Stops the pool.
     *
     * @param soft true for soft stop, false for a hard stop.
     *
     * A hard stop is almost immediate: no thread will start a new task. A soft
     * stop wait for any pending tasks to complete. After a hard stop, there may
     * still be unexecuted tasks in the queue. The start/stop calls should never
     * be invoked from more than one thread simultaneously.
     */
    virtual void stop(bool soft = true);

    /**
     * Sets a tracker to use for created threads.
     *
     * @param threadTracker tracker to use
     */
    void setThreadTracker(ThreadTracker &threadTracker);
};

/**
 * AbstractThreadPool is a templated extension of ThreadPoolBase. It's a
 * template to avoid requiring task queue entries to be dynamically allocated.
 * It is abstract: an implementing subclass provides the crucial method
 * doTask().
 */
template <class Task>
class AbstractThreadPool : public ThreadPoolBase
{
    std::deque<Task> queue;

protected:
    virtual void doTask(Task& k) = 0;
    virtual void runOneTask(StrictMutexGuard &guard)
    {
        Task task = queue.front();
        queue.pop_front();
        guard.unlock();
        doTask(task);
        guard.lock();
    }

    virtual size_t queueSize() const {
        return queue.size();
    }

    virtual bool isQueueEmpty() const {
        return queue.empty();
    }

    virtual void clearQueue() {
        queue.clear();
    }

    /**
     * Constructor.
     */
    explicit AbstractThreadPool() {
        pThreadTracker = NULL;
    }

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~AbstractThreadPool() {
    }

    /**
     * Applies a functor to all Tasks in the queue
     */
    template <class Functor>
    void mapQueue(Functor& f) {
        StrictMutexGuard guard(mutex);
        std::for_each(queue.begin(), queue.end(), f);
    }

public:

    /**
     * Adds a task to the tail of the queue. It will be executed in turn, as
     * soon as a thread is available.
     *
     * @param task the task to execute
     * @return true when the task is accepted, false when rejected.
     */
    bool submitTask(Task &task) {
        StrictMutexGuard guard(mutex);
        if (state == STATE_STARTED || state == STATE_INITIAL) {
            queue.push_back(task);
            condition.notify_one();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Adds a task to the head of the queue. It will be executed next, as soon
     * as a thread is available.
     *
     * @param task the task to execute
     * @return true when the task is accepted, false when rejected.
     */
    bool submitTaskAtHead(Task &task) {
        StrictMutexGuard guard(mutex);
        if (state == STATE_STARTED || state == STATE_INITIAL) {
            queue.push_front(task);
            condition.notify_one();
            return true;
        } else {
            return false;
        }
    }

};


/**
 * ThreadPool is a very simple thread-pooling implementation.
 * The template parameter Task must behave as a concrete data type, and
 * must have a method execute().
 */
template <class Task>
class ThreadPool : public AbstractThreadPool<Task>
{
protected:
    virtual void doTask(Task& k) {
        k.execute();
    }
public:
    explicit ThreadPool() {
    }
    virtual ~ThreadPool() {
    }
};

FENNEL_END_NAMESPACE

#endif

// End ThreadPool.h

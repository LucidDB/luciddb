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

#ifndef Fennel_Thread_Included
#define Fennel_Thread_Included

#include "fennel/synch/SynchObj.h"
#include <boost/utility.hpp>

namespace boost
{
class thread;
};

FENNEL_BEGIN_NAMESPACE

/**
 * Thread is a wrapper around boost::thread which allows for the thread object
 * to be created before it is actually started.
 */
class FENNEL_SYNCH_EXPORT Thread : public boost::noncopyable
{
protected:
    boost::thread *pBoostThread;
    bool bRunning;
    std::string name;

    void initAndRun();
    virtual void run() = 0;
    virtual void beforeRun();
    virtual void afterRun();

public:
    explicit Thread(std::string const &description = "anonymous thread");
    virtual ~Thread();

    /**
     * Spawns the OS thread.
     */
    virtual void start();

    /**
     * Waits for the OS thread to terminate.
     */
    void join();

    /**
     * @return true if start has been called (and subsequent join has not
     * completed)
     */
    bool isStarted() const
    {
        return pBoostThread ? true : false;
    }

    /**
     * @return opposite of isStarted()
     */
    bool isStopped() const
    {
        return !isStarted();
    }

    /**
     * Accesses the underlying boost::thread, e.g. for use in a
     * boost::thread_group.  This thread must already be started.
     *
     * @return the underlying boost::thread
     */
    boost::thread &getBoostThread()
    {
        assert(isStarted());
        return *pBoostThread;
    }

    std::string getName()
    {
        return name;
    }

    void setName(std::string const &s)
    {
        name = s;
    }
};

FENNEL_END_NAMESPACE

#endif

// End Thread.h

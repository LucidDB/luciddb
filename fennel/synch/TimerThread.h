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

#ifndef Fennel_TimerThread_Included
#define Fennel_TimerThread_Included

#include "fennel/synch/Thread.h"
#include "fennel/synch/SynchMonitoredObject.h"
#include "fennel/synch/ThreadTracker.h"

FENNEL_BEGIN_NAMESPACE

/**
 * TimerThreadClient receives callbacks from a TimerThread.
 */
class FENNEL_SYNCH_EXPORT TimerThreadClient : public ThreadTracker
{
public:
    virtual ~TimerThreadClient();

    /**
     * Calculates the interval which should elapse before the next call to
     * onTimerInterval.  This can be different each time.  A return value of 0
     * will cause the TimerThread to cease calling back.
     */
    virtual uint getTimerIntervalMillis() = 0;

    /**
     * Receives notification from TimerThread that interval has elapsed.
     */
    virtual void onTimerInterval() = 0;
};

/**
 * TimerThread implements a timer callback via a dedicated thread.  Once
 * started, the thread runs until stop() is called.
 */
class FENNEL_SYNCH_EXPORT TimerThread
    : public Thread, private SynchMonitoredObject
{
    TimerThreadClient &client;
    bool bStop;

    virtual void run();

public:
    explicit TimerThread(
        TimerThreadClient &clientInit);

    /**
     * Stops (and joins) the timer thread.
     */
    void stop();

    /**
     * Requests an immediate execution of onTimerInterval() in the timer thread
     * context.  Afterwards, timed execution resumes as usual.
     */
    void signalImmediate();
};

FENNEL_END_NAMESPACE

#endif

// End TimerThread.h

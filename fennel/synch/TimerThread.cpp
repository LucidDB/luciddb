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
#include "fennel/synch/TimerThread.h"

FENNEL_BEGIN_CPPFILE("$Id$");

TimerThread::TimerThread(
    TimerThreadClient &clientInit)
    : Thread("TimerThread"),
      client(clientInit)
{
    bStop = false;
}

void TimerThread::run()
{
    // TODO jvs 13-Oct-2006:  resource acquisition as initialization
    client.onThreadStart();
    try {
        for (;;) {
            uint millis = client.getTimerIntervalMillis();
            if (!millis) {
                break;
            }
            boost::xtime atv;
            convertTimeout(millis, atv);
            StrictMutexGuard mutexGuard(mutex);
            while (!bStop) {
                if (!condition.timed_wait(mutexGuard, atv)) {
                    break;
                }
            }
            if (bStop) {
                break;
            }
            client.onTimerInterval();
        }
    } catch (...) {
        client.onThreadEnd();
        throw;
    }
    client.onThreadEnd();
}

void TimerThread::stop()
{
    StrictMutexGuard mutexGuard(mutex);
    if (bStop || !isStarted()) {
        return;
    }
    bStop = true;
    condition.notify_all();
    mutexGuard.unlock();
    join();
    mutexGuard.lock();
    bStop = false;
}

void TimerThread::signalImmediate()
{
    StrictMutexGuard mutexGuard(mutex);
    condition.notify_all();
}

TimerThreadClient::~TimerThreadClient()
{
}

FENNEL_END_CPPFILE("$Id$");

// End TimerThread.cpp

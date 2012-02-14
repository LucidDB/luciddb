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

#ifndef Fennel_AioSignalScheduler_Included
#define Fennel_AioSignalScheduler_Included

#ifdef USE_AIO_H

#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/synch/SynchObj.h"

#include <aio.h>

#include <signal.h>
#include <vector>

FENNEL_BEGIN_NAMESPACE

class AioSignalHandlerThread;

/**
 * AioSignalScheduler implements DeviceAccessScheduler via Unix aio calls and
 * threads which run a signal handler.
 */
class FENNEL_DEVICE_EXPORT AioSignalScheduler
    : public DeviceAccessScheduler
{
    friend class AioSignalHandlerThread;

    StrictMutex mutex;
    LocalCondition quitCondition;
    struct sigaction saOld;
    bool quit;
    std::vector<AioSignalHandlerThread *> threads;

// REVIEW: maybe change Thread from a wrapper to a derived class, and use a
// boost::thread_group?

    bool isStarted() const
    {
        return !threads.empty();
    }

public:
    /**
     * Constructor.
     */
    explicit AioSignalScheduler(
        DeviceAccessSchedulerParams const &);

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~AioSignalScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual bool schedule(RandomAccessRequest &request);
    virtual void stop();
};

FENNEL_END_NAMESPACE

#endif

#endif

// End AioSignalScheduler.h

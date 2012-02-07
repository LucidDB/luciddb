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

#ifndef Fennel_AioPollingScheduler_Included
#define Fennel_AioPollingScheduler_Included

#ifdef USE_AIO_H

#include "fennel/synch/Thread.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"
#include <vector>

#include <aio.h>
struct aiocb;

FENNEL_BEGIN_NAMESPACE

/**
 * AioPollingScheduler implements DeviceAccessScheduler via Unix aio calls and
 * threads which poll for completion.
 */
class FENNEL_DEVICE_EXPORT AioPollingScheduler
    : public DeviceAccessScheduler, public Thread
{
    StrictMutex mutex;
    LocalCondition newRequestPending;
    bool quit;

    std::vector<aiocb *> currentRequests;
    std::vector<aiocb *> newRequests;

public:
    /**
     * Constructor.
     */
    explicit AioPollingScheduler(
        DeviceAccessSchedulerParams const &);

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~AioPollingScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual bool schedule(RandomAccessRequest &request);
    virtual void stop();

// ----------------------------------------------------------------------
// Implementation of Thread interface (q.v.)
// ----------------------------------------------------------------------
    virtual void run();
};

FENNEL_END_NAMESPACE

#endif

#endif

// End AioPollingScheduler.h

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

#ifndef Fennel_ThreadPoolScheduler_Included
#define Fennel_ThreadPoolScheduler_Included

#include "fennel/synch/ThreadPool.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ThreadPoolScheduler implements DeviceAccessScheduler by combining
 * a thread pool with synchronous I/O calls.
 */
class FENNEL_DEVICE_EXPORT ThreadPoolScheduler
    : public DeviceAccessScheduler
{
    ThreadPool<RandomAccessRequest> pool;

public:
    /**
     * Constructor.
     */
    explicit ThreadPoolScheduler(DeviceAccessSchedulerParams const &);

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~ThreadPoolScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual bool schedule(RandomAccessRequest &request);
    virtual void stop();
};

FENNEL_END_NAMESPACE

#endif

// End ThreadPoolScheduler.h

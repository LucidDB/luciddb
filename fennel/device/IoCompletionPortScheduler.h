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

#ifndef Fennel_IoCompletionPortScheduler_Included
#define Fennel_IoCompletionPortScheduler_Included

#ifdef __MSVC__

#include <vector>
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"

#include <windows.h>

FENNEL_BEGIN_NAMESPACE

class IoCompletionPortThread;

/**
 * IoCompletionPortScheduler implements DeviceAccessScheduler via
 * the Win32 IoCompletionPort facility.
 */
class FENNEL_DEVICE_EXPORT IoCompletionPortScheduler
    : public DeviceAccessScheduler
{
    friend class IoCompletionPortThread;

    HANDLE hCompletionPort;
    std::vector<IoCompletionPortThread *> threads;
    bool quit;

    bool isStarted() const
    {
        return !threads.empty();
    }

public:
    /**
     * Constructor.
     */
    explicit IoCompletionPortScheduler(DeviceAccessSchedulerParams const &);

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~IoCompletionPortScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual void registerDevice(SharedRandomAccessDevice pDevice);
    virtual bool schedule(RandomAccessRequest &request);
    virtual void stop();
};

FENNEL_END_NAMESPACE

#endif

#endif

// End IoCompletionPortScheduler.h

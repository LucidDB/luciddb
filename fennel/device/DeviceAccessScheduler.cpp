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
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/device/ThreadPoolScheduler.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/common/FennelResource.h"

#ifdef __MSVC__
#include "fennel/device/IoCompletionPortScheduler.h"
#include "fennel/common/SysCallExcn.h"
#include <windows.h>
#else
#include <dlfcn.h>
#endif

#ifdef USE_AIO_H
#include "fennel/device/AioPollingScheduler.h"
#include "fennel/device/AioSignalScheduler.h"
#endif

FENNEL_BEGIN_CPPFILE("$Id$");

#ifdef USE_LIBAIO_H
static DeviceAccessScheduler *dlopenAioLinuxScheduler(
    DeviceAccessSchedulerParams const &params)
{
    // TODO jvs 4-Sept-2006:  add corresponding dlclose if anyone cares
    void *hLib = dlopen("libfennel_device_aio.so", RTLD_NOW | RTLD_GLOBAL);
    if (!hLib) {
        return NULL;
    }
    void *pFactory = dlsym(hLib, "newAioLinuxScheduler");
    if (!pFactory) {
        return NULL;
    }
    typedef DeviceAccessScheduler *(*PDeviceAccessSchedulerFactory)(
        DeviceAccessSchedulerParams const &);
    PDeviceAccessSchedulerFactory pSchedulerFactory =
        (PDeviceAccessSchedulerFactory) pFactory;
    return (*pSchedulerFactory)(params);
}
#endif

DeviceAccessScheduler *
DeviceAccessScheduler::newScheduler(
    DeviceAccessSchedulerParams const &params)
{
    switch (params.schedulerType) {
    case DeviceAccessSchedulerParams::THREAD_POOL_SCHEDULER:
        return new ThreadPoolScheduler(params);

#ifdef __MSVC__
    case DeviceAccessSchedulerParams::IO_COMPLETION_PORT_SCHEDULER:
        return new IoCompletionPortScheduler(params);
#endif

#ifdef USE_LIBAIO_H
    case DeviceAccessSchedulerParams::AIO_LINUX_SCHEDULER:
        {
            DeviceAccessScheduler *pScheduler = dlopenAioLinuxScheduler(params);
            if (pScheduler) {
                return pScheduler;
            } else {
                // if the aioLinux scheduler was explicitly selected (vs simply
                // using the default type for the OS), then the AIO runtime
                // library must be installed; otherwise, fall through to use
                // ThreadPoolScheduler as fallback
                if (params.usingDefaultSchedulerType) {
                    break;
                }
                throw FennelExcn(FennelResource::instance().libaioRequired());
            }
        }
#endif

#ifdef USE_AIO_H
    case DeviceAccessSchedulerParams::AIO_POLLING_SCHEDULER:
        return new AioPollingScheduler(params);
    case DeviceAccessSchedulerParams::AIO_SIGNAL_SCHEDULER:
        return new AioSignalScheduler(params);
#endif

    default:
        // fall through to use ThreadPoolScheduler as a fallback
        break;
    }
    return new ThreadPoolScheduler(params);
}

DeviceAccessScheduler::~DeviceAccessScheduler()
{
}

RandomAccessRequestBinding::RandomAccessRequestBinding()
{
#ifdef __MSVC__
    // TODO:  only create this when ThreadPoolScheduler is being used?
    hEvent = CreateEvent(NULL, 1, 0, NULL);
    if (!hEvent) {
        throw new SysCallExcn("CreateEvent failed");
    }
#endif
}

RandomAccessRequestBinding::~RandomAccessRequestBinding()
{
#ifdef __MSVC__
    CloseHandle(hEvent);
#endif
}

void RandomAccessRequest::execute()
{
    pDevice->transfer(*this);
}

void DeviceAccessScheduler::registerDevice(
    SharedRandomAccessDevice)
{
}

void DeviceAccessScheduler::unregisterDevice(
    SharedRandomAccessDevice)
{
}

FENNEL_END_CPPFILE("$Id$");

// End DeviceAccessScheduler.cpp

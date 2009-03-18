/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

#include "fennel/common/CommonPreamble.h"
#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/device/ThreadPoolScheduler.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/common/FennelResource.h"

#ifdef __MINGW32__
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

#ifdef __MINGW32__
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
#ifdef __MINGW32__
    // TODO:  only create this when ThreadPoolScheduler is being used?
    hEvent = CreateEvent(NULL,1,0,NULL);
    if (!hEvent) {
        throw new SysCallExcn("CreateEvent failed");
    }
#endif
}

RandomAccessRequestBinding::~RandomAccessRequestBinding()
{
#ifdef __MINGW32__
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

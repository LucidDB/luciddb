/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/common/ConfigMap.h"
#include "fennel/common/FennelExcn.h"
#include "fennel/common/FennelResource.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ParamName DeviceAccessSchedulerParams::paramSchedulerType =
    "deviceSchedulerType";
ParamName DeviceAccessSchedulerParams::paramThreadCount =
    "deviceSchedulerThreadCount";
ParamName DeviceAccessSchedulerParams::paramMaxRequests =
    "deviceSchedulerMaxRequests";

ParamVal DeviceAccessSchedulerParams::valThreadPoolScheduler = "threadPool";
ParamVal DeviceAccessSchedulerParams::valIoCompletionPortScheduler =
"ioCompletionPort";
ParamVal DeviceAccessSchedulerParams::valAioPollingScheduler = "aioPolling";
ParamVal DeviceAccessSchedulerParams::valAioSignalScheduler = "aioSignal";
ParamVal DeviceAccessSchedulerParams::valAioLinuxScheduler = "aioLinux";

DeviceAccessSchedulerParams::DeviceAccessSchedulerParams()
{
#ifdef __MINGW32__
    schedulerType = IO_COMPLETION_PORT_SCHEDULER;
#elif defined(USE_LIBAIO_H)
    schedulerType = AIO_LINUX_SCHEDULER;
#else
    schedulerType = THREAD_POOL_SCHEDULER;
#endif
    nThreads = 1;
    maxRequests = 1024;
    usingDefaultSchedulerType = true;
}

void DeviceAccessSchedulerParams::readConfig(ConfigMap const &configMap)
{
    usingDefaultSchedulerType = false;
    std::string s = configMap.getStringParam(paramSchedulerType);
    if (s == valThreadPoolScheduler) {
        schedulerType = THREAD_POOL_SCHEDULER;
    } else if (s == valAioPollingScheduler) {
        schedulerType = AIO_POLLING_SCHEDULER;
    } else if (s == valAioSignalScheduler) {
        schedulerType = AIO_SIGNAL_SCHEDULER;
    } else if (s == valAioLinuxScheduler) {
        schedulerType = AIO_LINUX_SCHEDULER;
    } else if (s == valIoCompletionPortScheduler) {
        schedulerType = IO_COMPLETION_PORT_SCHEDULER;
    } else {
        // treat unrecognized as default
        usingDefaultSchedulerType = true;
    }
    nThreads = configMap.getIntParam(
        paramThreadCount,nThreads);
    maxRequests = configMap.getIntParam(
        paramMaxRequests,maxRequests);
}

FENNEL_END_CPPFILE("$Id$");

// End DeviceAccessSchedulerParams.cpp

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
#ifdef __MSVC__
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
        paramThreadCount, nThreads);
    maxRequests = configMap.getIntParam(
        paramMaxRequests, maxRequests);
}

FENNEL_END_CPPFILE("$Id$");

// End DeviceAccessSchedulerParams.cpp

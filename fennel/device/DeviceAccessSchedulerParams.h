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

#ifndef Fennel_DeviceAccessSchedulerParams_Included
#define Fennel_DeviceAccessSchedulerParams_Included

FENNEL_BEGIN_NAMESPACE

class ConfigMap;

/**
 * DeviceAccessSchedulerParams defines parameters used to create a
 * DeviceAccessScheduler.
 */
class FENNEL_DEVICE_EXPORT DeviceAccessSchedulerParams
{
public:
    static ParamName paramSchedulerType;
    static ParamName paramThreadCount;
    static ParamName paramMaxRequests;

    static ParamVal valThreadPoolScheduler;
    static ParamVal valIoCompletionPortScheduler;
    static ParamVal valAioPollingScheduler;
    static ParamVal valAioSignalScheduler;
    static ParamVal valAioLinuxScheduler;

    /**
     * Enumeration of available scheduler implementations
     */
    enum SchedulerType {
        THREAD_POOL_SCHEDULER,
        IO_COMPLETION_PORT_SCHEDULER,
        AIO_POLLING_SCHEDULER,
        AIO_SIGNAL_SCHEDULER,
        AIO_LINUX_SCHEDULER
    };

    /**
     * Type of scheduler to create.
     */
    SchedulerType schedulerType;

    /**
     * True if using the default scheduler type, as opposed to the one that was
     * explicitly specified in the configuration file
     */
    bool usingDefaultSchedulerType;

    /**
     * Suggested number of threads to dedicate to scheduling
     * activities; the scheduler may adjust this number based on
     * maxRequests.
     */
    uint nThreads;

    /**
     * The maximum number of simultaneous requests that this scheduler should
     * be able to handle; additional requests will be queued.
     */
    uint maxRequests;

    /**
     * Defines a default set of scheduler parameters.
     */
    DeviceAccessSchedulerParams();

    /**
     * Reads parameter settings from a ConfigMap.
     */
    void readConfig(ConfigMap const &configMap);
};

FENNEL_END_NAMESPACE

#endif

// End DeviceAccessSchedulerParams.h

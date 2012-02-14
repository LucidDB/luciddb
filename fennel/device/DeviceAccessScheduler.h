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

#ifndef Fennel_DeviceAccessScheduler_Included
#define Fennel_DeviceAccessScheduler_Included

FENNEL_BEGIN_NAMESPACE

class RandomAccessRequest;
class DeviceAccessSchedulerParams;

/**
 * DeviceAccessScheduler is an interface representing the ability to
 * initiate access requests on devices and handle their completions
 * asynchronously.  For more information, see DeviceDesign.
 */
class FENNEL_DEVICE_EXPORT DeviceAccessScheduler
{
public:
    /**
     * Creates a scheduler.
     *
     * @param params DeviceAccessSchedulerParams to use
     *
     * @return new scheduler; caller is responsible for deleting it
     */
    static DeviceAccessScheduler *newScheduler(
        DeviceAccessSchedulerParams const &params);

    virtual ~DeviceAccessScheduler();

    /**
     * Registers a device for which this scheduler will process requests.
     * The default implementation does nothing.
     *
     * @param pDevice device to be registered
     */
    virtual void registerDevice(
        SharedRandomAccessDevice pDevice);

    /**
     * Unregisters a device.
     * The default implementation does nothing.
     *
     * @param pDevice device to be unregistered
     */
    virtual void unregisterDevice(
        SharedRandomAccessDevice pDevice);

    /**
     * Initiates a request, the details of which must already have been defined
     * by the caller.  When the request completes, this scheduler will call
     * notifyTransferCompletion on each binding associated with the request,
     * and also break up the binding list.  The bindings must not be altered by
     * the caller until this notification is received.  However, the request
     * parameter itself need not live beyond this call.
     *
     *<p>
     *
     * Care must be taken to ensure that the schedule/notify sequences cannot
     * deadlock.  For example, the caller of schedule may hold a lock on a
     * binding, and the implementation of schedule may acquire a scheduler lock
     * internally.  The notification callback may also need to take a lock on
     * the binding.  Thus, it is important that no
     * scheduler lock be held while notifyTransferCompletion is called.
     *
     * @param request parameters for the request to be scheduled
     *
     * @return true if the request was successfully scheduled without any
     * retries
     */
    virtual bool schedule(RandomAccessRequest &request) = 0;

    /**
     * Shuts down, waiting for all pending requests to complete.
     */
    virtual void stop() = 0;
};

FENNEL_END_NAMESPACE

#endif

// End DeviceAccessScheduler.h

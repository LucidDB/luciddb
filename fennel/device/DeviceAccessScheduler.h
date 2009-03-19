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
class DeviceAccessScheduler
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

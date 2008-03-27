/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
// Copyright (C) 2005-2007 LucidEra, Inc.
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

#ifndef Fennel_AioLinuxScheduler_Included
#define Fennel_AioLinuxScheduler_Included

#ifdef USE_LIBAIO_H

#include "fennel/device/DeviceAccessScheduler.h"
#include "fennel/device/RandomAccessRequest.h"
#include "fennel/common/AtomicCounter.h"
#include "fennel/synch/Thread.h"

#include <vector>
#include <deque>
#include <boost/scoped_array.hpp>

FENNEL_BEGIN_NAMESPACE
    
/**
 * AioLinuxScheduler implements DeviceAccessScheduler via Linux-specific
 * kernel-mode libaio calls.
 *
 * @author John V. Sichi
 * @version $Id$
 */
class AioLinuxScheduler : public DeviceAccessScheduler, public Thread
{
    /**
     * Context for calling libaio.
     */
    io_context_t context;

    /**
     * Number of requests for which io_submit has been called and a
     * corresponding io_getevents notification has not yet been
     * processed.  We track these since it is illegal to attempt to close
     * the context while requests are still outstanding.
     */
    AtomicCounter nRequestsOutstanding;

    /**
     * Flag for passively asking the scheduler to shut down.
     */
    bool quit;

    /**
     * FIFO queue of requests which have been deferred due to
     * not-fully-successful io_submit calls.  Note that these
     * do not yet count as outstanding.  Front of deque is first-in;
     * back of deque is last-in.
     */
    std::deque<RandomAccessRequest::BindingList> deferredQueue;

    /**
     * Mutex used to protect deferredQueue.
     */
    StrictMutex deferredQueueMutex;

    inline bool isStarted() const;

    /**
     * Submits a list of requests which have already been fully prepared.
     *
     * @param bindingList list of requests
     *
     * @return whether all requests were successfully submitted without
     * requiring retry
     */
    bool submitRequests(RandomAccessRequest::BindingList &bindingList);

    /**
     * Saves failed requests to the deferred queue for retry.
     *
     * @param ppLeftovers failed requests
     *
     * @param nLeftovers number of failed requests
     */
    void deferLeftoverRequests(
        iocb **ppLeftovers,
        uint nLeftovers);

    /**
     * Retries submission of requests from the deferred queue;
     * continues until either a submission attempt fails or
     * the queue is exhausted.
     *
     * @return whether all deferred requests were successfully submitted
     * (returns true if there were no deferred requests to begin with)
     */
    bool retryDeferredRequests();

public:
    /**
     * Constructor.
     */
    explicit AioLinuxScheduler(DeviceAccessSchedulerParams const &);

    /**
     * Destructor:  stop must already have been called.
     */
    virtual ~AioLinuxScheduler();

// ----------------------------------------------------------------------
// Implementation of DeviceAccessScheduler interface (q.v.)
// ----------------------------------------------------------------------
    virtual void registerDevice(SharedRandomAccessDevice pDevice);
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

// End AioLinuxScheduler.h

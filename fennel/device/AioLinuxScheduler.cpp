/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
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

#include "fennel/common/CommonPreamble.h"

#ifdef USE_LIBAIO_H

#include "fennel/device/AioLinuxScheduler.h"
#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/common/SysCallExcn.h"
#include <errno.h>

FENNEL_BEGIN_CPPFILE("$Id$");

extern "C"
DeviceAccessScheduler *newAioLinuxScheduler(
    DeviceAccessSchedulerParams const &params)
{
    return new AioLinuxScheduler(params);
}

AioLinuxScheduler::AioLinuxScheduler(
    DeviceAccessSchedulerParams const &params)
{
    quit = false;
    nRequestsOutstanding.clear();
    context = NULL;
    int rc = io_queue_init(params.maxRequests, &context);
    if (rc) {
        throw SysCallExcn("io_queue_init failed");
    }

    // NOTE jvs 29-Oct-2005:  we ignore params.nThreads because
    // no more than one thread is needed for io_getevents
    Thread::start();
}

inline bool AioLinuxScheduler::isStarted() const
{
    return Thread::isStarted();
}

AioLinuxScheduler::~AioLinuxScheduler()
{
    assert(!isStarted());
    assert(!nRequestsOutstanding);
    int rc = io_queue_release(context);
    if (rc) {
        throw SysCallExcn("io_queue_release failed");
    }
}

void AioLinuxScheduler::registerDevice(
    SharedRandomAccessDevice pDevice)
{
    int hFile = pDevice->getHandle();

    // Linux requires files accessed via libaio to be opened with O_DIRECT,
    // so force that now.
    int flags = fcntl(hFile, F_GETFL);
    fcntl(hFile, F_SETFL, flags | O_DIRECT);
}

bool AioLinuxScheduler::schedule(RandomAccessRequest &request)
{
    assert(isStarted());
    request.pDevice->prepareTransfer(request);
    return submitRequests(request.bindingList);
}

bool AioLinuxScheduler::submitRequests(
    RandomAccessRequest::BindingList &bindingList)
{
    iocb *requestsArray[bindingList.size()];
    iocb **requests = requestsArray;

    // convert list to array
    int n = 0;
    RandomAccessRequest::BindingListMutator bindingMutator(bindingList);
    for (; bindingMutator; ++n) {
        RandomAccessRequestBinding *pBinding = bindingMutator.detach();
        requests[n] = pBinding;
    }

    if (n == 0) {
        // just in case someone asks for a nop
        return true;
    }

    // submit array
    int rc = io_submit(context, n, requests);
    if (rc == -EAGAIN) {
        rc = 0;
    }

    if (rc < 0) {
        // hard error
        throw SysCallExcn("io_submit failed");
    }

    // keep track of the number successfully submitted
    // (can't use += because nRequestsOutstanding is
    // an AtomicCounter)
    for (int i = 0; i < rc; ++i) {
        ++nRequestsOutstanding;
    }

    if (rc == n) {
        // we're done
        return true;
    } else {
        // io_submit is allowed to do less than we asked for, so
        // we need to resubmit some leftovers
        requests += rc;
        n -= rc;
        deferLeftoverRequests(requests, n);
        return false;
    }
}

void AioLinuxScheduler::deferLeftoverRequests(
    iocb **ppLeftovers,
    uint nLeftovers)
{
    assert(nLeftovers > 0);

    // convert array back to list
    RandomAccessRequest::BindingList bindingList;

    for (uint i = 0; i < nLeftovers; ++i) {
        RandomAccessRequestBinding *pBinding =
            static_cast<RandomAccessRequestBinding *>(ppLeftovers[i]);
        bindingList.push_back(*pBinding);
    }

    StrictMutexGuard deferredQueueGuard(deferredQueueMutex);
    deferredQueue.push_back(bindingList);
}

bool AioLinuxScheduler::retryDeferredRequests()
{
    for (;;) {
        StrictMutexGuard deferredQueueGuard(deferredQueueMutex);
        if (deferredQueue.empty()) {
            // all resubmitted successfully (or none to begin with)
            return true;
        }
        RandomAccessRequest::BindingList bindingList = deferredQueue.front();
        deferredQueue.pop_front();
        // release mutex now to avoid potential deadlocks
        deferredQueueGuard.unlock();

        bool success = submitRequests(bindingList);
        if (!success) {
            // at least one failed
            return false;
        }
    }
}

void AioLinuxScheduler::stop()
{
    assert(isStarted());
    quit = true;

    Thread::join();
}

void AioLinuxScheduler::run()
{
    while (nRequestsOutstanding || !quit) {
        io_event event;
        timespec ts;

        // Check the deferred request queue before entering wait state.
        if (retryDeferredRequests()) {
            // If we retried any requests, they all succeeded, so we're in our
            // normal wait state: timeout every second to check the quit flag.
            ts.tv_sec = 1;
            ts.tv_nsec = 0;
        } else {
            // At least one retry just failed, so during wait, timeout in a
            // millisecond so we can retry the failed requests.
            ts.tv_sec = 0;
            ts.tv_nsec = 1000000;
        }

        long rc = io_getevents(context, 1, 1, &event, &ts);

        // NOTE jvs 20-Jan-2008:  Docs don't mention the possibility of
        // spurious interrupts, but they can occur, at least while
        // debugging with gdb, so treat them as timeout.
        if ((rc == 0) || (rc == -EINTR)) {
            // timed out
            continue;
        }

        if (rc != 1) {
            throw SysCallExcn("io_getevents failed");
        }
        RandomAccessRequestBinding *pBinding =
            static_cast<RandomAccessRequestBinding *>(event.obj);
        bool success = (pBinding->getBufferSize() == event.res)
            && !event.res2;
        pBinding->notifyTransferCompletion(success);
        --nRequestsOutstanding;
    }
}

FENNEL_END_CPPFILE("$Id$");

#endif

// End AioLinuxScheduler.cpp

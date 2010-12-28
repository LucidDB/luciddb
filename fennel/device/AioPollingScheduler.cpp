/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifdef USE_AIO_H

#include "fennel/device/AioPollingScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/device/RandomAccessDevice.h"
#include <errno.h>

FENNEL_BEGIN_CPPFILE("$Id$");

AioPollingScheduler::AioPollingScheduler(
    DeviceAccessSchedulerParams const &)
{
    // TODO:  pass params.maxRequests on to OS, and use
    // params.nThreads
    quit = false;
    Thread::start();
}

AioPollingScheduler::~AioPollingScheduler()
{
}

bool AioPollingScheduler::schedule(RandomAccessRequest &request)
{
    StrictMutexGuard guard(mutex);
    uint iFirst = newRequests.size();
    request.pDevice->prepareTransfer(request);
    RandomAccessRequest::BindingListMutator bindingMutator(request.bindingList);
    while (bindingMutator) {
        RandomAccessRequestBinding *pBinding = bindingMutator.detach();
        pBinding->aio_sigevent.sigev_notify = SIGEV_NONE;
        newRequests.push_back(pBinding);
    }
    aiocb **pFirst = &(newRequests.front()) + iFirst;
    int rc = lio_listio(LIO_NOWAIT, pFirst, newRequests.size() - iFirst, NULL);
    // TODO:  handle error cases
    assert(rc == 0);
    newRequestPending.notify_all();
    return true;
}

void AioPollingScheduler::stop()
{
    StrictMutexGuard guard(mutex);
    quit = true;
    newRequestPending.notify_all();
    guard.unlock();
    Thread::join();
}

void AioPollingScheduler::run()
{
    int rc;
    struct timespec ts;
    // poll every tenth of a millisecond
    // TODO:  determine a reasonable default, or adjust this
    // dynamically?
    ts.tv_sec = 0;
    ts.tv_nsec = 100000;
    for (;;) {
        StrictMutexGuard guard(mutex);
        while (!newRequests.size()) {
            if (quit) {
                return;
            }
            newRequestPending.wait(guard);
        }
        currentRequests.resize(newRequests.size());
        std::copy(
            newRequests.begin(),
            newRequests.end(),
            currentRequests.begin());
        newRequests.clear();
        guard.unlock();
        do {
            // REVIEW jvs 4-Aug-2004:  Using &front like this is not portable.
            rc = aio_suspend(
                &(currentRequests.front()),
                currentRequests.size(),
                &ts);
            if (rc) {
                switch (errno) {
                case EAGAIN:
                case EINTR:
                    continue;
                default:
                    std::cerr << rc << std::endl;
                    permAssert(false);
                }
            }
        } while (false);
        // currentRequests does not need a lock, since this thread is the only
        // one which manipulates it.  And the lock cannot be held when
        // notifyTransferCompletion is called, otherwise deadlock is possible.
        // However, access to newRequests does require a lock, since other
        // threads may be calling schedule, so use a fine-grained lock on it.
        for (uint i = 0; i < currentRequests.size(); ++i) {
            aiocb *pcb = currentRequests[i];
            RandomAccessRequestBinding *pBinding =
                static_cast<RandomAccessRequestBinding *>(pcb);
            rc = aio_error(pcb);
            if (rc == EINPROGRESS) {
                guard.lock();
                // cout << "EINPROGRESS " << pcb->aio_offset << std::endl;
                newRequests.push_back(pcb);
                guard.unlock();
            } else {
                rc = aio_return(pcb);
                // guard.lock();
                // cout << "complete " << pcb->aio_offset << " " << rc
                // << std::endl;
                // guard.unlock();
                pBinding->notifyTransferCompletion(rc >= 0);
            }
        }
        currentRequests.clear();
    }
}

FENNEL_END_CPPFILE("$Id$");

#endif

// End AioPollingScheduler.cpp

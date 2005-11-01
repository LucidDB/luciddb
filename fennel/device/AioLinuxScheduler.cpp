/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

FENNEL_BEGIN_CPPFILE("$Id$");

AioLinuxScheduler::AioLinuxScheduler(
    DeviceAccessSchedulerParams const &params)
{
    quit = false;
    nRequestsMax = params.maxRequests;
    nRequestsPending.clear();
    context = NULL;
    int rc = io_queue_init(nRequestsMax, &context);
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

void AioLinuxScheduler::schedule(RandomAccessRequest &request)
{
    iocb *requests[request.bindingList.size()];
    
    assert(isStarted());

    request.pDevice->prepareTransfer(request);

    int n = 0;
    RandomAccessRequest::BindingListMutator bindingMutator(request.bindingList);
    for (; bindingMutator; ++n) {
        assert(n < nRequestsMax);
        RandomAccessRequestBinding *pBinding = bindingMutator.detach();
        requests[n] = pBinding;
    }
    int rc = io_submit(context, n, requests);
    // In case any requests failed, get the bookkeeping right to avoid
    // further complications on shutdown.
    for (int i = 0; i < rc; ++i) {
        ++nRequestsPending;
    }
    if (rc != n) {
        throw SysCallExcn("io_submit failed");
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
    while (nRequestsPending || !quit) {
        io_event event;
        timespec ts;
        
        // timeout every second, because that's the only means available for
        // checking the quit flag
        ts.tv_sec = 1;
        ts.tv_nsec = 0;
        long rc = io_getevents(context, 1, 1, &event, &ts);
        if (rc == 0) {
            // timed out:  check quit flag
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
        --nRequestsPending;
    }
}

FENNEL_END_CPPFILE("$Id$");

#endif

// End AioLinuxScheduler.cpp

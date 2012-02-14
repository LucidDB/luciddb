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

#ifdef USE_AIO_H

#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/AioSignalScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/synch/Thread.h"

#include <errno.h>
#include <sys/signal.h>

FENNEL_BEGIN_CPPFILE("$Id$");

class AioSignalHandlerThread : public Thread
{
    AioSignalScheduler &scheduler;
public:
    AioSignalHandlerThread(AioSignalScheduler &schedulerInit)
        : scheduler(schedulerInit)
    {
    }
    virtual void run();
};

static void aio_handler(int, siginfo_t *pSiginfo, void *)
{
    assert(pSiginfo->si_code == SI_ASYNCIO);
    RandomAccessRequestBinding *pBinding =
        static_cast<RandomAccessRequestBinding *>(pSiginfo->si_value.sival_ptr);

    // static_cast assigned to lpBinding is a workaround
    // for a gcc bug that shows up on Ubuntu 8.04 when
    // passing pBinding to aio_* methods
    aiocb *lpBinding = static_cast<aiocb *>(pBinding);

    int rc = aio_error(lpBinding);
    if (rc != EINPROGRESS) {
        rc = aio_return(lpBinding);
        pBinding->notifyTransferCompletion(rc >= 0);
    }
    // TODO:  chain?
}

AioSignalScheduler::AioSignalScheduler(
    DeviceAccessSchedulerParams const &params)
{
    // TODO:  pass params.maxSimultaneousRequests on to OS

    // block signal in this thread so that child threads will also have it
    // blocked
    int rc;
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask,SIGRTMIN);
    rc = pthread_sigmask(SIG_BLOCK, &mask, NULL);
    assert(!rc);

    // TODO:  come up with a way to ensure signal is blocked in all threads
    // except the one spawned below

    struct sigaction sa;
    sa.sa_flags = SA_SIGINFO;
    sa.sa_sigaction = aio_handler;
    sigemptyset(&(sa.sa_mask));
    rc = sigaction(SIGRTMIN,&sa,&saOld);
    assert(!rc);

    quit = false;
    for (uint i = 0; i < params.nThreads; ++i) {
        AioSignalHandlerThread *pThread = new AioSignalHandlerThread(*this);
        pThread->start();
        threads.push_back(pThread);
    }
}

AioSignalScheduler::~AioSignalScheduler()
{
    assert(!isStarted());
}

bool AioSignalScheduler::schedule(RandomAccessRequest &request)
{
    assert(isStarted());

    int rc;

    // TODO: use lio_listio instead?  but then is it possible to get
    // individual notifications?  Or keep chain and notify all at once?
    request.pDevice->prepareTransfer(request);
    RandomAccessRequest::BindingListMutator bindingMutator(request.bindingList);
    while (bindingMutator) {
        RandomAccessRequestBinding *pBinding = bindingMutator.detach();
        pBinding->aio_sigevent.sigev_notify = SIGEV_SIGNAL;
        pBinding->aio_sigevent.sigev_signo = SIGRTMIN;
        pBinding->aio_sigevent.sigev_value.sival_ptr = pBinding;

        // static_cast assigned to lpBinding is a workaround
        // for a gcc bug that shows up on Ubuntu 8.04 when
        // passing pBinding to aio_* methods
        aiocb *lpBinding = static_cast<aiocb *>(pBinding);

        if (request.type == RandomAccessRequest::READ) {
            rc = aio_read(lpBinding);
        } else {
            rc = aio_write(lpBinding);
        }
        assert(!rc);
    }

    return true;
}

void AioSignalScheduler::stop()
{
    assert(isStarted());

    StrictMutexGuard guard(mutex);
    quit = true;
    quitCondition.notify_all();
    guard.unlock();

    for (uint i = 0; i < threads.size(); ++i) {
        threads[i]->join();
        deleteAndNullify(threads[i]);
    }
    threads.clear();

    int rc = sigaction(SIGRTMIN, &saOld, NULL);
    assert(!rc);
}

void AioSignalHandlerThread::run()
{
    int rc;

    // unblock signal in this thread only
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask,SIGRTMIN);
    rc = pthread_sigmask(SIG_UNBLOCK, &mask, NULL);
    assert(!rc);

    // NOTE: had to boost priority of this thread to get signal handler
    // to run frequently.
#ifdef sun
    int policy;
    sched_param param;
    rc = pthread_getschedparam(pthread_self(), &policy, &param);
    assert(!rc);
    param.sched_priority++;
    rc = pthread_setschedparam(pthread_self(), SCHED_RR, &param);
    assert(!rc);
#endif

    // NOTE: using a condition variable causes the signal handler
    // to run too infrequently.  Try again after switching threading models.
    while (!scheduler.quit) {
        sleep(1);
    }
}

FENNEL_END_CPPFILE("$Id$");

#endif

// End AioSignalScheduler.cpp

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

#ifdef __MSVC__

#include "fennel/device/RandomAccessDevice.h"
#include "fennel/device/IoCompletionPortScheduler.h"
#include "fennel/device/DeviceAccessSchedulerParams.h"
#include "fennel/common/SysCallExcn.h"
#include "fennel/synch/Thread.h"

FENNEL_BEGIN_CPPFILE("$Id$");

class IoCompletionPortThread : public Thread
{
    IoCompletionPortScheduler &scheduler;
public:
    IoCompletionPortThread(IoCompletionPortScheduler &schedulerInit)
        : scheduler(schedulerInit)
    {
    }
    virtual void run();
};

IoCompletionPortScheduler::IoCompletionPortScheduler(
    DeviceAccessSchedulerParams const &params)
{
    quit = false;

    hCompletionPort = CreateIoCompletionPort(
        INVALID_HANDLE_VALUE,
        NULL,
        0,
        params.nThreads);
    if (!hCompletionPort) {
        throw SysCallExcn("CreateIoCompletionPort failed for scheduler");
    }

    for (uint i = 0; i < params.nThreads; ++i) {
        IoCompletionPortThread *pThread = new IoCompletionPortThread(*this);
        pThread->start();
        threads.push_back(pThread);
    }
}

IoCompletionPortScheduler::~IoCompletionPortScheduler()
{
    assert(!isStarted());
    if (!CloseHandle(hCompletionPort)) {
        throw SysCallExcn("CloseHandle failed for IoCompletionPort");
    }
}

bool IoCompletionPortScheduler::schedule(RandomAccessRequest &request)
{
    assert(isStarted());

    // TODO:  use ReadFileScatter/WriteFileGather

    FileSize cbOffset = request.cbOffset;
    RandomAccessRequest::BindingListMutator bindingMutator(request.bindingList);
    while (bindingMutator) {
        RandomAccessRequestBinding *pBinding = bindingMutator.detach();
        LARGE_INTEGER largeInt;
        largeInt.QuadPart = cbOffset;
        pBinding->Offset = largeInt.LowPart;
        pBinding->OffsetHigh = largeInt.HighPart;
        BOOL bCompleted;
        if (request.type == RandomAccessRequest::READ) {
            bCompleted = ReadFile(
                HANDLE(request.pDevice->getHandle()),
                pBinding->getBuffer(),
                pBinding->getBufferSize(),
                NULL,
                pBinding);
        } else {
            bCompleted = WriteFile(
                HANDLE(request.pDevice->getHandle()),
                pBinding->getBuffer(),
                pBinding->getBufferSize(),
                NULL,
                pBinding);
        }
        if (!bCompleted) {
            if (GetLastError() != ERROR_IO_PENDING) {
                pBinding->notifyTransferCompletion(false);
            }
        }
        cbOffset += pBinding->getBufferSize();
    }
    assert(cbOffset == request.cbOffset + request.cbTransfer);

    return true;
}

void IoCompletionPortScheduler::stop()
{
    assert(isStarted());

    quit = true;

    // post dummy wakeup notifications; threads will see these and
    // exit
    for (uint i = 0; i < threads.size(); ++i) {
        if (!PostQueuedCompletionStatus(hCompletionPort, 0, 0, NULL)) {
            throw SysCallExcn("PostQueuedCompletionStatus failed");
        }
    }

    for (uint i = 0; i < threads.size(); ++i) {
        threads[i]->join();
        deleteAndNullify(threads[i]);
    }
    threads.clear();
}

void IoCompletionPortThread::run()
{
    DWORD cbTransfer;
    ULONG_PTR pUnused;
    OVERLAPPED *pOverlapped;
    for (;;) {
        BOOL rc = GetQueuedCompletionStatus(
            scheduler.hCompletionPort,
            &cbTransfer,
            &pUnused,
            &pOverlapped,
            INFINITE);
        if (scheduler.quit) {
            return;
        }
        RandomAccessRequestBinding *pBinding =
            static_cast<RandomAccessRequestBinding *>(pOverlapped);
        if (rc) {
            assert(cbTransfer == pBinding->getBufferSize());
        }
        pBinding->notifyTransferCompletion(rc);
    }
}

void IoCompletionPortScheduler::registerDevice(
    SharedRandomAccessDevice pDevice)
{
    int hFile = pDevice->getHandle();
    if (hFile == -1) {
        return;
    }
    if (!CreateIoCompletionPort(
            HANDLE(hFile),
            hCompletionPort,
            0,
            threads.size()))
    {
        throw SysCallExcn("CreateIoCompletionPort failed for device");
    }

    // REVIEW:  is it OK to do nothing for unregister?
}

FENNEL_END_CPPFILE("$Id$");

#endif

// End IoCompletionPortScheduler.cpp

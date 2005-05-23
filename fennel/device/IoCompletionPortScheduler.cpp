/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifdef __MINGW32__

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

void IoCompletionPortScheduler::schedule(RandomAccessRequest &request)
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
}

void IoCompletionPortScheduler::stop()
{
    assert(isStarted());

    quit = true;

    // post dummy wakeup notifications; threads will see these and
    // exit
    for (uint i = 0; i < threads.size(); ++i) {
        if (!PostQueuedCompletionStatus(hCompletionPort,0,0,NULL)) {
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

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

#ifndef Fennel_RandomAccessRequest_Included
#define Fennel_RandomAccessRequest_Included

#include "fennel/common/IntrusiveList.h"

#ifdef HAVE_AIO_H
#include <aio.h>
struct aiocb;
#endif

#ifdef HAVE_LIBAIO_H
#include <libaio.h>
struct iocb;
#endif

#ifdef __MSVC__
#include <windows.h>
#endif

FENNEL_BEGIN_NAMESPACE

class RandomAccessDevice;

/**
 * RandomAccessRequestBinding binds a RandomAccessRequest to a particular
 * memory location being read from or written to.
 */
class FENNEL_DEVICE_EXPORT RandomAccessRequestBinding
    : public IntrusiveListNode
#ifdef USE_AIO_H
, public aiocb
#endif
#ifdef USE_LIBAIO_H
, public iocb
#endif
#ifdef __MSVC__
, public OVERLAPPED
#endif
{
public:
    explicit RandomAccessRequestBinding();
    virtual ~RandomAccessRequestBinding();

    /**
     * @return memory address where transfer should start.
     */
    virtual PBuffer getBuffer() const = 0;

    /**
     * @return number of contiguous bytes from getBuffer() to be used for
     * transfer.
     */
    virtual uint getBufferSize() const = 0;

    /**
     * Receives notification when a transfer completes.
     *
     * @param bSuccess true if the full buffer size was successfully
     * transferred for this binding
     */
    virtual void notifyTransferCompletion(bool bSuccess) = 0;
};

/**
 * RandomAccessRequest represents one logical unit of I/O against a
 * RandomAccessDevice.  Currently supported operations are reads and writes
 * spanning a contiguous range of byte offsets within the device.  The
 * RandomAccessRequestBinding memory locations need not be contiguous
 * (scatter/gather).
 */
class FENNEL_DEVICE_EXPORT RandomAccessRequest
{
public:
    enum Type {
        READ,
        WRITE
    };

    typedef IntrusiveList<RandomAccessRequestBinding> BindingList;
    typedef IntrusiveListIter<RandomAccessRequestBinding> BindingListIter;
    typedef IntrusiveListMutator<RandomAccessRequestBinding>
    BindingListMutator;

    /**
     * The device to be accessed.  It's a pointer rather than a reference so
     * that RandomAccessRequest behaves as a concrete class.
     */
    RandomAccessDevice *pDevice;

    /**
     * Byte offset within device at which access should start.
     */
    FileSize cbOffset;

    /**
     * Number of bytes to be transferred.
     */
    FileSize cbTransfer;

    /**
     * Access type:  READ for transfer from device to memory; WRITE for
     * transfer from memory to device.
     */
    Type type;

    /**
     * Bindings for memory source or destination and notifications.
     */
    BindingList bindingList;

    /**
     * Executes this request.  (Satisfies the ThreadPool Task signature,
     * allowing instances of this class to be submitted directly as a Task by
     * ThreadPoolScheduler).
     */
    void execute();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAccessRequest.h

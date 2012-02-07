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
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/device/RandomAccessRequest.h"

FENNEL_BEGIN_CPPFILE("$Id$");

RandomAccessFileDevice::RandomAccessFileDevice(
    std::string filename, DeviceMode mode, FileSize initialSize)
    : FileDevice(filename, mode, initialSize)
{
}

RandomAccessFileDevice::RandomAccessFileDevice(
    std::string filename, DeviceMode mode)
    : FileDevice(filename, mode, FileSize(0))
{
}

RandomAccessDevice::~RandomAccessDevice()
{
}

FileSize RandomAccessFileDevice::getSizeInBytes()
{
    return FileDevice::getSizeInBytes();
}

void RandomAccessFileDevice::setSizeInBytes(FileSize cbNew)
{
    FileDevice::setSizeInBytes(cbNew);
}

void RandomAccessFileDevice::transfer(RandomAccessRequest const &request)
{
    assert(request.pDevice == this);
    FileDevice::transfer(request);
}

void RandomAccessFileDevice::prepareTransfer(RandomAccessRequest &request)
{
    assert(request.pDevice == this);

#ifdef USE_AIO_H
    int aio_lio_opcode =
        (request.type == RandomAccessRequest::READ)
        ? LIO_READ : LIO_WRITE;
    FileSize cbOffset = request.cbOffset;
    for (RandomAccessRequest::BindingListIter
             bindingIter(request.bindingList);
         bindingIter; ++bindingIter)
    {
        RandomAccessRequestBinding *pBinding = bindingIter;
        pBinding->aio_fildes = handle;
        pBinding->aio_lio_opcode = aio_lio_opcode;
        // TODO:  initialize constant fields below only once
        pBinding->aio_buf = pBinding->getBuffer();
        pBinding->aio_nbytes = pBinding->getBufferSize();
        pBinding->aio_reqprio = 0;
        pBinding->aio_offset = cbOffset;
        cbOffset += pBinding->aio_nbytes;
    }
    assert(cbOffset == request.cbOffset + request.cbTransfer);
#endif

#ifdef USE_LIBAIO_H
    FileSize cbOffset = request.cbOffset;
    for (RandomAccessRequest::BindingListIter
             bindingIter(request.bindingList);
         bindingIter; ++bindingIter)
    {
        RandomAccessRequestBinding *pBinding = bindingIter;

        if (request.type == RandomAccessRequest::READ) {
            io_prep_pread(
                pBinding, handle, pBinding->getBuffer(),
                pBinding->getBufferSize(), cbOffset);
        } else {
            io_prep_pwrite(
                pBinding, handle, pBinding->getBuffer(),
                pBinding->getBufferSize(), cbOffset);
        }
        cbOffset += pBinding->getBufferSize();
    }
    assert(cbOffset == request.cbOffset + request.cbTransfer);
#endif
}

void RandomAccessFileDevice::flush()
{
    FileDevice::flush();
}

int RandomAccessFileDevice::getHandle()
{
    return handle;
}

FENNEL_END_CPPFILE("$Id$");

// End RandomAccessFileDevice.cpp

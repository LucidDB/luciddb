/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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
#include "fennel/common/FennelResource.h"
#include "fennel/common/SysCallExcn.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/lucidera/flatfile/FlatFileBinding.h"
#include "fennel/lucidera/flatfile/FlatFileBuffer.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileBuffer::FlatFileBuffer(const std::string &path)
{
    this->path = path;
    pBuffer = NULL;
    bufferSize = 0;
    contentSize = 0;
    pCurrent = NULL;
}

FlatFileBuffer::~FlatFileBuffer()
{
    close();
}

void FlatFileBuffer::closeImpl()
{
    pRandomAccessDevice.reset();
    contentSize = 0;
    pCurrent = NULL;
}

void FlatFileBuffer::setStorage(char *pBuffer, uint size)
{
    this->pBuffer = pBuffer;
    bufferSize = size;
    contentSize = 0;
    pCurrent = NULL;
}

// TODO: review exception mechanism
void FlatFileBuffer::open()
{
    DeviceMode openMode;
    openMode.readOnly = openMode.sequential = 1;
    try {
        pRandomAccessDevice.reset(
            new RandomAccessFileDevice(path,openMode));
    } catch (SysCallExcn e) {
        FENNEL_TRACE(TRACE_SEVERE, e.getMessage());
        throw FennelExcn(
            FennelResource::instance().readDataFailed(path));
    }
    filePosition = 0;
    fileEnd = pRandomAccessDevice->getSizeInBytes();
    contentSize = 0;
    pCurrent = NULL;
}

uint FlatFileBuffer::read()
{
    int residual = 0;
    if (pCurrent != NULL) {
        assert(pBuffer <= pCurrent && pCurrent <= getEndPtr());
        residual = getEndPtr() - pCurrent;
        memmove(pBuffer, pCurrent, residual * sizeof(char));
        contentSize = residual;
    }
    pCurrent = pBuffer;

    uint free = bufferSize - residual;
    char *target = pBuffer + residual;
    uint targetSize =
        std::min( free * sizeof(char), (uint) (fileEnd - filePosition) );
    
    RandomAccessRequest readRequest;
    readRequest.pDevice = pRandomAccessDevice.get();
    readRequest.cbOffset = filePosition;
    readRequest.cbTransfer = targetSize;
    readRequest.type = RandomAccessRequest::READ;
    FlatFileBinding binding(path, target, targetSize);
    readRequest.bindingList.push_back(binding);
    pRandomAccessDevice->transfer(readRequest);
    filePosition += targetSize;

    contentSize = residual + targetSize/sizeof(char);
    return targetSize/sizeof(char);
}

char *FlatFileBuffer::getReadPtr()
{
    assert(pCurrent != NULL && pBuffer <= pCurrent);
    return pCurrent;
}

char *FlatFileBuffer::getEndPtr()
{
    assert(pBuffer != NULL);
    return pBuffer + contentSize;
}

int FlatFileBuffer::getSize()
{
    return getEndPtr() - getReadPtr();
}

bool FlatFileBuffer::isFull()
{
    assert(pBuffer != NULL);
    return (pCurrent == pBuffer && contentSize == bufferSize);
}

bool FlatFileBuffer::isComplete()
{
    assert(filePosition <= fileEnd);
    return filePosition == fileEnd;
}

void FlatFileBuffer::setReadPtr(char *ptr) 
{
    assert(pBuffer <= pCurrent && pCurrent <= ptr && ptr <= getEndPtr());
    pCurrent = ptr;
}

FENNEL_END_CPPFILE("$Id: //open/dt/dev/fennel/lucidera/flatfile/FlatFileBuffer.cpp#6 $");

// End FlatFileBuffer.cpp

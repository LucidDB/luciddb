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

#include "fennel/lucidera/flatfile/FlatFileBuffer.h"

#include "fennel/common/FennelResource.h"
#include "fennel/common/SysCallExcn.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/device/RandomAccessRequest.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileBuffer::~FlatFileBuffer()
{
    close();
}

FlatFileBuffer::FlatFileBuffer(const std::string &path)
{
    this->path = path;
    this->buffer = NULL;
    this->bufferSize = 0;
    this->contentSize = 0;
}

void FlatFileBuffer::setStorage(char *buffer, uint size)
{
    this->buffer = buffer;
    this->bufferSize = size;
    this->contentSize = 0;
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
        FENNEL_TRACE(TRACE_FINE, e.getMessage());
        throw FennelExcn(
            FennelResource::instance().readDataFailed(path));
    }
    filePosition = 0;
    fileEnd = pRandomAccessDevice->getSizeInBytes();
    contentSize = 0;
}

void FlatFileBuffer::closeImpl()
{
    pRandomAccessDevice.reset();
    contentSize = 0;
}

bool FlatFileBuffer::readCompleted()
{
    assert(filePosition <= fileEnd);
    return filePosition == fileEnd;
}

/**
 * Specifies parameters for flat file read requests
 */
class FlatFileBinding : public RandomAccessRequestBinding
{
    std::string path;
    char *buffer;
    uint bufferSize;
    
public:
    FlatFileBinding(std::string &path, char *buf, uint size) 
    {
        this->path = path;
        buffer = buf;
        bufferSize = size;
    }
        
    PBuffer getBuffer() const { return (PBuffer) buffer; }
    uint getBufferSize() const { return bufferSize; }
    void notifyTransferCompletion(bool bSuccess) {
        if (!bSuccess) {
            throw FennelExcn(FennelResource::instance().dataTransferFailed(
                                 path, bufferSize));
        }
    }
};

inline uint min(uint a, uint b) 
{
    return (a < b) ? a : b;
}

uint FlatFileBuffer::fill(char *unread)
{
    int residual = 0;
    if (unread) {
        char *contentEnd = buffer + contentSize;
        assert(buffer <= unread && unread <= contentEnd);
        residual = contentEnd - unread;
        memmove(buffer, unread, residual * sizeof(char));
        contentSize = residual;
    }

    int free = bufferSize - residual;
    char *target = buffer + residual;
    uint targetSize = min(free*sizeof(char), fileEnd-filePosition);
    
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

bool FlatFileBuffer::full() 
{
    return (contentSize == bufferSize);
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileBuffer.cpp

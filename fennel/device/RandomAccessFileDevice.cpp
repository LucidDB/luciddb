/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later Eigenbase-approved version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307  USA
*/

#include "fennel/common/CommonPreamble.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/device/RandomAccessRequest.h"

FENNEL_BEGIN_CPPFILE("$Id$");

RandomAccessFileDevice::RandomAccessFileDevice(
    std::string filename,DeviceMode mode)
    : FileDevice(filename,mode)
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

#ifdef HAVE_AIO_H
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

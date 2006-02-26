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
#include "fennel/common/ByteBuffer.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ByteBuffer::~ByteBuffer() {}

void ByteBuffer::setMem(uint pos, UnsignedByte value, uint len) 
{
    assert(pos + len < getSize());
    uint current = pos;
    uint remaining = len;
    uint chunkLen;

    while (remaining > 0) {
        chunkLen = getContiguousMemSize(current, remaining);
        memset(getMem(current, chunkLen), value, chunkLen);
        current += chunkLen;
        remaining -= chunkLen;
    }
}

void ByteBuffer::copyMem(uint pos, PConstBuffer mem, uint len)
{
    assert(pos + len < getSize());
    uint current = pos;
    PConstBuffer currentMem = mem;
    uint remaining = len;
    uint chunkLen;

    while (remaining > 0) {
        chunkLen = getContiguousMemSize(current, remaining);
        memcpy(getMem(current, chunkLen), currentMem, chunkLen);
        current += chunkLen;
        currentMem += chunkLen;
        remaining -= chunkLen;
    }
}

void ByteBuffer::mergeMem(uint pos, PConstBuffer mem, uint len)
{
    assert(pos + len < getSize());
    uint current = pos;
    PConstBuffer currentMem = mem;
    uint remaining = len;
    uint chunkLen;

    while (remaining > 0) {
        chunkLen = getContiguousMemSize(current, remaining);
        memmerge(getMem(current, chunkLen), currentMem, chunkLen);
        current += chunkLen;
        currentMem += chunkLen;
        remaining -= chunkLen;
    }
}

VirtualByteBuffer::VirtualByteBuffer()
{
    nBuffers = 0;
    bufferSize = 0;
}

VirtualByteBuffer::~VirtualByteBuffer()
{
}

void VirtualByteBuffer::init(
    boost::shared_array<PBuffer> ppBuffers, uint nBuffers, uint bufSize)
{
    this->ppBuffers = ppBuffers;
    this->nBuffers = nBuffers;
    this->bufferSize = bufSize;
}

uint VirtualByteBuffer::getSize() 
{
    return nBuffers * bufferSize;
}

UnsignedByte VirtualByteBuffer::getByte(uint pos)
{
    uint i, j;
    getOffset(pos, i, j);
    return ppBuffers[i][j];
}

void VirtualByteBuffer::setByte(uint pos, UnsignedByte b)
{
    uint i, j;
    getOffset(pos, i, j);
    ppBuffers[i][j] = b;
}

void VirtualByteBuffer::mergeByte(uint pos, UnsignedByte b)
{
    uint i, j;
    getOffset(pos, i, j);
    ppBuffers[i][j] |= b;
}

uint VirtualByteBuffer::getContiguousMemSize(uint pos, uint max)
{
    uint size = bufferSize - (pos % bufferSize);
    return max ? std::min(max, size) : size;
}

PBuffer VirtualByteBuffer::getMem(uint pos, uint len)
{
    if (len <= getContiguousMemSize(pos)) {
        return NULL;
    }
    uint i, j;
    getOffset(pos, i, j);
    return &ppBuffers[i][j];
}

FENNEL_END_CPPFILE("$Id$");

// End ByteBuffer.cpp

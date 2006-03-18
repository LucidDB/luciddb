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
ByteBuffer::ByteBuffer()
{
    nBuffers = 0;
    bufferSize = 0;
}

ByteBuffer::~ByteBuffer()
{
}

void ByteBuffer::init(
    boost::shared_array<PBuffer> ppBuffers, uint nBuffers, uint bufSize)
{
    this->ppBuffers = ppBuffers;
    this->nBuffers = nBuffers;
    this->bufferSize = bufSize;

    // Currently we use bit masking (rather than mod) to calculate virtual
    // offsets. This scheme requires that the buffer size be a power of 2.
    // If this cannot be guaranteed, we should change to another scheme.
    bufferMask = bufSize - 1;
    assert((bufferMask & bufSize) == 0);

    bufferShift = 0;
    uint tmp = bufferMask;
    while (tmp > 0) {
        bufferShift++;
        tmp >>=1;
    }
}

uint ByteBuffer::getSize() 
{
    return nBuffers * bufferSize;
}

void ByteBuffer::setMem(uint pos, UnsignedByte value, uint len) 
{
    assert(pos + len <= getSize());

    uint chunkLen = getContiguousMemSize(pos, len);
    memset(getMem(pos), value, chunkLen);

    if (chunkLen < len) {
        uint current = pos + chunkLen;
        uint remaining = len - chunkLen;
        while (remaining > 0) {
            chunkLen = getContiguousMemSize(current, remaining);
            memset(getMem(current), value, chunkLen);
            current += chunkLen;
            remaining -= chunkLen;
        }
    }
}

void ByteBuffer::copyMem(uint pos, PConstBuffer mem, uint len)
{
    assert(pos + len <= getSize());

    uint chunkLen = getContiguousMemSize(pos, len);
    memcpy(getMem(pos), mem, chunkLen);

    if (chunkLen < len) {
        uint current = pos + chunkLen;
        PConstBuffer currentMem = mem + chunkLen;
        uint remaining = len - chunkLen;
        while (remaining > 0) {
            chunkLen = getContiguousMemSize(current, remaining);
            memcpy(getMem(current), currentMem, chunkLen);
            current += chunkLen;
            remaining -= chunkLen;
        }
    }
}

void ByteBuffer::mergeMem(uint pos, PConstBuffer mem, uint len)
{
    assert(pos + len <= getSize());

    uint chunkLen = getContiguousMemSize(pos, len);
    memmerge(getMem(pos), mem, chunkLen);

    if (chunkLen < len) {
        uint current = pos + chunkLen;
        PConstBuffer currentMem = mem + chunkLen;
        uint remaining = len - chunkLen;
        while (remaining > 0) {
            chunkLen = getContiguousMemSize(current, remaining);
            memmerge(getMem(current), currentMem, chunkLen);
            current += chunkLen;
            remaining -= chunkLen;
        }
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ByteBuffer.cpp

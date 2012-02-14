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
#include "fennel/common/ByteBuffer.h"

FENNEL_BEGIN_CPPFILE("$Id$");
ByteBuffer::ByteBuffer()
{
    nBuffers = 0;
    bufferSize = 0;
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
    uint current = pos;
    uint remaining = len;

    while (remaining > 0) {
        uint chunkLen;
        PBuffer mem = getMem(current, chunkLen);
        if (chunkLen >= remaining) {
            memset(mem, value, remaining);
            break;
        }
        memset(mem, value, chunkLen);
        current += chunkLen;
        remaining -= chunkLen;
    }
}

void ByteBuffer::copyMem(uint pos, PConstBuffer data, uint len)
{
    assert(pos + len <= getSize());
    uint current = pos;
    PConstBuffer currentData = data;
    uint remaining = len;

    while (remaining > 0) {
        uint chunkLen;
        PBuffer mem = getMem(current, chunkLen);
        if (chunkLen >= remaining) {
            memcpy(mem, currentData, remaining);
            break;
        }
        memcpy(mem, currentData, chunkLen);
        current += chunkLen;
        currentData += chunkLen;
        remaining -= chunkLen;
    }
}

void ByteBuffer::mergeMem(uint pos, PConstBuffer data, uint len)
{
    assert(pos + len <= getSize());
    uint current = pos;
    PConstBuffer currentData = data;
    uint remaining = len;

    while (remaining > 0) {
        uint chunkLen;
        PBuffer mem = getMem(current, chunkLen);
        if (chunkLen >= remaining) {
            memmerge(mem, currentData, remaining);
            break;
        }
        memmerge(mem, currentData, chunkLen);
        current += chunkLen;
        currentData += chunkLen;
        remaining -= chunkLen;
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ByteBuffer.cpp

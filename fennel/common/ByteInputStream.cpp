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
#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ByteInputStream::ByteInputStream()
{
    nullifyBuffer();
}

uint ByteInputStream::readBytes(
    void *pData,uint cbRequested)
{
    uint cbActual = 0;
    if (pNextByte == pEndByte) {
        readNextBuffer();
    }
    for (;;) {
        uint cbAvailable = getBytesAvailable();
        if (!cbAvailable) {
            break;
        }
        if (cbRequested <= cbAvailable) {
            if (pData) {
                memcpy(pData, pNextByte, cbRequested);
            }
            pNextByte += cbRequested;
            cbActual += cbRequested;
            break;
        }
        if (pData) {
            memcpy(pData, pNextByte, cbAvailable);
            pData = static_cast<char *>(pData) + cbAvailable;
        }
        cbRequested -= cbAvailable;
        cbActual += cbAvailable;
        readNextBuffer();
    }
    cbOffset += cbActual;
    return cbActual;
}

void ByteInputStream::readPrevBuffer()
{
    permAssert(false);
}

void ByteInputStream::seekBackward(uint cb)
{
    assert(cb <= cbOffset);
    cbOffset -= cb;
    if (pNextByte == pFirstByte) {
        readPrevBuffer();
        pNextByte = pEndByte;
    }
    for (;;) {
        uint cbAvailable = getBytesConsumed();
        assert(cbAvailable);
        if (cb <= cbAvailable) {
            pNextByte -= cb;
            break;
        }
        cb -= cbAvailable;
        readPrevBuffer();
        pNextByte = pEndByte;
    }
}

SharedByteStreamMarker ByteInputStream::newMarker()
{
    return SharedByteStreamMarker(new SequentialByteStreamMarker(*this));
}

void ByteInputStream::mark(ByteStreamMarker &marker)
{
    assert(&(marker.getStream()) == this);

    SequentialByteStreamMarker &seqMarker =
        dynamic_cast<SequentialByteStreamMarker &>(marker);
    seqMarker.cbOffset = getOffset();
}

void ByteInputStream::reset(ByteStreamMarker const &marker)
{
    assert(&(marker.getStream()) == this);

    SequentialByteStreamMarker const &seqMarker =
        dynamic_cast<SequentialByteStreamMarker const &>(marker);
    assert(!isMAXU(seqMarker.cbOffset));
    if (cbOffset == seqMarker.cbOffset) {
        // expedite common case where stream has not moved since mark
        return;
    } else if (cbOffset > seqMarker.cbOffset) {
        seekBackward(cbOffset - seqMarker.cbOffset);
    } else {
        seekForward(seqMarker.cbOffset - cbOffset);
    }
}

FENNEL_END_CPPFILE("$Id$");

// End ByteInputStream.cpp

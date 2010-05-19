/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

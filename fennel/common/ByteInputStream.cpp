/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
                memcpy(pData,pNextByte,cbRequested);
            }
            pNextByte += cbRequested;
            cbActual += cbRequested;
            break;
        }
        if (pData) {
            memcpy(pData,pNextByte,cbAvailable);
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
    assert(false);
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

FENNEL_END_CPPFILE("$Id$");

// End ByteInputStream.cpp

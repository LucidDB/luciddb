/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 SQLstream, Inc.
// Copyright (C) 2005-2007 LucidEra, Inc.
// Portions Copyright (C) 1999-2007 John V. Sichi
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
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

ByteOutputStream::ByteOutputStream()
{
    pNextByte = NULL;
    cbWritable = 0;
}

void ByteOutputStream::writeBytes(void const *pData,uint cb)
{
    cbOffset += cb;
    if (!cbWritable) {
        flushBuffer(1);
    }
    for (;;) {
        assert(cbWritable);
        if (cb <= cbWritable) {
            memcpy(pNextByte,pData,cb);
            cbWritable -= cb;
            pNextByte += cb;
            return;
        }
        memcpy(pNextByte,pData,cbWritable);
        pData = static_cast<char const *>(pData) + cbWritable;
        cb -= cbWritable;
        cbWritable = 0;
        flushBuffer(1);
    }
}

void ByteOutputStream::closeImpl()
{
    flushBuffer(0);
}

void ByteOutputStream::hardPageBreak()
{
    flushBuffer(0);
    cbWritable = 0;
    pNextByte = NULL;
}

void ByteOutputStream::setWriteLatency(WriteLatency writeLatencyInit)
{
    writeLatency = writeLatencyInit;
}

FENNEL_END_CPPFILE("$Id$");

// End ByteOutputStream.cpp

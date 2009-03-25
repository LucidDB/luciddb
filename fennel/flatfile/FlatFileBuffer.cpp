/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2009-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
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
#include "fennel/common/FennelExcn.h"
#include "fennel/device/RandomAccessFileDevice.h"
#include "fennel/flatfile/FlatFileBuffer.h"

FENNEL_BEGIN_CPPFILE("$Id$");

FlatFileBuffer::FlatFileBuffer(const std::string &path)
{
    this->path = path;
    pBuffer = NULL;
    bufferSize = 0;
    contentSize = 0;
    pCurrent = NULL;
    pFile = NULL;
}

FlatFileBuffer::~FlatFileBuffer()
{
}

void FlatFileBuffer::closeImpl()
{
    if (pFile) {
        fclose(pFile);
        pFile = NULL;
    }
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

void FlatFileBuffer::open()
{
    // in case we are reopening
    close();

    // NOTE jvs 17-Oct-2008:  we use fopen here instead of ifstream
    // in case we want to support popen("gunzip") in the future.

    pFile = fopen(path.c_str(), "r");
    if (!pFile) {
        throw FennelExcn(
            FennelResource::instance().readDataFailed(path));
    }

    needsClose = true;

    filePosition = 0;
    contentSize = 0;
    pCurrent = NULL;
}

uint FlatFileBuffer::read()
{
    int residual = 0;
    if (pCurrent != NULL) {
        assert(pBuffer <= pCurrent && pCurrent <= getEndPtr());
        residual = getEndPtr() - pCurrent;
        memmove(pBuffer, pCurrent, residual);
        contentSize = residual;
    }
    pCurrent = pBuffer;

    uint free = bufferSize - residual;
    char *target = pBuffer + residual;
    uint targetSize = free;

    size_t actualSize = fread(target, 1, targetSize, pFile);
    if (ferror(pFile)) {
        // FIXME jvs 19-Oct-2008:  the error message here is confusingly
        // switched with the error message for opening the file.
        throw FennelExcn(
            FennelResource::instance().dataTransferFailed(path, targetSize));
    }
    filePosition += actualSize;
    contentSize = residual + actualSize;
    return actualSize;
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
    assert(pFile);
    return feof(pFile);
}

bool FlatFileBuffer::isDone()
{
    return isComplete() && getReadPtr() >= getEndPtr();
}

void FlatFileBuffer::setReadPtr(char *ptr)
{
    assert(pBuffer <= pCurrent && pCurrent <= ptr && ptr <= getEndPtr());
    pCurrent = ptr;
}

FENNEL_END_CPPFILE("$Id$");

// End FlatFileBuffer.cpp

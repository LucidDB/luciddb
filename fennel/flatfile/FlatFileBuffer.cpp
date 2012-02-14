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

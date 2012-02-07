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
#include "fennel/common/ByteArrayInputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedByteArrayInputStream ByteArrayInputStream::newByteArrayInputStream(
    PConstBuffer pBuffer,
    uint cbBuffer)
{
    return SharedByteArrayInputStream(
        new ByteArrayInputStream(pBuffer, cbBuffer),
        ClosableObjectDestructor());
}

ByteArrayInputStream::ByteArrayInputStream(
    PConstBuffer pBuffer,
    uint cbBuffer)
{
    setBuffer(pBuffer, cbBuffer);
}

void ByteArrayInputStream::readNextBuffer()
{
    nullifyBuffer();
}

void ByteArrayInputStream::readPrevBuffer()
{
    nullifyBuffer();
}

void ByteArrayInputStream::closeImpl()
{
    // nothing to do
}

void ByteArrayInputStream::resetArray(
    PConstBuffer pBuffer,
    uint cbBuffer)
{
    setBuffer(pBuffer, cbBuffer);
    cbOffset = 0;
}

FENNEL_END_CPPFILE("$Id$");

// End ByteArrayInputStream.cpp

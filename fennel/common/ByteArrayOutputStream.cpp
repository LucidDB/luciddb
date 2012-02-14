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
#include "fennel/common/ByteArrayOutputStream.h"

FENNEL_BEGIN_CPPFILE("$Id$");

SharedByteArrayOutputStream ByteArrayOutputStream::newByteArrayOutputStream(
    PBuffer pBuffer,
    uint cbBuffer)
{
    return SharedByteArrayOutputStream(
        new ByteArrayOutputStream(pBuffer, cbBuffer),
        ClosableObjectDestructor());
}

ByteArrayOutputStream::ByteArrayOutputStream(
    PBuffer pBufferInit,
    uint cbBufferInit)
{
    pBuffer = pBufferInit;
    cbBuffer = cbBufferInit;
    setBuffer(pBuffer, cbBuffer);
}

void ByteArrayOutputStream::flushBuffer(uint)
{
    permAssert(false);
}

void ByteArrayOutputStream::closeImpl()
{
    // nothing to do
}

void ByteArrayOutputStream::clear()
{
    setBuffer(pBuffer, cbBuffer);
}

FENNEL_END_CPPFILE("$Id$");

// End ByteArrayOutputStream.cpp

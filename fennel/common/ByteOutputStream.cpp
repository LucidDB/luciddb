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
            memcpy(pNextByte, pData, cb);
            cbWritable -= cb;
            pNextByte += cb;
            return;
        }
        memcpy(pNextByte, pData, cbWritable);
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

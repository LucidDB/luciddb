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

#ifndef Fennel_ByteArrayOutputStream_Included
#define Fennel_ByteArrayOutputStream_Included

#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteArrayOutputStream implements the ByteOutputStream interface by writing
 * data to an existing fixed-size array of bytes.
 */
class FENNEL_COMMON_EXPORT ByteArrayOutputStream : public ByteOutputStream
{
    PBuffer pBuffer;
    uint cbBuffer;

    // implement the ByteOutputStream interface
    virtual void flushBuffer(uint cbRequested);
    virtual void closeImpl();

    explicit ByteArrayOutputStream(
        PBuffer pBuffer,
        uint cbBuffer);

public:
    /**
     * Creates a new ByteArrayOutputStream.
     *
     * @param pBuffer byte array to fill
     *
     * @param cbBuffer buffer capacity
     *
     * @return shared_ptr to new ByteArrayOutputStream
     */
    static SharedByteArrayOutputStream newByteArrayOutputStream(
        PBuffer pBuffer,
        uint cbBuffer);

    /**
     * Clears any data written to the buffer, leaving it in the same
     * state as after construction.
     */
    void clear();
};

FENNEL_END_NAMESPACE

#endif

// End ByteArrayOutputStream.h

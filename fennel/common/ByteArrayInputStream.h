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

#ifndef Fennel_ByteArrayInputStream_Included
#define Fennel_ByteArrayInputStream_Included

#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteArrayInputStream implements the ByteInputStream interface by reading data
 * from an existing fixed-size array of bytes.
 */
class FENNEL_COMMON_EXPORT ByteArrayInputStream : public ByteInputStream
{
    // implement the ByteInputStream interface
    virtual void readNextBuffer();
    virtual void readPrevBuffer();
    virtual void closeImpl();

    explicit ByteArrayInputStream(
        PConstBuffer pBuffer,
        uint cbBuffer);

public:
    /**
     * Creates a new ByteArrayInputStream.
     *
     * @param pBuffer bytes to read
     *
     * @param cbBuffer number of bytes to read
     *
     * @return shared_ptr to new ByteArrayInputStream
     */
    static SharedByteArrayInputStream newByteArrayInputStream(
        PConstBuffer pBuffer,
        uint cbBuffer);

    /**
     * Resets stream to read from a new array.
     *
     * @param pBuffer bytes to read
     *
     * @param cbBuffer number of bytes to read
     */
    void resetArray(
        PConstBuffer pBuffer,
        uint cbBuffer);
};

FENNEL_END_NAMESPACE

#endif

// End ByteArrayInputStream.h

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

#ifndef Fennel_ByteStream_Included
#define Fennel_ByteStream_Included

#include "fennel/common/ClosableObject.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteStream is a common base class for ByteInputStream and ByteOutputStream.
 */
class FENNEL_COMMON_EXPORT ByteStream
    : virtual public ClosableObject
{
protected:
    /**
     * Byte position in stream.
     */
    FileSize cbOffset;

    explicit ByteStream();
public:

    /**
     * @return current offset from beginning of stream
     */
    FileSize getOffset() const;
};

/**
 * ByteStreamMarker is an opaque position within a ByteStream.  Stream
 * implementations define derived marker classes containing hidden state.
 * ByteInputStream::newMarker() serves as a factory method for creating new
 * marker instances.
 */
class FENNEL_COMMON_EXPORT ByteStreamMarker
{
    friend class ByteStream;

    /**
     * Marked stream.
     */
    ByteStream const &stream;

protected:
    explicit ByteStreamMarker(ByteStream const &stream);
    virtual ~ByteStreamMarker()
    {
    }

public:
    /**
     * @return marked stream
     */
    ByteStream const &getStream() const;

    /**
     * @return byte offset of marked position within stream
     */
    virtual FileSize getOffset() const = 0;
};

/**
 * SequentialByteStreamMarker is a default implementation of
 * ByteStreamMarker based on sequential byte position.
 */
class FENNEL_COMMON_EXPORT SequentialByteStreamMarker : public ByteStreamMarker
{
    friend class ByteInputStream;

    /**
     * Byte position in stream.
     */
    FileSize cbOffset;

public:
    virtual ~SequentialByteStreamMarker()
    {
    }

protected:
    explicit SequentialByteStreamMarker(ByteStream const &stream);

    // implement ByteStreamMarker
    virtual FileSize getOffset() const;
};

inline FileSize ByteStream::getOffset() const
{
    return cbOffset;
}

FENNEL_END_NAMESPACE

#endif

// End ByteStream.h

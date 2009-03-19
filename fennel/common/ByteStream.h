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

#ifndef Fennel_ByteStream_Included
#define Fennel_ByteStream_Included

#include "fennel/common/ClosableObject.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteStream is a common base class for ByteInputStream and ByteOutputStream.
 */
class ByteStream : virtual public ClosableObject
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
class ByteStreamMarker
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
class SequentialByteStreamMarker : public ByteStreamMarker
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

/*
// $Id$
// Fennel is a relational database kernel.
// Copyright (C) 1999-2004 John V. Sichi.
//
// This program is free software; you can redistribute it and/or
// modify it under the terms of the GNU Lesser General Public License
// as published by the Free Software Foundation; either version 2.1
// of the License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with this program; if not, write to the Free Software
// Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
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
 * ByteStreamMarker is a position within a ByteStream.
 * It is created by ByteInputStream.mark
 * and consumed by ByteInputStream.reset.
 * To other classes it is opaque.
 */
class ByteStreamMarker
{
    friend class ByteInputStream;
    /**
     * Byte position in stream.
     */
    FileSize cbOffset;
    /**
     * Create a ByteStreamMarker (called by ByteInputStream.mark).
     */
    explicit inline ByteStreamMarker(FileSize cbOffset);            
};

inline FileSize ByteStream::getOffset() const
{
    return cbOffset;
}

inline ByteStreamMarker::ByteStreamMarker(FileSize cbOffset)
{
    this->cbOffset = cbOffset;
}

FENNEL_END_NAMESPACE

#endif

// End ByteStream.h

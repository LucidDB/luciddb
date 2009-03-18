/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2007 The Eigenbase Project
// Copyright (C) 2005-2007 Disruptive Tech
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

#ifndef Fennel_ByteArrayOutputStream_Included
#define Fennel_ByteArrayOutputStream_Included

#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteArrayOutputStream implements the ByteOutputStream interface by writing
 * data to an existing fixed-size array of bytes.
 */
class ByteArrayOutputStream : public ByteOutputStream
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

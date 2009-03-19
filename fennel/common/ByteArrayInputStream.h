/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2009 The Eigenbase Project
// Copyright (C) 2005-2009 SQLstream, Inc.
// Copyright (C) 2005-2009 LucidEra, Inc.
// Portions Copyright (C) 1999-2009 John V. Sichi
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

#ifndef Fennel_ByteArrayInputStream_Included
#define Fennel_ByteArrayInputStream_Included

#include "fennel/common/ByteInputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteArrayInputStream implements the ByteInputStream interface by reading data
 * from an existing fixed-size array of bytes.
 */
class ByteArrayInputStream : public ByteInputStream
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

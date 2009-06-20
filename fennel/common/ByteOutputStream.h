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

#ifndef Fennel_ByteOutputStream_Included
#define Fennel_ByteOutputStream_Included

#include "fennel/common/ByteStream.h"
#include "fennel/common/WriteLatency.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteOutputStream defines an interface for writing to a stream of bytes.
 */
class FENNEL_COMMON_EXPORT ByteOutputStream : virtual public ByteStream
{
    /**
     * Next position to write in output buffer.
     */
    PBuffer pNextByte;

    /**
     * Number of writable bytes remaining in output buffer.
     */
    uint cbWritable;

protected:
    /**
     * Current write latency mode.
     */
    WriteLatency writeLatency;

    explicit ByteOutputStream();

    /**
     * Must be implemented by derived class to flush buffered data.
     *
     * @param cbRequested if non-zero, the derived class should allocate a new
     * buffer with at least the requested size and call setBuffer
     */
    virtual void flushBuffer(uint cbRequested) = 0;

    virtual void closeImpl();

    /**
     * Sets the current buffer to be written.
     *
     * @param pBuffer receives start address of new buffer
     *
     * @param cbBuffer number of bytes in buffer
     */
    void setBuffer(PBuffer pBuffer, uint cbBuffer);

    /**
     * @return number of bytes remaining to be written in current buffer
     */
    uint getBytesAvailable() const;

public:
    /**
     * Writes bytes to the stream.
     *
     * @param pData source buffer containing bytes to be written
     *
     * @param cbRequested number of bytes to write
     */
    void writeBytes(void const *pData,uint cbRequested);

    /**
     * Copyless alternative for writing bytes to the stream.
     * Provides direct access to the stream's internal buffer, but doesn't
     * move the stream position (see consumeWritePointer).
     *
     * @param cbRequested number of contiguous bytes to access; if fewer
     * bytes are currently available in the buffer, the buffer is flushed and a
     * new buffer is returned
     *
     * @param pcbActual if non-NULL, receives actual number of contiguous
     * writable bytes, which will always be greater than or equal to
     * cbRequested
     *
     * @return pointer to cbActual bytes of writable buffer space
     */
    PBuffer getWritePointer(uint cbRequested, uint *pcbActual = NULL);

    /**
     * Advances stream position after a call to getWritePointer.
     *
     * @param cbUsed number of bytes to advance; must be less than or equal to
     * the value of cbActual returned by the last call to getWritePointer
     */
    void consumeWritePointer(uint cbUsed);

    /**
     * Marks the current buffer as complete regardless of how much data it
     * contains.  The exact semantics are dependent on the buffering
     * implementation.
     */
    void hardPageBreak();

    /**
     * Changes the write latency.  May not be meaningful for all stream
     * implementations.
     *
     * @param writeLatency new WriteLatency setting
     */
    virtual void setWriteLatency(WriteLatency writeLatency);

    /**
     * Writes a fixed-size type to the stream.
     *
     * @param value value to read; type must be memcpy-safe
     */
    template<class T>
    void writeValue(T const &value)
    {
        writeBytes(&value,sizeof(value));
    }
};

inline PBuffer ByteOutputStream::getWritePointer(
    uint cbRequested, uint *pcbActual)
{
    if (cbWritable < cbRequested) {
        flushBuffer(cbRequested);
        assert(cbWritable >= cbRequested);
    }
    if (pcbActual) {
        *pcbActual = cbWritable;
    }
    return pNextByte;
}

inline void ByteOutputStream::consumeWritePointer(uint cbUsed)
{
    assert(cbUsed <= cbWritable);
    cbWritable -= cbUsed;
    pNextByte += cbUsed;
    cbOffset += cbUsed;
}

inline void ByteOutputStream::setBuffer(PBuffer pBuffer, uint cbBuffer)
{
    pNextByte = pBuffer;
    cbWritable = cbBuffer;
}

inline uint ByteOutputStream::getBytesAvailable() const
{
    return cbWritable;
}

FENNEL_END_NAMESPACE

#endif

// End ByteOutputStream.h

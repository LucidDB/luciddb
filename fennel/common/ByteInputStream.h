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

#ifndef Fennel_ByteInputStream_Included
#define Fennel_ByteInputStream_Included

#include "fennel/common/ByteStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * ByteInputStream defines an interface for reading from a stream of bytes.
 */
class ByteInputStream : virtual public ByteStream
{
    /**
     * First buffered byte of data.
     */
    PConstBuffer pFirstByte;

    /**
     * Next buffered byte of data.
     */
    PConstBuffer pNextByte;

    /**
     * End of buffer (one past last byte of data in buffer).
     */
    PConstBuffer pEndByte;

protected:
    explicit ByteInputStream();

    /**
     * Must be implemented by derived class by calling either
     * setBuffer or nullifyBuffer.
     */
    virtual void readNextBuffer() = 0;

    /**
     * Must be implemented by derived class if seekBackward is
     * to be supported.
     */
    virtual void readPrevBuffer();

    /**
     * Sets the current buffer to be read.
     *
     * @param pBuffer receives start address of new buffer
     *
     * @param cbBuffer number of bytes in buffer
     */
    void setBuffer(
        PConstBuffer pBuffer,
        uint cbBuffer);

    /**
     * Nullifies the current buffer, indicating no more data is available.
     */
    void nullifyBuffer();

    /**
     * @return number of bytes remaining in current buffer
     */
    uint getBytesAvailable() const;

    /**
     * @return number of bytes already consumed from current buffer
     */
    uint getBytesConsumed() const;

public:
    /**
     * Reads bytes from the stream.
     *
     * @param pData target buffer to read into
     *
     * @param cbRequested number of bytes to read
     *
     * @return number of bytes actually read; 0 indicates end-of-stream
     */
    uint readBytes(void *pData,uint cbRequested);

    /**
     * Reads a fixed-size value from the stream.
     *
     * @param value value to write; type must be memcpy-safe
     *
     * @return number of bytes actually read
     */
    template<class T>
    uint readValue(T &value)
    {
        return readBytes(&value,sizeof(value));
    }

    /**
     * Copyless alternative for reading bytes from the stream.
     * Provides direct access to the stream's internal buffer, but doesn't
     * move the stream position (see consumeReadPointer).
     *
     *<p>
     *
     * Note that it is in general dangerous to assume that getReadPointer will
     * be able to access desired data items contiguously.  For example, a
     * stream created by calling ByteOutputStream::write is likely to have data
     * items split across buffers.  The assumption MAY be valid for streams
     * created by calling ByteOutputStream::getWritePointer with matching
     * values for cbRequested; it depends on the stream implementation.
     *
     * @param cbRequested number of contiguous bytes to access; if a non-zero
     * number of bytes are currently available in the buffer, cbRequested must
     * be no greater than the number of available bytes (otherwise an assertion
     * violation will result)
     *
     * @param pcbActual if non-NULL, receives actual number of contiguous bytes
     * available, which will always be greater than or equal to cbRequested
     * except 0 for end-of-stream
     *
     * @return pointer to cbActual bytes of available data, or NULL for
     * end-of-stream
     */
    PConstBuffer getReadPointer(uint cbRequested,uint *pcbActual = NULL);

    /**
     * Advances stream position after a call to getReadPointer.
     *
     * @param cbUsed number of bytes to advance; must be less than or equal to
     * the value of cbActual returned by the last call to getReadPointer
     */
    void consumeReadPointer(uint cbUsed);

    /**
     * Skips forward in stream.
     *
     * @param cb number of bytes to advance
     */
    void seekForward(uint cb);

    /**
     * Skips backward in stream.  Not all stream implementations support this
     * behavior.
     *
     * @param cb number of bytes backward to seek
     */
    void seekBackward(uint cb);


    /**
     * Creates a new uninitialized marker for this stream.  The returned marker
     * must be passed to mark() in order to initialize it.
     *
     * @return shared pointer to new marker
     */
    virtual SharedByteStreamMarker newMarker();

    /**
     * Marks the current stream position in preparation for a future call to
     * reset().  How long this marker remains valid depends upon the
     * implementation of the ByteInputStream.
     *
     * @param marker memento object created with newMarker() on the same
     * stream; receives the marked position (forgetting any previously
     * marked position)
     */
    virtual void mark(ByteStreamMarker &marker);

    /**
     * Resets stream to a previously marked position.  The base implementation
     * uses seekForward/seekBackward (i.e. sequential access), making
     * it inefficient for large streams.  Derived classes may override
     * with more efficient implementations such as random access.
     *
     * @param marker memento previously memorized with mark()
     */
    virtual void reset(ByteStreamMarker const &marker);
};

inline uint ByteInputStream::getBytesAvailable() const
{
    return static_cast<uint>(pEndByte - pNextByte);
}

inline uint ByteInputStream::getBytesConsumed() const
{
    return static_cast<uint>(pNextByte - pFirstByte);
}

inline PConstBuffer ByteInputStream::getReadPointer(
    uint cbRequested,uint *pcbActual)
{
    if (getBytesAvailable() < cbRequested) {
        assert(pNextByte == pEndByte);
        readNextBuffer();
        if (pNextByte == pEndByte) {
            if (pcbActual) {
                *pcbActual = 0;
            }
            return NULL;
        }
    }
    if (pcbActual) {
        *pcbActual = getBytesAvailable();
    }
    return pNextByte;
}

inline void ByteInputStream::seekForward(uint cb)
{
    uint cbActual = readBytes(NULL,cb);
    assert(cbActual == cb);
}

inline void ByteInputStream::consumeReadPointer(uint cbUsed)
{
    assert(cbUsed <= getBytesAvailable());
    pNextByte += cbUsed;
    cbOffset += cbUsed;
}

inline void ByteInputStream::setBuffer(
    PConstBuffer pBuffer,
    uint cbBuffer)
{
    pFirstByte = pBuffer;
    pEndByte = pBuffer + cbBuffer;
    pNextByte = pFirstByte;
}

inline void ByteInputStream::nullifyBuffer()
{
    setBuffer(NULL,0);
}

FENNEL_END_NAMESPACE

#endif

// End ByteInputStream.h

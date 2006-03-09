/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 LucidEra, Inc.
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

#ifndef Fennel_ByteBuffer_Included
#define Fennel_ByteBuffer_Included

#include "fennel/common/CommonPreamble.h"

#include <boost/shared_array.hpp>

FENNEL_BEGIN_NAMESPACE

typedef uint8_t UnsignedByte;
    
/**
 * ByteBuffer allows access to an array of buffers as a single
 * memory space. It allows for optimization with direct memory access.
 * It has an interface for accessing one byte at a time (less overhead),
 * and an interface for accessing runs of bytes (more overhead, but
 * processes one memory chunk at a time). The methods setMem and
 * copyMem are similar to memset and memcpy.
 *
 * This class neither allocates nor deallocates memory, except through
 * shared pointers. As usual, indexes are zero-based.
 *    
 * @author John Pham
 * @version $Id$
 */
class ByteBuffer
{
    boost::shared_array<PBuffer> ppBuffers;
    uint nBuffers;
    uint bufferSize;
    uint bufferMask;
    uint bufferShift;

    /**
     * Merges (OR's) one buffer into another
     */
    static void memmerge(PBuffer dest, PConstBuffer src, uint len) 
    {
        PBuffer end = dest + len;
        while (dest < end) {
            *dest++ |= *src++;
        }
    }

    /**
     * Returns offsets to use for a buffer access
     */
    inline void getOffset(uint pos, uint &i, uint &j)
    {
        i = pos >> bufferShift;
        j = pos & bufferMask;
        assert(i < nBuffers);
        assert(j < bufferSize);
    }

public:
    explicit ByteBuffer();
    ~ByteBuffer();

    /**
     * Provides storage for the virtual byte buffer 
     */
    void init(
        boost::shared_array<PBuffer> ppBuffers, uint nBuffers, uint bufSize);

    /**
     * Returns the size of the buffer, in bytes
     */
    uint getSize();

    /**
     * Returns the byte at pos
     */
    inline UnsignedByte getByte(uint pos) 
    {
        uint i, j;
        getOffset(pos, i, j);
        return ppBuffers[i][j];
    }

    /**
     * Sets byte at pos
     */
    inline void setByte(uint pos, UnsignedByte b)
    {
        uint i, j;
        getOffset(pos, i, j);
        ppBuffers[i][j] = b;
    }

    /**
     * Merges (OR's) byte at pos
     */
    inline void mergeByte(uint pos, UnsignedByte b)
    {
        uint i, j;
        getOffset(pos, i, j);
        ppBuffers[i][j] |= b;
    }

    /**
     * Returns the size of contiguous memory starting from a position
     *
     * @param pos memory position
     * @param max if nonzero, limits the memory size returned
     */
    inline uint getContiguousMemSize(uint pos, uint max)
    {
        uint size = bufferSize - (pos & bufferMask);
        return max ? std::min(max, size) : size;
    }

    /**
     * Returns a pointer to contiguous memory.
     *
     * @param pos memory position
     */
    inline PBuffer getMem(uint pos)
    {
        uint i, j;
        getOffset(pos, i, j);
        return &ppBuffers[i][j];
    }

    /**
     * Initializes a run of bytes in the buffer
     */
    void setMem(uint pos, UnsignedByte value, uint len);

    /**
     * Copies a run of bytes into the buffer
     */
    void copyMem(uint pos, PConstBuffer mem, uint len);

    /**
     * Merges (OR's) a run of bytes into the buffer; similar to memmerge
     */
    void mergeMem(uint pos, PConstBuffer mem, uint len);
};

/**
 * This class represents an ascending circular buffer. It's like an array
 * whose start and end are not fixed. At any time the buffer is valid for
 * entries between start and end. However, the start may be increased, as
 * data is read. The end may be increased as more data is written. The
 * amount of data is limited by the buffer's capacity, and data cannot be
 * written past the limit. 
 *
 * Note that other implementations differ from this one, because they focus
 * on reader/writer control and synchronization. Unlike other
 * implementations, this buffer does NOT provide synchronization.
 *
 * The contents of a CircularBuffer can be considered to be initialized
 * to zero. The CircularBuffer neither allocates nor frees any memory.
 * It is templatized to support different kinds of indexes, such as opaque
 * integers.
 */
template <class IndexT>
class AbstractCircularBuffer
{
protected:
    /**
     * Internal buffer
     */
    SharedByteBuffer buffer;

    /**
     * Size of internal buffer
     */
    uint bufferSize;

    /**
     * Offset of current start index
     */
    uint startOffset;

    /**
     * Current start index; buffer has no data for index < start
     */
    IndexT start;

    /**
     * Index not yet written; buffer has no data for index >= end
     */
    IndexT end;

    /**
     * Retrieves the offset of index in buffer. This call will
     * fail if the index is invalid
     */
    inline uint getOffset(IndexT index)  const
    {
        assert(index >= start);
        assert(index < start + bufferSize);
        return indexToOffset((index - start + startOffset) % bufferSize);
    }

    /**
     * Casts an index to an integer offset
     */
    virtual uint indexToOffset(IndexT index) const = 0;

    /**
     * Returns size of buffer starting at index, which is not wrapped
     * by the circular buffer. Returns up to max.
     */
    inline uint getUnwrappedMemSize(IndexT index, uint max) 
    {
        uint len = bufferSize - getOffset(index);
        return max ? std::min(len, max) : len;
    }

public:
    virtual ~AbstractCircularBuffer() 
    {}

    /**
     * Initialize a buffer, valid from index 0
     */
    void init(SharedByteBuffer buffer)
    {
        this->buffer = buffer;
        bufferSize = buffer->getSize();
        reset();
    }

    /**
     * Reinitializes buffer to empty condition
     */
    void reset()
    {
        startOffset = 0;
        start = end = (IndexT) 0;
    }

    /**
     * Returns the capacity of the buffer
     */
    inline uint getCapacity() const
    {
        return bufferSize;
    }

    /**
     * Returns the index of the first entry stored in the buffer
     */
    inline IndexT getStart() const
    {
        return start;
    }

    /**
     * Returns the index of the end of data (the first entry with no data)
     */
    inline IndexT getEnd() const
    {
        return end;
    }

    /**
     * Returns the write limit of the buffer (the first invalid index)
     */
    inline IndexT getLimit()
    {
        return start + bufferSize;
    }

    /**
     * Returns the value of the byte at the specified index
     */
    inline UnsignedByte getByte(IndexT index) const
    {
        assert(index >= start && index < end);
        return buffer->getByte(getOffset(index));
    }

    /**
     * Returns the size of contiguous memory available at a position
     */
    virtual uint getContiguousMemSize(IndexT index, uint max)
    {
        return buffer->getContiguousMemSize(getOffset(index), max);
    }

    /**
     * Returns contiguous memory at position or NULL
     */
    virtual PBuffer getMem(IndexT index)
    {
        return buffer->getMem(getOffset(index));
    }

    /**
     * Advances the start of the buffer, releasing part of the
     * buffer to store more values.
     */
    void advance(IndexT pos) 
    {
        assert (pos >= start);

        // If we not keeping any data, reset the start pointer to the
        // beginning of the internal buffer. Otherwise, advance it.
        if (pos >= end) {
            startOffset = 0;
            end = pos;
        } else {
            startOffset = getOffset(pos);
        }
        start = pos;
    }

    /**
     * Advances the end pointer, initializing new entries to zero
     */
    void advanceEnd(IndexT pos)
    {
        assert(pos > end);
        assert(pos <= getLimit());

        uint len = indexToOffset(pos - end);
        uint chunkSize = getUnwrappedMemSize(end, len);
        buffer->setMem(getOffset(end), 0, chunkSize);
        if (chunkSize < len) {
            buffer->setMem(0, 0, len - chunkSize);
        }
        end = pos;
    }

    /**
     * Merge a byte into the buffer
     */
    void mergeByte(IndexT index, UnsignedByte val) 
    {
        assert(index >= start);
        assert(index < getLimit());
        if (index >= end) {
            advanceEnd(index + 1);
        }
        buffer->mergeByte(getOffset(index), val);
    }

    /**
     * Merge a series of bytes into the buffer
     */
    void mergeMem(IndexT index, PConstBuffer byteSeg, uint len)
    {
        assert(index >= start);
        assert(len < bufferSize);
        assert(index + len <= getLimit());
        assert(len > 0);
        
        // zero from end to index
        IndexT writeEnd = index + len;
        if (writeEnd > end) {
            advanceEnd(writeEnd);
        }

        // write byte-wise for small writes
        if (len < 3) {
            for (uint i = 0; i < len; i++) {
                buffer->mergeByte(getOffset(index + i), byteSeg[i]);
            }
            return;
        }

        uint chunkSize = getUnwrappedMemSize(index, len);
        buffer->mergeMem(getOffset(index), byteSeg, chunkSize);
        if (chunkSize < len) {
            buffer->mergeMem(0, byteSeg + chunkSize, len - chunkSize);
        }
    }
};

template <class IndexT>
class CircularBuffer : public AbstractCircularBuffer<IndexT>
{
    inline uint indexToOffset(IndexT index) const
    {
        return (uint) index;
    }
};

template <class IndexT>
class OpaqueIndexedCircularBuffer : public AbstractCircularBuffer<IndexT>
{
    inline uint indexToOffset(IndexT index) const
    {
        return (uint) opaqueToInt(index);
    }
};

FENNEL_END_NAMESPACE

#endif

// End ByteBuffer.h

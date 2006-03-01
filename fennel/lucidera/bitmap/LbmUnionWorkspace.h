/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 LucidEra, Inc.
// Copyright (C) 2005-2005 The Eigenbase Project
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

#ifndef Fennel_LbmUnionWorkspace_Included
#define Fennel_LbmUnionWorkspace_Included

#include "fennel/common/CommonPreamble.h"
#include "fennel/lucidera/bitmap/LbmSegment.h"

FENNEL_BEGIN_NAMESPACE

/**
 * This class encapsulates a single byte segment, as opposed to
 * a tuple which contains a set of them
 */    
class LbmByteSegment
{
public:
    LcsRid byteNum;
    PBuffer byteSeg;
    uint len;

    inline void reset() 
    {
        byteNum = (LcsRid) 0;
        byteSeg = NULL;
        len = 0;
    }
    
    inline LcsRid getSrid() const
    {
        return (LcsRid) (byteNum * LbmSegment::LbmOneByteSize);
    }

    inline bool isNull() const
    {
        return byteSeg == NULL;
    }
};

/**
 * This class represents an ascending circular buffer
 */
template <class IndexT>
class CircularBuffer
{
    PBuffer buffer;
    uint bufferSize;
    uint startOffset;
    IndexT startIndex;

    /**
     * Retrieves the offset of index in buffer. This call will
     * fail if the index is invalid
     */
    uint getOffset(IndexT index)  const
    {
        assert(index >= startIndex);
        assert(index < startIndex + bufferSize);
        return indexToOffset((index - startIndex) % bufferSize);
    }

    /**
     * Merges one memory buffer onto another
     */
    void memMerge(PBuffer dest, PBuffer src, uint len) 
    {
        assert(dest >= buffer && (dest + len) < (buffer + bufferSize));
        while (len > 0) {
            *dest++ |= *src++;
            len--;
        }
    }

    /**
     * Extracts the integer value of an index
     */
    virtual uint indexToOffset(IndexT index) const = 0;
    
public:
    virtual ~CircularBuffer() {};

    /**
     * Initialize a new zeroed buffer spanning from 0 to size - 1
     */
    void init(PBuffer buffer, uint bufferSize) 
    {
        this->buffer = buffer;
        this->bufferSize = bufferSize;
        reset();
    }

    /**
     * Returns the capacity of the buffer
     */
    uint getCapacity() 
    {
        return bufferSize;
    }

    /**
     * Reinitialize the buffer to initialized condition
     */
    void reset() 
    {
        startOffset = 0;
        startIndex = (IndexT) 0;
        memset(buffer, 0, bufferSize);
    }

    IndexT getStartByte() const
    {
        return startIndex;
    }

    /**
     * Moves the start of the buffer to a new location, releasing
     * part of the buffer to store more values. Initializes the 
     * released portion to zero. If buffer is already past min,
     * then this method has no effect;
     */
    void setMin(IndexT min) 
    {
        if (min < startIndex) {
            return;
        }
        if (min >= startIndex + bufferSize) {
            // initialize a completely new buffer
            startOffset = 0;
            startIndex = min;
            memset(buffer, 0, bufferSize);
        } else {
            // initialize part of the existing buffer
            for (IndexT i = startIndex; i < min; i++) {
                setByte(i, 0);
            }
            startOffset = getOffset(min);
            startIndex = min;
        }
    }

    /**
     * Returns the value of the byte at the specified index
     */
    uint8_t getByte(IndexT index) const
    {
        return buffer[getOffset(index)];
    }

    /**
     * Sets the value of the byte at the given index
     */
    void setByte(IndexT index, uint8_t val)
    {
        buffer[getOffset(index)] = val;
    }
    
    /**
     * Sets the value of the byte at the given index to the
     * current value OR'd with the new value
     */
    void mergeByte(IndexT index, uint8_t val)
    {
        buffer[getOffset(index)] |= val;
    }

    /**
     * Merge a series of bytes
     */
    bool mergeByteSegment(IndexT index, PBuffer byteSeg, uint len)
    {
        assert(len < bufferSize);
        if (index + len > startIndex + bufferSize) {
            return false;
        }
        
        uint offset = getOffset(index);
        uint trailingSize = std::min(bufferSize - offset, len);
        memMerge(buffer + offset, byteSeg, trailingSize);
        if (len > trailingSize) {
            uint leadingSize = len - trailingSize;
            memMerge(buffer, byteSeg + trailingSize, leadingSize);
        }
        return true;
    }
};

class LbmUnionMergeArea : public CircularBuffer<LcsRid>
{
    inline uint indexToOffset(LcsRid index) const
    {
        return (uint) opaqueToInt(index);
    }
};

/**
 * The union workspace merges byte segments
 *
 * @author John Pham
 * @version $Id$
 */
class LbmUnionWorkspace
{
    /**
     * Buffer used to merge segments, indexed by ByteNumber
     */
    LbmUnionMergeArea mergeArea;

    /**
     * Buffer used to build a contiguous segment
     */
    PBuffer segmentArea;

    /**
     * Maximum size of a segment that can be produced by this workspace
     */
    uint maxSegmentSize;

    /**
     * Whether there is a limit on production
     */
    bool limited;

    /**
     * Byte number of the last byte which can be produced
     */
    LcsRid productionLimitByte;

    /**
     * Highest byte written to merge area
     */
    LcsRid highestByte;

    /**
     * A segment that can be returned by the workspace
     */
    LbmByteSegment segment;

    /**
     * Returns the byte number the row id is located in
     */
    inline LcsRid getByteNumber(LcsRid rid);

public:
    /**
     * Initialize the workspace
     */
    void init(PBuffer buffer, uint bufferSize, uint maxSegmentSize);

    /**
     * Returns the maximum tuple size the workspace can handle and
     * still produce segments of maxSegmentSize
     */
    uint getRidLimit();

    /**
     * Empty the workspace
     */
    void reset();

    /**
     * Advance the workspace to the requested Srid
     */
    void advance(LcsRid requestedSrid);

    /**
     * Advance the workspace past the current workspace segment
     * Precondition is that segment must be set.
     */
    void advanceSegment();

    /**
     * Sets the upper bound of production; the workspace will not
     * allow a segment to be added within the bounds of production
     */
    void setLimit(LcsRid productionLimitRid);

    /**
     * Remove production limit; this allows the workspace to flush
     * its entire contents; no more segments can be added after this
     * call
     */
    void removeLimit();

    /**
     * Whether the workspace is completely empty
     */
    bool isEmpty() const;

    /**
     * Whether the workspace is able to produce a complete segments;
     * segments currently being extended will not be produced
     */
    bool canProduce();

    /**
     * Returns the current segment
     */
    const LbmByteSegment &getSegment();

    /**
     * Adds a segment to the workspace; the segment must not fall within
     * the current bounds of production
     */
    bool addSegment(const LbmByteSegment &segment);
};

inline LcsRid LbmUnionWorkspace::getByteNumber(LcsRid rid)    
{
    return rid / LbmSegment::LbmOneByteSize;
}

FENNEL_END_NAMESPACE

#endif

// End LbmUnionWorkspace.h

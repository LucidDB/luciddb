/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005 The Eigenbase Project
// Copyright (C) 2005 SQLstream, Inc.
// Copyright (C) 2005 Dynamo BI Corporation
// Portions Copyright (C) 1999 John V. Sichi
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

#ifndef Fennel_CircularBuffer_Included
#define Fennel_CircularBuffer_Included

FENNEL_BEGIN_NAMESPACE

/**
 * A circular buffer containing a maximum of N entries of type T.  The max
 * size of the buffer must be set before the buffer can be used.
 */
template <class T>
class CircularBuffer
{
    /**
     * Vector used to represent the contents of the circular buffer
     */
    std::vector<T> buffer;

    /**
     * Number of entries in the buffer
     */
    uint nEntries;

    /**
     * Number of free entries in the buffer
     */
    uint nFreeEntries;

    /**
     * Position of the current, first entry in the circular buffer.  Position
     * always increases, even after positions in the circular buffer are
     * recycled.
     */
    uint firstPos;

    /**
     * If true, no additional new entries can be added to the buffer
     */
    bool readOnly;

    /**
     * Initializes the buffer.
     *
     * @param nEntriesInit size of the buffer
     */
    void init(uint nEntriesInit)
    {
        nEntries = nEntriesInit;
        buffer.resize(nEntries);
        clear();
    }

public:
    explicit CircularBuffer()
    {
        init(0);
    }

    explicit CircularBuffer(uint nEntriesInit)
    {
        init(nEntriesInit);
    }

    /**
     * Initializes the buffer to an empty state.
     */
    void clear()
    {
        firstPos = 0;
        nFreeEntries = nEntries;
        readOnly = false;
    }

    /**
     * Resizes the number of entries in the circular buffer.  The buffer
     * positions are also reset back to their initial state, so any
     * existing entries stored in the buffer are ignored.
     *
     * @param nEntriesInit new number of entries
     */
    void resize(uint nEntriesInit)
    {
        init(nEntriesInit);
    }

    /**
     * @return max number of entries that can be stored in the buffer
     */
    uint size()
    {
        return nEntries;
    }

    /**
     * @return true if the buffer is empty
     */
    bool empty()
    {
        return (nFreeEntries == nEntries);
    }

    /**
     * @return true if there is space available in the buffer
     */
    bool spaceAvailable()
    {
        return (nFreeEntries != 0);
    }

    /**
     * @return number of free entries available in the buffer
     */
    uint nFreeSpace()
    {
        return nFreeEntries;
    }

    /**
     * @return the position of the first entry in the buffer
     */
    uint getFirstPos()
    {
        assert(!empty());
        return firstPos;
    }

    /**
     * @return the position of the last entry in the buffer
     */
    uint getLastPos()
    {
        return nEntries - nFreeEntries + firstPos - 1;
    }

    /**
     * @return reference to the last entry in the buffer
     */
    T &reference_back()
    {
        assert(!empty());
        uint lastEntry = getLastPos() % nEntries;
        return buffer[lastEntry];
    }

    /**
     * Adds an entry to the end of the buffer.
     */
    void push_back(T &newEntry)
    {
        assert(!readOnly);
        assert(nFreeEntries > 0);
        uint freeEntry = (nEntries - nFreeEntries + firstPos) % nEntries;
        buffer[freeEntry] = newEntry;
        nFreeEntries--;
    }

    /**
     * @return reference to the first entry in the buffer
     */
    T &reference_front()
    {
        assert(!empty());
        return buffer[firstPos % nEntries];
    }

    /**
     * Removes the first entry from the buffer.
     */
    void pop_front()
    {
        assert(!empty());
        firstPos++;
        nFreeEntries++;
    }

    /**
     * Returns the entry at a specified position in the circular buffer.
     *
     * @param pos the position
     *
     * @return reference to the entry
     */
    T & operator [] (uint pos)
    {
        return buffer[pos % nEntries];
    }

    /**
     * @return true if the buffer is readonly
     */
    bool isReadOnly()
    {
        return readOnly;
    }

    /**
     * Sets the buffer to a readonly state.
     */
    void setReadOnly()
    {
        readOnly = true;
    }
};

/**
 * Iterator over a circular buffer.  The iterator starts at the 0th position
 * in the underlying circular buffer and always increases, even as positions
 * in the underlying circular buffer are recycled.
 *
 * <p>
 * Incrementing this iterator has not affect on the contents of the underlying
 * buffer.
 */
template <class T>
class CircularBufferIter
{
    /**
     * Underlying circular buffer object
     */
    CircularBuffer<T> *pCircularBuffer;

    /**
     * Current position of this iterator within the underlying circular
     * buffer
     */
    uint currPos;

public:

    explicit CircularBufferIter(CircularBuffer<T> *pCircularBufferInit)
    {
        pCircularBuffer = pCircularBufferInit;
        reset();
    }

    /**
     * Resets the iterator to the initial starting position
     */
    void reset()
    {
        currPos = 0;
    }

    /**
     * Increments the iterator position.
     */
    void operator ++ ()
    {
        assert(!pCircularBuffer->empty());
        currPos++;
    }

    /**
     * @return reference to the entry at the current iterator position
     */
    T & operator * ()
    {
        assert(!pCircularBuffer->empty());
        assert(
            currPos >= pCircularBuffer->getFirstPos()
            && currPos <= pCircularBuffer->getLastPos());
        return (*pCircularBuffer)[currPos];
    }

    /**
     * @return true if the the iterator is positioned past the end of the
     * buffer
     */
    bool end()
    {
        return pCircularBuffer->empty()
            || currPos > pCircularBuffer->getLastPos();
    }

    /**
     * @return true if all possible entries in the underlying buffer have been
     * iterated over
     */
    bool done()
    {
        return end() && pCircularBuffer->isReadOnly();
    }

    /**
     * @return current position within the iterator
     */
    uint getCurrPos()
    {
        return currPos;
    }

    /**
     * Sets the current position of the iterator.
     *
     * @param pos the posititon to be set
     */
    void setCurrPos(uint pos)
    {
        currPos = pos;
    }

    /**
     * Removes the first entry from the underlying circular buffer.
     */
    void removeFront()
    {
        pCircularBuffer->pop_front();
    }
};

FENNEL_END_NAMESPACE

#endif

// End CircularBuffer.h

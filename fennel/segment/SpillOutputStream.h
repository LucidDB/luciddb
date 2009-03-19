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

#ifndef Fennel_SpillOutputStream_Included
#define Fennel_SpillOutputStream_Included

#include "fennel/common/ByteOutputStream.h"
#include "fennel/segment/SegPageLock.h"

FENNEL_BEGIN_NAMESPACE

class LogicalTxn;

/**
 * SpillOutputStream implements the ByteOutputStream interface by
 * starting with writes to a cache scratch page.  If this overflows, the
 * contents are spilled to a new SegOutputStream to which all further writes
 * are directed.
 */
class SpillOutputStream : public ByteOutputStream
{
    /**
     * Factory to use for creating spill segment if necessary.
     */
    SharedSegmentFactory pSegmentFactory;

    /**
     * CacheAccessor to use for accessing spill segment if necessary.
     */
    SharedCacheAccessor pCacheAccessor;

    /**
     * Spill segment output stream.
     */
    SharedSegOutputStream pSegOutputStream;

    /**
     * Total number of bytes locked in current page.
     */
    uint cbBuffer;

    /**
     * Accessor for scratch segment.
     */
    SegmentAccessor scratchAccessor;

    /**
     * Page lock on scratch page for short streams.
     */
    SegPageLock scratchPageLock;

    /**
     * Filename to assign to spill segment.
     */
    std::string spillFileName;

    // implement the ByteOutputStream interface
    virtual void flushBuffer(uint cbRequested);
    virtual void closeImpl();

    void spill();
    void updatePage();

    explicit SpillOutputStream(
        SharedSegmentFactory,
        SharedCacheAccessor,
        std::string);

public:
    /**
     * Creates a new SpillOutputStream.
     *
     * @param pSegmentFactory the SegmentFactory to use if the output stream
     * spills
     *
     * @param pCacheAccessor the CacheAccessor to use
     *
     * @param spillFileName filename to assign to spill segment
     */
    static SharedSpillOutputStream newSpillOutputStream(
        SharedSegmentFactory pSegmentFactory,
        SharedCacheAccessor pCacheAccessor,
        std::string spillFileName);

    virtual ~SpillOutputStream();

    /**
     * Obtains a ByteInputStream suitable for accessing the contents of this
     * SpillOutputStream.  If spill has already occurred, then this is a
     * SegInputStream, otherwise a ByteArrayInputStream.
     *
     * @param seekPosition if SEEK_STREAM_BEGIN (the default), the input stream
     * is initially positioned before the first byte of stream data; otherwise,
     * after the last byte
     *
     * @return new ByteInputStream
     */
    SharedByteInputStream getInputStream(
        SeekPosition seekPosition = SEEK_STREAM_BEGIN);

    /**
     * Obtains a reference to the underlying segment if this stream has
     * spilled.
     *
     * @return the segment, or a singular pointer if the stream has not spilled
     */
    SharedSegment getSegment();

    /**
     * Obtains a reference to the underlying SegOutputStream if this stream has
     * spilled.
     *
     * @return the stream, or a singular pointer if the stream has not spilled
     */
    SharedSegOutputStream getSegOutputStream();

    // override ByteOutputStream
    virtual void setWriteLatency(WriteLatency writeLatency);
};

FENNEL_END_NAMESPACE

#endif

// End SpillOutputStream.h

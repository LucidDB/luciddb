/*
// $Id$
// Fennel is a library of data storage and processing components.
// Copyright (C) 2005-2005 The Eigenbase Project
// Copyright (C) 2005-2005 Disruptive Tech
// Copyright (C) 2005-2005 Red Square, Inc.
// Portions Copyright (C) 1999-2005 John V. Sichi
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

#ifndef Fennel_SegStream_Included
#define Fennel_SegStream_Included

#include "fennel/common/ByteStream.h"
#include "fennel/segment/SegPageLock.h"
#include "fennel/common/PseudoUuid.h"

FENNEL_BEGIN_NAMESPACE

// NOTE:  read comments on struct StoredNode before modifying
// SegStreamNode

/**
 * Header stored on each page of a SegStream.
 */
struct SegStreamNode : public StoredNode
{
    static const MagicNumber MAGIC_NUMBER = 0x99f28198d53750a5LL;
    
    uint cbData;
};

/**
 * Additional header information stored on each page of SegStreams
 * for which CRC's are requested.
 */
struct SegStreamCrc
{
    /**
     * The PageId of this page relative to the segment storing the stream.
     */
    PageId pageId;

    /**
     * The UUID of the online instance that wrote this page.
     */
    PseudoUuid onlineUuid;

    /**
     * CRC computed for this page.
     */
    uint64_t checksum;
};

typedef SegNodeLock<SegStreamNode> SegStreamLock;

/**
 * Memento for a position within a SegStream.
 */
struct SegStreamPosition 
{
    /**
     * Physical position.
     */
    SegByteId segByteId;

    /**
     * Logical position.
     */
    FileSize cbOffset;
};

/**
 * SegStream is a common base for SegInputStream and SegOutputStream.
 */
class SegStream : virtual public ByteStream
{
protected:
    /**
     * Accessor for segment containing stream data.
     */
    SegmentAccessor segmentAccessor;

    /**
     * Lock held on current page, if any.
     */
    SegStreamLock pageLock;

    /**
     * Number of bytes in page header.
     */
    uint cbPageHeader;

    // implement the ClosableObject interface
    virtual void closeImpl();

    explicit SegStream(SegmentAccessor const &,uint cbExtraHeader);
public:

    /**
     * Obtains the current stream position.
     *
     * @param pos receives the position
     */
    virtual void getSegPos(SegStreamPosition &pos) = 0;

    SharedSegment getSegment() const;
};

/**
 * SegStreamMarker refines ByteStreamMarker with a physical stream
 * position, allowing for random-access mark/reset.
 */
class SegStreamMarker : public ByteStreamMarker
{
    friend class SegInputStream;
    
    /**
     * Position for random-access mark/reset.
     */
    SegStreamPosition segPos;

    explicit SegStreamMarker(SegStream const &segStream);
    
    // implement ByteStreamMarker
    virtual FileSize getOffset() const;
};

FENNEL_END_NAMESPACE

#endif

// End SegStream.h

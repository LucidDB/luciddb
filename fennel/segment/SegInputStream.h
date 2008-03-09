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

#ifndef Fennel_SegInputStream_Included
#define Fennel_SegInputStream_Included

#include "fennel/segment/SegStream.h"
#include "fennel/common/ByteInputStream.h"
#include "fennel/segment/SegPageIter.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegInputStream implements the ByteInputStream interface by reading data
 * previously written via a SegOutputStream.  seekBackward is supported, but
 * only if the underlying segment supports CONSECUTIVE_ALLOCATION or
 * LINEAR_ALLOCATION.
 */
class SegInputStream : public SegStream, public ByteInputStream
{
protected:
    SegPageIter pageIter;
    
    /**
     * PageId of current page.
     */
    PageId currPageId;

    /**
     * Whether to deallocate stream pages when no longer needed.
     */
    bool shouldDeallocate;

    // implement the ByteInputStream interface
    virtual void readNextBuffer();
    virtual void readPrevBuffer();
    virtual void closeImpl();

    explicit SegInputStream(
        SegmentAccessor const &,PageId,uint cbExtraHeader = 0);
    
    virtual void lockBuffer();
    
public:
    /**
     * Creates a new SegInputStream, positioned
     * at beginning-of-stream.
     *
     * @param segmentAccessor accessor for the segment containing the stream
     * data
     * 
     * @param beginPageId the first page of stream data; if the default
     * FIRST_LINEAR_PAGE_ID is passed, the segment must support
     * LINEAR_ALLOCATION, and the stream starts at the first page of the
     * segment; if NULL_PAGE_ID is passed, an empty stream is returned
     *
     * @return shared_ptr to new SegInputStream
     */
    static SharedSegInputStream newSegInputStream(
        SegmentAccessor const &segmentAccessor,
        PageId beginPageId = FIRST_LINEAR_PAGE_ID);

    /**
     * Requests that prefetch be performed in anticipation of forward scan.
     */
    void startPrefetch();

    /**
     * Disables prefetch.
     */
    void endPrefetch();

    /**
     * Requests that pages be deallocated after the stream is done reading
     * them.  If in effect, any unread pages will also be deallocated when
     * the stream is closed.  Note that usage of deallocation may cause other
     * methods such as seekSegPos and seekBackward to fail.
     *
     * @param shouldDeallocate whether to deallocate pages
     */
    void setDeallocate(bool shouldDeallocate);

    /**
     * @return true if currently set to deallocate pages as they are read
     */
    bool isDeallocating();

    // implement the SegStream interface
    virtual void getSegPos(SegStreamPosition &pos);

    /**
     * Seeks to a previously recorded stream position.
     *
     * @param pos the new position, previously returned by getSegPos
     */
    void seekSegPos(SegStreamPosition const &pos);

    // override ByteInputStream
    virtual SharedByteStreamMarker newMarker();
    
    // override ByteInputStream
    virtual void mark(ByteStreamMarker &marker);

    // override ByteInputStream
    virtual void reset(ByteStreamMarker const &marker);
};

FENNEL_END_NAMESPACE

#endif

// End SegInputStream.h

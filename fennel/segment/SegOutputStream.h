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

#ifndef Fennel_SegOutputStream_Included
#define Fennel_SegOutputStream_Included

#include "fennel/segment/SegStream.h"
#include "fennel/common/ByteOutputStream.h"

FENNEL_BEGIN_NAMESPACE

/**
 * SegOutputStream implements the ByteOutputStream interface by writing data
 * to pages allocated from a Segment.  The Segment must support the
 * get/setPageSuccessor interface for chaining the pages together.
 */
class SegOutputStream : public SegStream, public ByteOutputStream
{
protected:
    /**
     * First PageId allocated.
     */
    PageId firstPageId;

    /**
     * Last PageId allocated.
     */
    PageId lastPageId;

    /**
     * Maximum number of data bytes which can be stored per page
     * (does not include header).
     */
    uint cbMaxPageData;

    /**
     * Counter for getPageCount().
     */
    BlockNum nPagesAllocated;

    /**
     * @return number of bytes already written to current page
     */
    uint getBytesWrittenThisPage() const;

    // implement the ByteOutputStream interface
    virtual void flushBuffer(uint cbRequested);
    virtual void closeImpl();

    explicit SegOutputStream(SegmentAccessor const &,uint cbExtraHeader = 0);

    /**
     * Hook for subclasses; default is to do nothing.
     *
     * @param node the node being flushed
     */
    virtual void writeExtraHeaders(SegStreamNode &node);

public:
    /**
     * Creates a new SegOutputStream.
     *
     * @param segmentAccessor accessor for the segment in which to store the
     * data
     *
     * @return shared_ptr to new SegOutputStream
     */
    static SharedSegOutputStream newSegOutputStream(
        SegmentAccessor const &segmentAccessor);

    /**
     * Gets the first PageId allocated.  For non-linear segments, this is
     * required in order to be able to read the data back via SegInputStream.
     *
     * @return the first PageId allocated, or NULL_PAGE_ID if no data has been
     * written to the stream yet
     */
    PageId getFirstPageId() const;

    /**
     * @return the number of pages allocated by this stream
     */
    BlockNum getPageCount() const;

    /**
     * Updates current page header without unlocking; allows
     * a SegInputStream to read the contents from the same thread
     * (but another thread would be locked out).
     */
    void updatePage();

    // implement the SegStream interface
    virtual void getSegPos(SegStreamPosition &pos);
};

FENNEL_END_NAMESPACE

#endif

// End SegOutputStream.h

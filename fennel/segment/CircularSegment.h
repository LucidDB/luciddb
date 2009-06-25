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

#ifndef Fennel_CircularSegment_Included
#define Fennel_CircularSegment_Included

#include "fennel/segment/DelegatingSegment.h"

FENNEL_BEGIN_NAMESPACE

/**
 * CircularSegment implements circular page allocation in terms of an
 * underlying LINEAR_ALLOCATION segment.  See <a
 * href="structSegmentDesign.html#CircularSegment">the design docs</a> for more
 * detail.
 *
 *<p>
 *
 * Deallocation of individual pages is not supported; however, the caller may
 * deallocate runs of oldest pages by calling deallocatePageRange with
 * NULL_PAGE_ID for startPageId; all pages up to and including the specified
 * endPageId will be deallocated.  This must be done periodically to prevent
 * the allocation point from wrapping around to the oldest allocated page, or
 * an assertion violation results.
 */
class FENNEL_SEGMENT_EXPORT CircularSegment
    : public DelegatingSegment
{
    friend class SegmentFactory;

    SharedCheckpointProvider pCheckpointProvider;

    BlockNum oldestPageNum;

    // TODO:  change design doc diagram from newestPageId to nextPageId
    BlockNum nextPageNum;

    BlockNum nPages;

    BlockNum checkpointThreshold1, checkpointThreshold2;

    explicit CircularSegment(
        SharedSegment delegateSegment,
        SharedCheckpointProvider pCheckpointProvider,
        PageId oldestPageId,
        PageId newestPageId);

public:
    virtual ~CircularSegment();

    // implement the Segment interface
    virtual BlockNum getAllocatedSizeInPages();
    virtual BlockId translatePageId(PageId);
    virtual PageId translateBlockId(BlockId);
    virtual PageId allocatePageId(PageOwnerId ownerId = ANON_PAGE_OWNER_ID);
    virtual void deallocatePageRange(PageId startPageId, PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
};

FENNEL_END_NAMESPACE

#endif

// End CircularSegment.h

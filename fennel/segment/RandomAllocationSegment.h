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

#ifndef Fennel_RandomAllocationSegment_Included
#define Fennel_RandomAllocationSegment_Included

#include "fennel/segment/DelegatingSegment.h"

#include <boost/enable_shared_from_this.hpp>

FENNEL_BEGIN_NAMESPACE

struct ExtentAllocationNode;

/**
 * RandomAllocationSegment implements RANDOM_ALLOCATION in terms of an
 * underlying segment supporting LINEAR_ALLOCATION.  See <a
 * href="structSegmentDesign.html#RandomAllocationSegment">the design docs</a>
 * for more detail.
 */
class RandomAllocationSegment
    : public DelegatingSegment,
        public boost::enable_shared_from_this<RandomAllocationSegment>
{
    friend class SegmentFactory;

    /**
     * Number of pages in one extent, including the ExtentAllocationNode itself
     * (so actual data capacity per extent is one less).  This is immutable.
     */
    BlockNum nPagesPerExtent;

    /**
     * Number of pages mapped by one SegmentAllocationNode, including the
     * SegmentAllocationNode itself.  This is immutable.
     */
    BlockNum nPagesPerSegAlloc;

    /**
     * Number of extents mapped by a full SegmentAllocationNode.  This
     * is immutable.
     */
    ExtentNum nExtentsPerSegAlloc;
    
    explicit RandomAllocationSegment(
        SharedSegment delegateSegment);

    /**
     * Allocates a new page from an extent known to have space.
     *
     * @param extentNum absolute 0-based extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @return allocated PageId
     */
    PageId allocateFromExtent(ExtentNum extentNum,PageOwnerId ownerId);

    /**
     * Allocates a new page from an extent known to have space, with the extent
     * allocation node already locked.
     *
     * @param extentNode locked ExtentAllocationNode corresponding to
     * extentNum
     *
     * @param extentNum absolute extent number from which to allocate
     *
     * @param ownerId PageOwnerId of owning object
     *
     * @return allocated PageId
     */
    PageId allocateFromLockedExtent(
        ExtentAllocationNode &extentNode,ExtentNum extentNum,
        PageOwnerId ownerId);

    /**
     * Calculates the PageId of a particular SegmentAllocationNode.
     *
     * @param iSegPage 0-based index of desired SegmentAllocationNode
     *
     * @return corresponding PageId
     */
    inline PageId getSegAllocPageId(uint iSegPage) const;

    /**
     * Calculates the PageId of a particular ExtentAllocationNode.
     *
     * @param extentNum absolute 0-based extent number
     *
     * @return corresponding PageId
     */
    inline PageId getExtentAllocPageId(ExtentNum extentNum) const;

    /**
     * Calculates a linear page number.
     *
     * @param extentNum absolute 0-based extent number of extent containing
     * desired page
     *
     * @param iPageInExtent 0-based index of page in extent
     *
     * @return BlockNum corresponding to a linear PageId in this segment
     */
    inline BlockNum makePageNum(
        ExtentNum extentNum,BlockNum iPageInExtent) const;

    /**
     * Maps a linear PageId from this segment into the corresponding
     * SegmentAllocationNode, ExtentAllocationNode, and extent-relative page
     * index.
     *
     * @param pageId input PageId
     *
     * @param iSegAlloc 0-based index of containing SegmentAllocationNode
     *
     * @param extentNum absolute 0-based extent number of containing
     * ExtentAllocationNode
     *
     * @param iPageInExtent 0-based index of page in extent
     */
    void splitPageId(
        PageId pageId,uint &iSegAlloc,
        ExtentNum &extentNum,BlockNum &iPageInExtent) const;

    /**
     * Tests whether the given PageId has valid contents
     * (either an allocated data page or an allocation map page).
     *
     * @param pageId the PageId to test
     *
     * @return true iff pageId is valid
     */
    bool isPageIdValid(PageId pageId);

    /**
     * Common implementation for isPageIdValid and isPageIdAllocated.
     */
    bool testPageId(PageId pageId,bool testAllocation);

    /**
     * Deallocates a single page.
     *
     * @param pageId PageId of page to deallocate
     */
    void deallocatePageId(PageId pageId);

    /**
     * @return the PageId of the first SegmentAllocationNode
     */
    inline PageId getFirstSegAllocPageId() const;

    /**
     * Infers the number of SegmentAllocationNodes from the
     * size of the underlying segment.
     */
    uint inferSegAllocCount();
    
    /**
     * Formats allocation pages based on current size of underlying segment,
     * marking all pages as deallocated.
     */
    void format();
    
    /**
     * Formats one extent allocation.
     *
     * @param extentNode locked ExtentAllocationNode
     */
    void formatExtent(ExtentAllocationNode &extentNode);

public:
    virtual ~RandomAllocationSegment();

    // implementation of Segment interface
    virtual BlockId translatePageId(PageId);
    virtual PageId getPageSuccessor(PageId pageId);
    virtual void setPageSuccessor(PageId pageId, PageId successorId);
    virtual PageId allocatePageId(PageOwnerId ownerId);
    virtual void deallocatePageRange(PageId startPageId,PageId endPageId);
    virtual bool isPageIdAllocated(PageId pageId);
    virtual AllocationOrder getAllocationOrder() const;
    virtual BlockNum getAllocatedSizeInPages();
};

FENNEL_END_NAMESPACE

#endif

// End RandomAllocationSegment.h
